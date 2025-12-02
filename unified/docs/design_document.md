# Unified SCD Type 2 Pipeline - Design Document

## 1. Executive Summary

### 1.1 MVP Scope
This pipeline implements **Stream 3 (Unification Layer)** of the overall data architecture.
Streams 1 & 2 (Bronze Raw & Bronze Processed) are already built and running upstream.

### 1.2 Goal
Merge **3 source paths for the same Customer entity** into a **single SCD Type 2 Streaming Table**
that provides a complete, sequenced history from legacy (Greenplum) to real-time (Kafka CDC).

### 1.3 Framework
- **Lakeflow Python API:** `from pyspark import pipelines as dp`
- **Target Table:** `unified_customer_scd2` (Lakeflow Streaming Table)
- **Architecture:** Metadata-driven, modular design
- **Configuration:** JSON metadata file as single source of truth

### 1.4 High-Level Architecture

```
Source Paths (Same Customer Entity)                    Target
===================================                    ======

+-------------------------------+
|  rdl_customer_hist_st         |---> gp_customer_v ---------+
|  (Greenplum Legacy History)   |     (normalized view)      |
|  Oldest data - loaded once    |                            |
+-------------------------------+                            |
                                                             |  3 x create_auto_cdc_flow()
+-------------------------------+                            |  (all target same table)
|  rdl_customer_init_st         |---> sql_customer_v --------+---> unified_customer_scd2
|  (SQL Server Initial Snapshot)|     (normalized view)      |     (SCD Type 2 Table)
|  Baseline state - loaded once |                            |
+-------------------------------+                            |
                                                             |
+-------------------------------+                            |
|  rdl_customer                 |---> cdc_customer_v --------+
|  (Kafka CDC Stream)           |     (normalized view)
|  Ongoing real-time changes    |
+-------------------------------+

Timeline: Greenplum (oldest) --> SQL Initial --> Kafka CDC (newest)
```

---

## 2. Data Sources

All 3 sources represent the **same Customer entity** from different paths/systems.

### 2.1 Greenplum History (`rdl_customer_hist_st`)
| Attribute | Value |
|-----------|-------|
| **Type** | Managed Table (loaded once via ADF/Lakeflow) |
| **Content** | Legacy historical customer data from Greenplum |
| **Purpose** | Provides the oldest history (e.g., 10+ years ago) |
| **Load Pattern** | One-time historical load |
| **Extra Columns** | `valid_from`, `valid_to`, `is_current` (pre-computed SCD2 from legacy) |

### 2.2 SQL Server Initial (`rdl_customer_init_st`)
| Attribute | Value |
|-----------|-------|
| **Type** | Managed Table (loaded once via ADF/Lakeflow) |
| **Content** | Initial snapshot from the new SQL Server system |
| **Purpose** | Provides baseline state before CDC stream started |
| **Load Pattern** | One-time snapshot load |

### 2.3 Kafka CDC Stream (`rdl_customer`)
| Attribute | Value |
|-----------|-------|
| **Type** | Streaming Table (Bronze Processed output from Stream 2) |
| **Content** | Real-time CDC events from SQL Server via Confluent Kafka |
| **Purpose** | Ongoing incremental changes (inserts, updates, deletes) |
| **Load Pattern** | Continuous streaming |

---

## 3. Source Schemas (Verified from Catalog)

All 3 tables have **aligned schemas** - minimal transformation needed.

### 3.1 Common Columns (All 3 Sources)

| Column | Type | SCD2 Role |
|--------|------|-----------|
| `customer_id` | STRING | **Primary Key** |
| `customer_name` | STRING | Track changes |
| `date_of_birth` | DATE | Track changes |
| `email` | STRING | Track changes |
| `phone` | STRING | Track changes |
| `state` | STRING | Track changes |
| `zip_code` | STRING | Track changes |
| `status` | STRING | Track changes |
| `last_login` | TIMESTAMP | Track changes |
| `session_count` | INT | Track changes |
| `page_views` | INT | Track changes |
| `is_deleted` | BOOLEAN | **Delete indicator** |
| `event_timestamp` | TIMESTAMP | **Sequence column** |
| `ingestion_timestamp` | TIMESTAMP | Exclude from tracking |
| `_version` | BIGINT | Exclude from tracking |

### 3.2 Source-Specific Columns

| Column | Greenplum | SQL Initial | Kafka CDC |
|--------|-----------|-------------|-----------|
| `source_system` | Yes (STRING) | Yes (STRING) | No - add as literal |
| `valid_from` | Yes (TIMESTAMP) | No | No |
| `valid_to` | Yes (TIMESTAMP) | No | No |
| `is_current` | Yes (BOOLEAN) | No | No |

### 3.3 Transformation Requirements

| Source | Transformation |
|--------|----------------|
| **Greenplum** | Exclude `valid_from`, `valid_to`, `is_current` (legacy SCD2 columns) |
| **SQL Initial** | No transformation needed |
| **Kafka CDC** | Add `source_system = 'kafka_cdc'` literal |

---

## 4. Technical Design

### 4.1 Step 1: Imports & Setup

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
```

### 4.2 Metadata Loading

All configuration is loaded from `pipeline_metadata.json`:

```python
# metadata_loader.py
METADATA = load_metadata()  # Loads JSON file

# Accessor functions
get_source_config("greenplum")  # Returns source configuration
get_column_mapping("sqlserver")  # Returns column mappings
get_target_table_name()          # Returns "unified_customer_scd2"
get_scd2_keys()                  # Returns ["customer_id"]
```

### 4.3 Schema Mapping

Each source has different column names. The `apply_schema_mapping()` function handles:
- **Column renaming**: `cust_id` → `customer_id`
- **Type conversion**: STRING dates → DATE type
- **Default values**: Missing `source_system` → "kafka_cdc"

```python
# transformations.py
def apply_schema_mapping(df, column_mapping, source_name):
    select_exprs = []
    for target_col, mapping in column_mapping.items():
        source_col = mapping.get("source_col")
        transform = mapping.get("transform")
        default_val = mapping.get("default")
        
        col_expr = _build_column_expression(source_col, transform, default_val)
        select_exprs.append(col_expr.alias(target_col))
    
    return df.select(*select_exprs)
```

### 4.4 Source Views

Views use schema mapping to normalize each source:

```python
# views.py
@dp.view(name=GP_VIEW_NAME, comment=get_source_config("greenplum")["description"])
def gp_customer_view():
    df = spark.readStream.table(GP_HISTORY_TABLE)
    return apply_schema_mapping(df, GP_COLUMN_MAPPING, "greenplum")

@dp.view(name=SQL_VIEW_NAME, comment=get_source_config("sqlserver")["description"])
def sql_customer_view():
    df = spark.readStream.table(SQL_INITIAL_TABLE)
    return apply_schema_mapping(df, SQL_COLUMN_MAPPING, "sqlserver")

@dp.view(name=CDC_VIEW_NAME, comment=get_source_config("kafka_cdc")["description"])
def cdc_customer_view():
    df = spark.readStream.table(CDC_STREAM_TABLE)
    return apply_schema_mapping(df, CDC_COLUMN_MAPPING, "kafka_cdc")
```

### 4.5 Target Streaming Table

```python
# pipeline.py
dp.create_streaming_table(
    name=TARGET_TABLE,  # From config: "unified_customer_scd2"
    comment="Unified SCD Type 2 - complete customer history"
)
```

### 4.6 Multiple CDC Flows

**Design Decision:** Per Databricks documentation, we use **3 separate `create_auto_cdc_flow()` calls**
targeting the same streaming table instead of `unionByName()`.

**Benefits:**
- Incremental updates without full refresh
- Independent processing per source
- Handles out-of-order data via `sequence_by`

```python
# pipeline.py - All config loaded from metadata
dp.create_auto_cdc_flow(
    name=GP_FLOW_NAME,           # From config
    target=TARGET_TABLE,
    source=GP_VIEW_NAME,         # From config
    keys=SCD2_KEYS,              # From config: ["customer_id"]
    sequence_by=F.col(SEQUENCE_COLUMN),
    stored_as_scd_type="2",
    apply_as_deletes=F.expr(DELETE_CONDITION),
    except_column_list=SCD2_EXCEPT_COLUMNS
)

# Same pattern for SQL and CDC flows...
```

---

## 5. Sequencing Strategy

### 5.1 Critical Requirement
The `event_timestamp` values must be properly ordered across sources:
- **Greenplum History:** Oldest timestamps (legacy data)
- **SQL Server Initial:** Middle timestamps (new system baseline)
- **Kafka CDC:** Newest timestamps (real-time changes)

### 5.2 How It Works
- All 3 sources use `event_timestamp` as the sequence column
- The SCD2 logic uses this to determine record ordering
- Records with newer timestamps supersede older ones for the same `customer_id`
- `__START_AT` and `__END_AT` are automatically managed by Lakeflow

### 5.3 Assumption
The source data already has properly sequenced `event_timestamp` values.
If not, a pre-processing step may be needed to ensure correct ordering.

---

## 6. SCD Type 2 Output

### 6.1 Target Table Columns

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | STRING | Primary key |
| `customer_name` | STRING | Tracked for changes |
| `date_of_birth` | DATE | Tracked for changes |
| `email` | STRING | Tracked for changes |
| `phone` | STRING | Tracked for changes |
| `state` | STRING | Tracked for changes |
| `zip_code` | STRING | Tracked for changes |
| `status` | STRING | Tracked for changes |
| `last_login` | TIMESTAMP | Tracked for changes |
| `session_count` | INT | Tracked for changes |
| `page_views` | INT | Tracked for changes |
| `__START_AT` | TIMESTAMP | SCD2 version start (auto-managed) |
| `__END_AT` | TIMESTAMP | SCD2 version end (auto-managed, NULL = current) |

### 6.2 Excluded from Change Tracking
- `source_system` - Metadata only
- `ingestion_timestamp` - ETL metadata
- `_version` - Internal versioning
- `is_deleted` - Used for delete detection, not tracking

### 6.3 Delete Handling
- When `is_deleted = true`, the record is logically deleted
- Lakeflow closes the current version with `__END_AT` timestamp
- No physical delete occurs (SCD2 preserves history)

---

## 7. File Structure

```
unified/
├── docs/
│   ├── design_document.md          # This file
│   └── implementation_plan.md      # Step-by-step plan
├── src/
│   ├── metadata/
│   │   └── pipeline_metadata.json  # Single source of truth (JSON)
│   ├── metadata_loader.py          # Loads & validates metadata
│   ├── config.py                   # Configuration from metadata
│   ├── schema.py                   # Target schema & column mappings
│   ├── transformations.py          # Type conversion functions
│   ├── views.py                    # Source view definitions
│   ├── pipeline.py                 # Main orchestration
│   └── data_setup/                 # Test data scripts
├── resources/
│   ├── unified.pipeline.yml        # Pipeline resource definition
│   └── unified.job.yml             # Job resource definition
├── tests/
│   └── main_test.py
├── databricks.yml                   # Bundle configuration
└── README.md
```

### Module Responsibilities

| Module | Purpose |
|--------|--------|
| `pipeline_metadata.json` | Single source of truth for all configuration |
| `metadata_loader.py` | Load JSON, validate, provide accessor functions |
| `config.py` | Source tables, target table, SCD2 settings |
| `schema.py` | Target schema, per-source column mappings |
| `transformations.py` | `apply_schema_mapping()`, date/boolean parsing |
| `views.py` | 3 source views with schema normalization |
| `pipeline.py` | Target table + 3 CDC flows |

---

## 8. Implementation Status

| Step | Description | Status |
|------|-------------|--------|
| 1 | Imports & Setup | ✅ Complete |
| 2 | Source Views (3 views) | ✅ Complete |
| 3 | Target Streaming Table | ✅ Complete |
| 4 | CDC Flows (3 flows) | ✅ Complete |
| 5 | Testing & Validation | ✅ Complete |
| 6 | Documentation | ✅ Complete |
| 7 | Modular Refactoring | ✅ Complete |
| 8 | Metadata-Driven Config | ✅ Complete |

### Pipeline Successfully Deployed

- **Pipeline ID:** `5c80b313-fc1f-47e9-8c1b-4f2c34ed1268`
- **Target Table:** `ltc_insurance.unified_dev.unified_customer_scd2`
- **All 3 CDC flows:** COMPLETED

---

## 9. Metadata Configuration

All configuration is stored in `src/metadata/pipeline_metadata.json`:

```json
{
  "pipeline": {
    "name": "unified_scd2_pipeline",
    "version": "2.0.0",
    "catalog": "ltc_insurance"
  },
  "target": {
    "table_name": "unified_customer_scd2",
    "keys": ["customer_id"],
    "sequence_by": "event_timestamp",
    "delete_condition": "is_deleted = true",
    "except_columns": ["source_system", "ingestion_timestamp", "_version"]
  },
  "sources": {
    "greenplum": { "table_name": "rdl_customer_hist_st", "column_mapping": {...} },
    "sqlserver": { "table_name": "rdl_customer_init_st", "column_mapping": {...} },
    "kafka_cdc": { "table_name": "rdl_customer", "column_mapping": {...} }
  },
  "target_schema": { "columns": {...} }
}
```

### Benefits of Metadata-Driven Approach

| Benefit | Description |
|---------|-------------|
| Single source of truth | All config in one JSON file |
| Easy modification | Change mappings without code changes |
| Self-documenting | Configuration visible and readable |
| Validation | Catches missing fields on load |
| Extensibility | Add new sources by adding to JSON |
