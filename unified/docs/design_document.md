# Unified SCD Type 2 Pipeline - Design Document

## 1. Executive Summary

### 1.1 MVP Scope
This pipeline implements **Stream 3 (Unification Layer)** of the overall data architecture.
Streams 1 & 2 (Bronze Raw & Bronze Processed) are already built and running upstream.

### 1.2 Goal
Merge **3 source paths for the same Customer entity** into a **single SCD Type 2 Streaming Table**
that provides a complete, sequenced history from legacy (Greenplum) to real-time (Kafka CDC).

### 1.3 Framework
- **Lakeflow Python API:** `import databricks.sdk.runtime.pipelines as dp`
- **Target Table:** `unified_customer_scd2` (Lakeflow Streaming Table)

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
import databricks.sdk.runtime.pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    TimestampType, IntegerType, BooleanType, LongType
)
```

### 4.2 Step 2: Source Views

Create 3 views that normalize each source to a common schema:

```python
@dp.view(name="gp_customer_v", comment="Greenplum legacy history - normalized")
def gp_customer_view():
    return spark.readStream.table("ltc_insurance.raw_data_layer.rdl_customer_hist_st").select(
        "customer_id", "customer_name", "date_of_birth", "email", "phone",
        "state", "zip_code", "status", "last_login", "session_count", 
        "page_views", "is_deleted", "event_timestamp", 
        "ingestion_timestamp", "source_system", "_version"
        # Excludes: valid_from, valid_to, is_current
    )

@dp.view(name="sql_customer_v", comment="SQL Server initial snapshot - normalized")
def sql_customer_view():
    return spark.readStream.table("ltc_insurance.raw_data_layer.rdl_customer_init_st").select(
        "customer_id", "customer_name", "date_of_birth", "email", "phone",
        "state", "zip_code", "status", "last_login", "session_count", 
        "page_views", "is_deleted", "event_timestamp", 
        "ingestion_timestamp", "source_system", "_version"
    )

@dp.view(name="cdc_customer_v", comment="Kafka CDC stream - normalized")
def cdc_customer_view():
    return spark.readStream.table("ltc_insurance.raw_data_layer.rdl_customer").select(
        "customer_id", "customer_name", "date_of_birth", "email", "phone",
        "state", "zip_code", "status", "last_login", "session_count", 
        "page_views", "is_deleted", "event_timestamp", 
        "ingestion_timestamp", 
        F.lit("kafka_cdc").alias("source_system"),  # Add missing column
        "_version"
    )
```

### 4.3 Step 3: Target Streaming Table

```python
dp.create_streaming_table(
    name="unified_customer_scd2",
    comment="Unified SCD Type 2 table - complete customer history from Greenplum to real-time CDC"
)
```

### 4.4 Step 4: Multiple CDC Flows

**Design Decision:** Per Databricks documentation, we use **3 separate `create_auto_cdc_flow()` calls**
targeting the same streaming table instead of `unionByName()`.

**Benefits:**
- Incremental updates without full refresh
- Independent processing per source
- Handles out-of-order data via `sequence_by`

```python
# Common CDC configuration
CDC_CONFIG = {
    "keys": ["customer_id"],
    "sequence_by": F.col("event_timestamp"),
    "stored_as_scd_type": "2",
    "apply_as_deletes": F.expr("is_deleted = true"),
    "except_column_list": ["source_system", "ingestion_timestamp", "_version"]
}

# Flow 1: Greenplum History (oldest data)
dp.create_auto_cdc_flow(
    target="unified_customer_scd2",
    source="gp_customer_v",
    **CDC_CONFIG
)

# Flow 2: SQL Server Initial (baseline snapshot)
dp.create_auto_cdc_flow(
    target="unified_customer_scd2",
    source="sql_customer_v",
    **CDC_CONFIG
)

# Flow 3: Kafka CDC (ongoing real-time changes)
dp.create_auto_cdc_flow(
    target="unified_customer_scd2",
    source="cdc_customer_v",
    **CDC_CONFIG
)
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
│   ├── design_document.md      # This file
│   └── implementation_plan.md  # Step-by-step plan
├── src/
│   ├── pipeline.py             # Main Lakeflow pipeline
│   ├── metadata/
│   │   ├── schemas.py          # Schema definitions
│   │   └── config.py           # Configuration
│   └── data_setup/             # Test data scripts
├── resources/
│   ├── unified.pipeline.yml    # Pipeline resource definition
│   └── unified.job.yml         # Job resource definition
├── tests/
│   └── main_test.py
├── databricks.yml              # Bundle configuration
└── README.md
```

---

## 8. Next Steps

| Step | Description | Status |
|------|-------------|--------|
| 1 | Imports & Setup | ✅ Complete |
| 2 | Source Views (3 views) | ⬜ Not Started |
| 3 | Target Streaming Table | ⬜ Not Started |
| 4 | CDC Flows (3 flows) | ⬜ Not Started |
| 5 | Testing & Validation | ⬜ Not Started |
| 6 | Deployment | ⬜ Not Started |
