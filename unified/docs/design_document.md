# Unified SCD Type 2 Pipeline - Design Document

## 1. Executive Summary

### 1.1 Purpose
This document describes the architecture and design of a **metadata-driven SCD Type 2 Pipeline Framework** 
built on Databricks Lakeflow Declarative Pipelines. The framework supports merging multiple source paths 
into unified streaming tables with automatic SCD2 history tracking.

### 1.2 Key Capabilities
- **Many-to-One Pattern**: Multiple sources → Single unified target (e.g., CDC consolidation)
- **Many-to-Many Pattern**: Multiple sources → Multiple targets (e.g., workflow events)
- **Metadata-Driven**: JSON configuration drives pipeline behavior
- **Environment Agnostic**: Same code works across dev/prod with variable injection

### 1.3 Framework Components
- **Lakeflow Python API:** `from pyspark import pipelines as dp`
- **Configuration:** JSON metadata files with `targets[]` array structure
- **Runtime Injection:** Catalog/schema from Spark config (set by databricks.yml)

### 1.4 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    METADATA-DRIVEN SCD2 PIPELINE FRAMEWORK                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  SOURCES (Shared Definitions)                                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │   │
│  │  │  Source A   │  │  Source B   │  │  Source C   │  ...              │   │
│  │  │ table_name  │  │ table_name  │  │ table_name  │                   │   │
│  │  │ description │  │ description │  │ description │                   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  TARGETS (Per-Target Source Mappings)                                 │   │
│  │  ┌────────────────────────────────────────────────────────────────┐  │   │
│  │  │  Target 1: unified_entity_scd2                                  │  │   │
│  │  │  ├── source_mappings:                                           │  │   │
│  │  │  │   ├── source_a: {view_name, flow_name, column_mapping}       │  │   │
│  │  │  │   └── source_b: {view_name, flow_name, column_mapping}       │  │   │
│  │  │  ├── schema: {column definitions}                               │  │   │
│  │  │  └── transforms: {type conversions}                             │  │   │
│  │  └────────────────────────────────────────────────────────────────┘  │   │
│  │  ┌────────────────────────────────────────────────────────────────┐  │   │
│  │  │  Target 2: another_entity_scd2 (optional)                       │  │   │
│  │  │  └── ... (same structure)                                       │  │   │
│  │  └────────────────────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Bronze Layer Pattern - Data Preservation

### 2.1 Design Principle

This unified layer follows the **Bronze Layer pattern** - we are NOT cleaning, deduplicating, or modifying source data. We are:

| What We DO | What We DON'T Do |
|------------|------------------|
| ✅ **Preserve** all source records as-is | ❌ Clean or deduplicate data |
| ✅ **Map** column names to unified schema | ❌ Apply business rules |
| ✅ **Convert** data types (string→date, epoch→timestamp) | ❌ Filter invalid records |
| ✅ **Track** source lineage via `source_system` | ❌ Merge records across sources |
| ✅ **Add** SCD2 history tracking | ❌ Resolve conflicts between sources |

### 2.2 Composite Key: `entity_key + source_system`

The critical design decision is that records from different sources **remain separate** even if they have the same entity key. The composite key is:

```
PRIMARY KEY = (entity_id, source_system)
```

This means:
- Customer `C001` from `system_a` is a **different record** than Customer `C001` from `system_b`
- Both records coexist in the unified table
- Downstream silver/gold layers decide how to resolve or merge them

### 2.3 Example: Unified Customer Table After Each Source Union

#### Source Tables (Raw Data Layer)

**Source A: Legacy CRM System** (`rdl_customer_crm_st`)
| id | name | email | created_date | is_active | event_ts |
|----|------|-------|--------------|-----------|----------|
| C001 | John Smith | john@old.com | 2020-01-15 | Y | 2024-01-01 10:00:00 |
| C002 | Jane Doe | jane@old.com | 2019-06-20 | N | 2024-01-01 10:00:00 |

**Source B: Salesforce CDC** (`rdl_customer_salesforce_st`)
| record_id | full_name | email_addr | registration_dt | active_flag | kafka_ts |
|-----------|-----------|------------|-----------------|-------------|----------|
| C001 | John M. Smith | john@new.com | 01/15/2020 | TRUE | 2024-06-15 14:30:00 |
| C003 | Bob Wilson | bob@sf.com | 03/22/2023 | TRUE | 2024-06-15 14:30:00 |

**Source C: Mobile App Events** (`rdl_customer_mobile_st`)
| customer_id | customer_name | contact_email | signup_epoch_ms | is_active | event_time |
|-------------|---------------|---------------|-----------------|-----------|------------|
| C001 | John Smith | john@mobile.com | 1579046400000 | 1 | 2024-07-01 09:00:00 |
| C004 | Alice Brown | alice@mobile.com | 1690243200000 | 1 | 2024-07-01 09:00:00 |

---

#### After Source A CDC Flow (First Source)

**Unified Table: `unified_customer_scd2`**

| customer_id | source_system | customer_name | email | created_date | is_active | event_timestamp | __START_AT | __END_AT |
|-------------|---------------|---------------|-------|--------------|-----------|-----------------|------------|----------|
| C001 | **crm** | John Smith | john@old.com | 2020-01-15 | true | 2024-01-01 10:00:00 | 2024-01-01 | NULL |
| C002 | **crm** | Jane Doe | jane@old.com | 2019-06-20 | false | 2024-01-01 10:00:00 | 2024-01-01 | NULL |

**What happened:**
- ✅ Column `id` → mapped to `customer_id`
- ✅ Column `name` → mapped to `customer_name`
- ✅ `is_active` "Y"/"N" → transformed to boolean true/false
- ✅ `source_system` = "crm" (from metadata default)
- ✅ SCD2 columns `__START_AT`, `__END_AT` auto-added by Lakeflow

---

#### After Source B CDC Flow (Second Source Added)

**Unified Table: `unified_customer_scd2`**

| customer_id | source_system | customer_name | email | created_date | is_active | event_timestamp | __START_AT | __END_AT |
|-------------|---------------|---------------|-------|--------------|-----------|-----------------|------------|----------|
| C001 | **crm** | John Smith | john@old.com | 2020-01-15 | true | 2024-01-01 10:00:00 | 2024-01-01 | NULL |
| C002 | **crm** | Jane Doe | jane@old.com | 2019-06-20 | false | 2024-01-01 10:00:00 | 2024-01-01 | NULL |
| C001 | **salesforce** | John M. Smith | john@new.com | 2020-01-15 | true | 2024-06-15 14:30:00 | 2024-06-15 | NULL |
| C003 | **salesforce** | Bob Wilson | bob@sf.com | 2023-03-22 | true | 2024-06-15 14:30:00 | 2024-06-15 | NULL |

**What happened:**
- ✅ Column `record_id` → mapped to `customer_id`
- ✅ Column `full_name` → mapped to `customer_name`
- ✅ Column `email_addr` → mapped to `email`
- ✅ `registration_dt` "01/15/2020" → transformed to DATE 2020-01-15
- ✅ `active_flag` "TRUE" → transformed to boolean true
- ✅ `source_system` = "salesforce" (from metadata default)
- ⚠️ **C001 exists TWICE** - different records because different `source_system`!

---

#### After Source C CDC Flow (Third Source Added)

**Unified Table: `unified_customer_scd2`**

| customer_id | source_system | customer_name | email | created_date | is_active | event_timestamp | __START_AT | __END_AT |
|-------------|---------------|---------------|-------|--------------|-----------|-----------------|------------|----------|
| C001 | **crm** | John Smith | john@old.com | 2020-01-15 | true | 2024-01-01 10:00:00 | 2024-01-01 | NULL |
| C002 | **crm** | Jane Doe | jane@old.com | 2019-06-20 | false | 2024-01-01 10:00:00 | 2024-01-01 | NULL |
| C001 | **salesforce** | John M. Smith | john@new.com | 2020-01-15 | true | 2024-06-15 14:30:00 | 2024-06-15 | NULL |
| C003 | **salesforce** | Bob Wilson | bob@sf.com | 2023-03-22 | true | 2024-06-15 14:30:00 | 2024-06-15 | NULL |
| C001 | **mobile** | John Smith | john@mobile.com | 2020-01-15 | true | 2024-07-01 09:00:00 | 2024-07-01 | NULL |
| C004 | **mobile** | Alice Brown | alice@mobile.com | 2023-07-25 | true | 2024-07-01 09:00:00 | 2024-07-01 | NULL |

**What happened:**
- ✅ Column `customer_id` → mapped to `customer_id` (same name, no change)
- ✅ Column `contact_email` → mapped to `email`
- ✅ `signup_epoch_ms` 1579046400000 → transformed to DATE 2020-01-15 (epoch_to_timestamp)
- ✅ `is_active` "1" → transformed to boolean true
- ✅ `source_system` = "mobile" (from metadata default)
- ⚠️ **C001 now exists THREE times** - one per source system!

---

#### SCD2 History Example (Source A Update)

When Source A sends an update for C001:

**Incoming CDC Record:**
| id | name | email | is_active | event_ts | __operation |
|----|------|-------|-----------|----------|-------------|
| C001 | John R. Smith | john.r@new.com | Y | 2024-08-01 12:00:00 | UPDATE |

**Unified Table After Update:**

| customer_id | source_system | customer_name | email | is_active | event_timestamp | __START_AT | __END_AT |
|-------------|---------------|---------------|-------|-----------|-----------------|------------|----------|
| C001 | crm | John Smith | john@old.com | true | 2024-01-01 10:00:00 | 2024-01-01 | **2024-08-01** |
| C001 | crm | **John R. Smith** | **john.r@new.com** | true | **2024-08-01 12:00:00** | **2024-08-01** | NULL |
| C001 | salesforce | John M. Smith | john@new.com | true | 2024-06-15 14:30:00 | 2024-06-15 | NULL |
| C001 | mobile | John Smith | john@mobile.com | true | 2024-07-01 09:00:00 | 2024-07-01 | NULL |

**What happened:**
- ✅ Old CRM record closed (`__END_AT` = 2024-08-01)
- ✅ New CRM record created with updated values
- ✅ Salesforce and Mobile records **untouched** - different source_system
- ✅ Full history preserved - no data lost

---

### 2.4 What Gets Preserved vs. Served

| Category | Preserved (Bronze) | Served to Silver/Gold |
|----------|-------------------|----------------------|
| **Raw Values** | ✅ All original column values | ✅ In target column names |
| **Data Types** | ❌ Converted to target types | ✅ Consistent types across sources |
| **Source Lineage** | ✅ `source_system` column | ✅ Can filter/group by source |
| **Duplicates** | ✅ Same entity from different sources kept separate | ✅ Downstream deduplication |
| **History** | ✅ Full SCD2 history per source | ✅ `__START_AT`, `__END_AT` columns |
| **Invalid Data** | ✅ Not filtered or cleaned | ✅ Silver layer handles quality |
| **Conflicts** | ✅ C001 from 3 sources = 3 records | ✅ Gold layer merges/resolves |

### 2.5 Why This Pattern?

1. **Audit Trail**: Every source record is preserved with full history
2. **Debugging**: Can trace any issue back to source system
3. **Flexibility**: Silver/Gold layers can implement different merge strategies
4. **No Data Loss**: Original values never modified, only type-converted
5. **Parallel Processing**: Each source CDC flow operates independently

---

## 3. Metadata Structure

### 3.1 New `targets[]` Array Structure

The framework uses a flexible metadata structure that separates shared source definitions from target-specific mappings:

```json
{
  "pipeline": {
    "name": "pipeline_name",
    "version": "2.0.0",
    "description": "Pipeline description"
  },
  
  "sources": {
    "source_a": {
      "table_name": "rdl_source_a",
      "description": "Source A description",
      "source_system_value": "source_a"
    },
    "source_b": {
      "table_name": "rdl_source_b",
      "description": "Source B description",
      "source_system_value": "source_b"
    }
  },
  
  "targets": [
    {
      "name": "unified_entity_scd2",
      "enabled": true,
      "keys": ["entity_id", "source_system"],
      "sequence_by": "event_timestamp",
      "delete_condition": "is_deleted = true",
      "track_history_except_columns": ["source_system", "ingestion_timestamp"],
      
      "source_mappings": {
        "source_a": {
          "enabled": true,
          "view_name": "source_a_v",
          "flow_name": "source_a_to_unified_flow",
          "column_mapping": {
            "entity_id": {"source_col": "id", "transform": null},
            "entity_name": {"source_col": "name", "transform": null}
          }
        },
        "source_b": {
          "enabled": true,
          "view_name": "source_b_v",
          "flow_name": "source_b_to_unified_flow",
          "column_mapping": {
            "entity_id": {"source_col": "record_id", "transform": null},
            "entity_name": {"source_col": "full_name", "transform": null}
          }
        }
      },
      
      "schema": {
        "entity_id": {"dtype": "STRING", "nullable": false},
        "entity_name": {"dtype": "STRING", "nullable": true}
      },
      
      "transforms": {
        "to_date": {"formats": ["yyyy-MM-dd", "MM/dd/yyyy"]},
        "cast_boolean": {"true_values": ["TRUE", "1", "Y"]}
      }
    }
  ]
}
```

### 3.2 Why This Structure?

| Benefit | Description |
|---------|-------------|
| **Separation of concerns** | Shared source properties vs target-specific mappings |
| **Flexibility** | Same source can map differently to different targets |
| **Extensibility** | Add new targets without duplicating source definitions |
| **Enable/disable** | Toggle sources per target independently |

---

## 4. Supported Patterns

### 4.1 Many-to-One (CDC Consolidation)

Multiple data sources feeding a single unified table:

```
┌─────────────┐
│  Source A   │──┐
└─────────────┘  │
                 │
┌─────────────┐  │    ┌─────────────────────┐
│  Source B   │──┼───▶│  unified_scd2       │
└─────────────┘  │    │  (Single Target)    │
                 │    └─────────────────────┘
┌─────────────┐  │
│  Source C   │──┘
└─────────────┘

Use Case: Customer CDC from multiple source systems
```

### 4.2 Many-to-Many (Workflow Events)

Multiple sources feeding multiple domain-specific targets:

```
┌─────────────────┐
│  Event Stream   │──┬──▶ underwriting_scd2
└─────────────────┘  │
                     ├──▶ claims_scd2
┌─────────────────┐  │
│  History Table  │──┴──▶ policy_scd2
└─────────────────┘

Use Case: Workflow events split by domain/workflow_type
```

---

## 5. Module Architecture

### 5.1 File Structure

```
src/
├── metadata/
│   └── {processing_type}/{layer}/{domain}/
│       └── {domain}_pipeline.json       # Metadata configuration
├── metadata_loader.py                   # Load & inject runtime config
├── transformations.py                   # Type conversion logic
├── views.py                             # Source view generation
└── pipeline.py                          # Main orchestration

resources/
└── {processing_type}/{layer}/{domain}/
    ├── {domain}_pipeline.yml            # Pipeline resource
    └── {domain}_job.yml                 # Job resource
```

### 5.2 Module Responsibilities

| Module | Purpose |
|--------|----------|
| `metadata_loader.py` | Load JSON, inject catalog/schema from Spark config, provide accessor functions |
| `mapper.py` | `apply_mapping()` - column renaming, default values (no type changes) |
| `transformations.py` | `apply_transforms()` - type conversions using registry pattern |
| `views.py` | Create Lakeflow views, orchestrate mapping → transforms pipeline |
| `pipeline.py` | Create streaming tables and CDC flows for all targets |

### 5.3 Pipeline Flow

```
Source Table
     │
     ▼
mapper.apply_mapping(df, column_mapping)
  ├── Rename source columns to target names
  ├── Apply default values for missing columns
  └── Preserve original data types
     │
     ▼
transformations.apply_transforms(df, column_mapping)
  ├── Apply registered transforms (to_date, epoch_to_timestamp, etc.)
  └── Cast all columns to target data types
     │
     ▼
Lakeflow View → CDC Flow → Target Streaming Table
```

### 5.4 Accessor Functions

```python
# Target accessors (support multiple targets)
get_all_targets()                        # List of all targets
get_target(index=0)                      # Get specific target
get_target_name(index=0)                 # Target table name
get_target_schema(index=0)               # Column definitions
get_scd2_keys(index=0)                   # Primary keys

# Source mapping accessors (per-target)
get_source_mappings(target_index=0)      # All source mappings for target
get_enabled_source_mappings(target_index=0)  # Only enabled ones
get_column_mapping(source_name, target_index=0)  # Column mapping
get_full_source_config(source_name, target_index=0)  # Merged config
```

---

## 6. Configuration Flow

### 6.1 Environment Variables

Defined in `databricks.yml` per environment:

```yaml
targets:
  dev:
    variables:
      catalog: my_catalog
      schema: dev_schema
      source_catalog: my_catalog
      source_schema: raw_data_layer
  prod:
    variables:
      catalog: my_catalog
      schema: prod_schema
      source_catalog: my_catalog
      source_schema: raw_data_layer
```

### 6.2 Variable Injection

```
databricks.yml (variables per environment)
         │
         ▼
pipeline.yml (passes to Spark config)
         │
         ▼
metadata_loader.py (reads Spark config, injects into metadata)
         │
         ▼
Runtime metadata with catalog/schema populated
```

### 6.3 Pipeline Configuration

```yaml
# {domain}_pipeline.yml
configuration:
  bundle.sourcePath: ${workspace.file_path}/src
  pipeline.catalog: ${var.catalog}
  pipeline.schema: ${var.schema}
  pipeline.source_catalog: ${var.source_catalog}
  pipeline.source_schema: ${var.source_schema}
  pipeline.metadata_path: stream/unified/{domain}/{domain}_pipeline.json
```

---

## 7. SCD Type 2 Implementation

### 7.1 CDC Flow Configuration

Each source mapping creates a CDC flow:

```python
dp.create_auto_cdc_flow(
    name=source_mapping["flow_name"],
    target=target_name,
    source=source_mapping["view_name"],
    keys=get_scd2_keys(target_index),
    sequence_by=F.col(get_sequence_column(target_index)),
    stored_as_scd_type="2",
    apply_as_deletes=F.expr(get_delete_condition(target_index)),
    track_history_except_column_list=get_track_history_except_columns(target_index)
)
```

### 7.2 Auto-Managed Columns

Lakeflow automatically adds and manages:

| Column | Purpose |
|--------|---------|
| `__START_AT` | Version start timestamp |
| `__END_AT` | Version end timestamp (NULL = current) |

### 7.3 Track History Except Columns

Columns in `track_history_except_columns` are updated in-place (SCD Type 1 behavior) without creating new history records:

```json
"track_history_except_columns": [
  "source_system",
  "ingestion_timestamp",
  "_version"
]
```

---

## 8. Schema Mapping

### 8.1 Column Mapping Format

```json
"column_mapping": {
  "target_column": {
    "source_col": "source_column_name",
    "transform": "transform_type",
    "default": "default_value"
  }
}
```

### 8.2 Transform Registry

Transforms are implemented using a registry pattern for easy extensibility:

```python
TRANSFORM_REGISTRY = {
    "to_date": _parse_date,
    "to_timestamp": _parse_timestamp,
    "epoch_to_timestamp": _epoch_to_timestamp_ms,
    "epoch_to_timestamp_ms": _epoch_to_timestamp_ms,
    "epoch_to_timestamp_s": _epoch_to_timestamp_s,
    "cast_string": lambda c: c.cast("STRING"),
    "cast_int": lambda c: c.cast("INT"),
    "cast_long": lambda c: c.cast("LONG"),
    "cast_boolean": _normalize_boolean,
}
```

| Transform | Description |
|-----------|-------------|
| `null` | No transformation (direct mapping) |
| `to_date` | Parse multiple date string formats (ISO, US, legacy) |
| `to_timestamp` | Parse multiple timestamp formats |
| `epoch_to_timestamp` | Convert epoch milliseconds to TIMESTAMP (default) |
| `epoch_to_timestamp_ms` | Convert epoch milliseconds to TIMESTAMP (explicit) |
| `epoch_to_timestamp_s` | Convert epoch seconds to TIMESTAMP |
| `cast_string` | Cast to STRING |
| `cast_int` | Cast to INT |
| `cast_long` | Cast to LONG |
| `cast_boolean` | Normalize boolean values (TRUE/1/Y/YES → true) |

### 8.3 Default Values

When `source_col` is null, the default value is used:

```json
"source_system": {
  "source_col": null,
  "transform": null,
  "default": "kafka_cdc"
}
```

---

## 9. Deployment

### 9.1 Bundle Commands

```bash
# Validate configuration
databricks bundle validate

# Deploy to workspace
databricks bundle deploy

# Run pipeline
databricks bundle run {pipeline_name}

# Run job
databricks bundle run {job_name}
```

### 9.2 Workspace Structure

```
/Workspace/Shared/.bundle/{bundle_name}/{target}/
├── files/
│   └── src/
│       ├── metadata/
│       ├── pipeline.py
│       ├── views.py
│       └── ...
└── artifacts/
```

---

## 10. Adding New Pipelines

### 10.1 Steps

1. **Create metadata folder**: `src/metadata/stream/unified/{domain}/`
2. **Create metadata JSON**: `{domain}_pipeline.json` with sources, targets, schema
3. **Create resources folder**: `resources/stream/unified/{domain}/`
4. **Create pipeline YAML**: `{domain}_pipeline.yml`
5. **Create job YAML**: `{domain}_job.yml`
6. **Update databricks.yml**: Add include pattern

### 10.2 Include Pattern

```yaml
include:
  - "resources/stream/unified/{domain}/{domain}_*.yml"
```

---

## 11. Testing

### 11.1 Test Pipeline

A test pipeline validates the metadata structure:

```
resources/test/test_metadata.pipeline.yml
src/metadata/test/test_pipeline.json
src/test_pipeline.py
```

### 11.2 Unit Tests

```
tests/
├── conftest.py         # Pytest fixtures
├── main_test.py        # Pipeline configuration tests
└── ...
```

---

## 12. Best Practices

1. **Naming Conventions**
   - Folder pattern: `{processing_type}/{layer}/{domain}/`
   - File pattern: `{domain}_pipeline.json`, `{domain}_pipeline.yml`

2. **Enable/Disable Sources**
   - Use `enabled: false` in source_mappings to disable without removing

3. **Environment Isolation**
   - Never hardcode catalog/schema in JSON
   - All environment-specific values in databricks.yml

4. **Sequential Merging**
   - Enable sources one at a time for controlled data migration
