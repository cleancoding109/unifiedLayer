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

## 2. Metadata Structure

### 2.1 New `targets[]` Array Structure

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
      "keys": ["entity_id"],
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

### 2.2 Why This Structure?

| Benefit | Description |
|---------|-------------|
| **Separation of concerns** | Shared source properties vs target-specific mappings |
| **Flexibility** | Same source can map differently to different targets |
| **Extensibility** | Add new targets without duplicating source definitions |
| **Enable/disable** | Toggle sources per target independently |

---

## 3. Supported Patterns

### 3.1 Many-to-One (CDC Consolidation)

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

### 3.2 Many-to-Many (Workflow Events)

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

## 4. Module Architecture

### 4.1 File Structure

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

### 4.2 Module Responsibilities

| Module | Purpose |
|--------|---------|
| `metadata_loader.py` | Load JSON, inject catalog/schema from Spark config, provide accessor functions |
| `transformations.py` | `apply_schema_mapping()` - column renaming, type conversion |
| `views.py` | Create Lakeflow views for each enabled source mapping |
| `pipeline.py` | Create streaming table and CDC flows |

### 4.3 Accessor Functions

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

## 5. Configuration Flow

### 5.1 Environment Variables

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

### 5.2 Variable Injection

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

### 5.3 Pipeline Configuration

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

## 6. SCD Type 2 Implementation

### 6.1 CDC Flow Configuration

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

### 6.2 Auto-Managed Columns

Lakeflow automatically adds and manages:

| Column | Purpose |
|--------|---------|
| `__START_AT` | Version start timestamp |
| `__END_AT` | Version end timestamp (NULL = current) |

### 6.3 Track History Except Columns

Columns in `track_history_except_columns` are updated in-place (SCD Type 1 behavior) without creating new history records:

```json
"track_history_except_columns": [
  "source_system",
  "ingestion_timestamp",
  "_version"
]
```

---

## 7. Schema Mapping

### 7.1 Column Mapping Format

```json
"column_mapping": {
  "target_column": {
    "source_col": "source_column_name",
    "transform": "transform_type",
    "default": "default_value"
  }
}
```

### 7.2 Supported Transforms

| Transform | Description |
|-----------|-------------|
| `null` | No transformation (direct mapping) |
| `to_date` | Parse string to DATE |
| `to_timestamp` | Parse string to TIMESTAMP |
| `cast_string` | Cast to STRING |
| `cast_int` | Cast to INT |
| `cast_long` | Cast to LONG |
| `cast_boolean` | Normalize boolean values |

### 7.3 Default Values

When `source_col` is null, the default value is used:

```json
"source_system": {
  "source_col": null,
  "transform": null,
  "default": "kafka_cdc"
}
```

---

## 8. Deployment

### 8.1 Bundle Commands

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

### 8.2 Workspace Structure

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

## 9. Adding New Pipelines

### 9.1 Steps

1. **Create metadata folder**: `src/metadata/stream/unified/{domain}/`
2. **Create metadata JSON**: `{domain}_pipeline.json` with sources, targets, schema
3. **Create resources folder**: `resources/stream/unified/{domain}/`
4. **Create pipeline YAML**: `{domain}_pipeline.yml`
5. **Create job YAML**: `{domain}_job.yml`
6. **Update databricks.yml**: Add include pattern

### 9.2 Include Pattern

```yaml
include:
  - "resources/stream/unified/{domain}/{domain}_*.yml"
```

---

## 10. Testing

### 10.1 Test Pipeline

A test pipeline validates the metadata structure:

```
resources/test/test_metadata.pipeline.yml
src/metadata/test/test_pipeline.json
src/test_pipeline.py
```

### 10.2 Unit Tests

```
tests/
├── conftest.py         # Pytest fixtures
├── main_test.py        # Pipeline configuration tests
└── ...
```

---

## 11. Best Practices

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
