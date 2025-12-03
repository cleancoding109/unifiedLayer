# Unified SCD Type 2 Pipeline - Implementation Plan

## Project Overview

Building a **Metadata-Driven SCD Type 2 Pipeline Framework** using Databricks Lakeflow Declarative Pipelines 
to merge multiple source paths into unified streaming tables with automatic history tracking.

---

## Implementation Status

### Phase 1: Core Framework ✅ COMPLETE

| Task | Status |
|------|--------|
| Project structure created | ✅ |
| databricks.yml configured | ✅ |
| Pipeline and job YAML resources | ✅ |
| metadata_loader.py with accessor functions | ✅ |
| transformations.py with schema mapping | ✅ |
| views.py with dynamic view generation | ✅ |
| pipeline.py with CDC flow creation | ✅ |

### Phase 2: Metadata Structure Refactoring ✅ COMPLETE

| Task | Status |
|------|--------|
| Migrated from `target` (singular) to `targets[]` (array) | ✅ |
| Moved column_mapping from sources to target.source_mappings | ✅ |
| Added get_enabled_source_mappings(target_index) | ✅ |
| Added get_full_source_config(source_name, target_index) | ✅ |
| Backward compatibility with old structure | ✅ |

### Phase 3: Multi-Domain Support ✅ COMPLETE

| Task | Status |
|------|--------|
| customer_cdc domain (3 sources → 1 target) | ✅ |
| underwriting_event domain (2 sources → 1 target) | ✅ |
| Nested folder structure (stream/unified/{domain}/) | ✅ |
| Domain-specific pipeline and job YAMLs | ✅ |

### Phase 4: Testing & Validation ✅ COMPLETE

| Task | Status |
|------|--------|
| Test pipeline for metadata structure validation | ✅ |
| customer_cdc_pipeline deployed and running | ✅ |
| underwriting_pipeline deployed and running | ✅ |
| All CDC flows completing successfully | ✅ |

---

## Current Pipelines

### customer_cdc_pipeline

| Property | Value |
|----------|-------|
| **Sources** | greenplum, sqlserver, kafka_cdc |
| **Target** | unified_customer_scd2 |
| **Pattern** | Many-to-One (CDC consolidation) |
| **Status** | ✅ Running |

### underwriting_pipeline

| Property | Value |
|----------|-------|
| **Sources** | pega_event_stream, pega_bix_history |
| **Target** | unified_underwriting_scd2 |
| **Pattern** | Many-to-One (workflow events) |
| **Status** | ✅ Running |

---

## File Structure

```
unified/
├── databricks.yml                       # Bundle configuration
├── pyproject.toml                       # Python project config
├── README.md
├── docs/
│   ├── design_document.md               # Architecture documentation
│   └── implementation_plan.md           # This file
├── src/
│   ├── metadata/
│   │   ├── stream/unified/
│   │   │   ├── customer_cdc/
│   │   │   │   └── customer_cdc_pipeline.json
│   │   │   └── underwriting_event/
│   │   │       └── underwriting_event_pipeline.json
│   │   └── test/
│   │       └── test_pipeline.json
│   ├── metadata_loader.py               # Metadata loading & accessor functions
│   ├── transformations.py               # Schema mapping transformations
│   ├── views.py                         # Dynamic view generation
│   ├── pipeline.py                      # Main orchestration
│   ├── test_pipeline.py                 # Test pipeline notebook
│   └── data_setup/                      # Test data scripts
├── resources/
│   ├── stream/unified/
│   │   ├── customer_cdc/
│   │   │   ├── customer_cdc_pipeline.yml
│   │   │   └── customer_cdc_job.yml
│   │   └── underwriting_event/
│   │       ├── underwriting_event_pipeline.yml
│   │       └── underwriting_event_job.yml
│   └── test/
│       ├── test_metadata.pipeline.yml
│       └── *.job.yml
└── tests/
    ├── conftest.py
    └── main_test.py
```

---

## Metadata Structure

### New `targets[]` Array Format

```json
{
  "pipeline": { "name": "...", "version": "2.0.0" },
  
  "sources": {
    "source_a": { "table_name": "...", "source_system_value": "..." },
    "source_b": { "table_name": "...", "source_system_value": "..." }
  },
  
  "targets": [
    {
      "name": "unified_entity_scd2",
      "enabled": true,
      "keys": ["entity_id"],
      "sequence_by": "event_timestamp",
      
      "source_mappings": {
        "source_a": {
          "enabled": true,
          "view_name": "source_a_v",
          "flow_name": "source_a_flow",
          "column_mapping": { ... }
        }
      },
      
      "schema": { ... },
      "transforms": { ... }
    }
  ]
}
```

---

## Key Accessor Functions

```python
# Target accessors
get_all_targets()                              # List all targets
get_target(index=0)                            # Get specific target
get_target_name(index=0)                       # Target table name
get_target_schema(index=0)                     # Column definitions

# Source mapping accessors
get_source_mappings(target_index=0)            # All source mappings
get_enabled_source_mappings(target_index=0)    # Only enabled mappings
get_column_mapping(source_name, target_index=0) # Column mapping
get_full_source_config(source_name, target_index=0) # Merged config
```

---

## Deployment Commands

```bash
# Validate bundle
databricks bundle validate

# Deploy to workspace
databricks bundle deploy

# Run pipelines
databricks bundle run customer_cdc_pipeline
databricks bundle run underwriting_pipeline

# Run jobs (with scheduling)
databricks bundle run customer_cdc_job
databricks bundle run underwriting_job

# List resources
databricks bundle summary
```

---

## Adding New Domains

### Steps

1. **Create metadata folder**
   ```
   src/metadata/stream/unified/{domain}/
   ```

2. **Create metadata JSON**
   ```
   {domain}_pipeline.json
   ```

3. **Create resources folder**
   ```
   resources/stream/unified/{domain}/
   ```

4. **Create pipeline YAML**
   ```yaml
   # {domain}_pipeline.yml
   resources:
     pipelines:
       {domain}_pipeline:
         name: unified_{domain}_scd2_pipeline
         catalog: ${var.catalog}
         target: ${var.schema}
         serverless: true
         libraries:
           - notebook:
               path: ../../../../src/pipeline.py
         configuration:
           pipeline.metadata_path: stream/unified/{domain}/{domain}_pipeline.json
   ```

5. **Create job YAML**
   ```yaml
   # {domain}_job.yml
   resources:
     jobs:
       {domain}_job:
         name: unified_{domain}_scd2_job
         tasks:
           - task_key: run_pipeline
             pipeline_task:
               pipeline_id: ${resources.pipelines.{domain}_pipeline.id}
   ```

6. **Update databricks.yml includes**
   ```yaml
   include:
     - "resources/stream/unified/{domain}/{domain}_*.yml"
   ```

---

## Environment Configuration

### databricks.yml Variables

```yaml
variables:
  catalog:
    description: Unity Catalog name
  schema:
    description: Schema name for pipeline target
  source_catalog:
    description: Catalog containing source tables
  source_schema:
    description: Schema containing source tables

targets:
  dev:
    mode: development
    variables:
      catalog: my_catalog
      schema: dev_schema
      source_catalog: my_catalog
      source_schema: raw_data_layer
  prod:
    mode: production
    variables:
      catalog: my_catalog
      schema: prod_schema
      source_catalog: my_catalog
      source_schema: raw_data_layer
```

---

## Next Steps (Future Enhancements)

| Enhancement | Priority | Description |
|-------------|----------|-------------|
| Multi-target support | Medium | Single pipeline writing to multiple targets |
| Data quality rules | Medium | Add validation in transformations |
| Schema evolution | Low | Handle source schema changes gracefully |
| Monitoring dashboard | Low | Pipeline health and metrics |
