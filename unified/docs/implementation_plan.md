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

### Phase 5: Multi-Target Support ✅ COMPLETE

| Task | Status |
|------|--------|
| Updated pipeline.py to loop through all targets | ✅ |
| Updated views.py for multi-target view generation | ✅ |
| pega_workflow_pipeline (3 targets from 2 sources) | ✅ |
| Backward compatibility with single-target pipelines | ✅ |

### Phase 6: Mapper/Transforms Refactoring ✅ COMPLETE

| Task | Status |
|------|--------|
| Created mapper.py with apply_mapping() | ✅ |
| Refactored transformations.py with registry pattern | ✅ |
| Added epoch_to_timestamp transforms | ✅ |
| Updated views.py for sequential mapping → transforms | ✅ |
| All pipelines verified working | ✅ |

### Phase 7: Deduplication & Out-of-Order Handling ✅ COMPLETE

| Task | Status |
|------|--------|
| Created dedup.py with watermark-based deduplication | ✅ |
| Added apply_dedup() for full dedup pipeline | ✅ |
| Added apply_watermark_only() for watermark without dedup | ✅ |
| Added validate_dedup_config() for configuration validation | ✅ |
| Added get_dedup_stats() for testing/debugging | ✅ |
| Created claims_cdc test pipeline with dedup enabled | ✅ |
| Created step-by-step test job framework | ✅ |
| All test steps (1-6) verified passing | ✅ |

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

### pega_workflow_pipeline (Multi-Target Example)

| Property | Value |
|----------|-------|
| **Sources** | pega_event_stream, pega_bix_history |
| **Targets** | pega_underwriting_scd2, pega_claims_scd2, pega_service_scd2 |
| **Pattern** | Many-to-Many (multiple targets from shared sources) |
| **Status** | ✅ Running |

### claims_cdc_pipeline (Dedup Test Pipeline)

| Property | Value |
|----------|-------|
| **Sources** | legacy_claims (no dedup), kafka_claims (with dedup) |
| **Target** | unified_claims_scd2 |
| **Pattern** | Many-to-One with selective deduplication |
| **Features** | Watermark, offset dedup, logical dedup |
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
│   │   └── stream/unified/{domain}/
│   │       └── {domain}_pipeline.json   # Per-domain metadata
│   ├── data_setup/                      # Test data setup notebooks
│   │   ├── 06_create_claims_tables.py
│   │   └── 07_load_claims_data.py
│   ├── test_notebooks/                  # Step-by-step test notebooks
│   │   ├── test_claims_metadata.py
│   │   ├── test_claims_mapper.py
│   │   ├── test_claims_transformations.py
│   │   └── test_claims_dedup.py
│   ├── mapper.py                        # Column mapping (rename, defaults)
│   ├── metadata_loader.py               # Metadata loading & accessor functions
│   ├── transformations.py               # Type conversions (registry pattern)
│   ├── dedup.py                         # Watermark-based deduplication
│   ├── views.py                         # Dynamic view generation
│   ├── pipeline.py                      # Main orchestration (multi-target)
│   └── test_pipeline.py                 # Test pipeline notebook
├── resources/
│   └── stream/unified/{domain}/
│       ├── {domain}_pipeline.yml        # Pipeline resource
│       ├── {domain}_job.yml             # Job resource
│       └── {domain}_test_job.yml        # Step-by-step test job (optional)
└── tests/
    ├── conftest.py
    └── main_test.py
```

### Module Pipeline Flow

```
Source Table
     │
     ▼
mapper.apply_mapping()      # Rename columns, apply defaults
     │
     ▼
transformations.apply_transforms()   # Type conversions
     │
     ▼
dedup.apply_dedup()         # Watermark + offset + logical dedup (optional)
     │
     ▼
Lakeflow View → CDC Flow → Target
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

## Dedup Configuration

### Dedup Config Structure (in source_mappings)

```json
"dedup_config": {
  "enabled": true,
  "watermark_column": "event_timestamp",
  "watermark_delay": "30 minutes",
  "offset_dedup": {
    "enabled": true,
    "columns": ["kafka_partition", "kafka_offset"]
  },
  "logical_dedup": {
    "enabled": true,
    "columns": ["claim_id", "source_system"]
  }
}
```

### When to Enable Dedup

| Source Type | Enable Dedup? | Reason |
|-------------|---------------|--------|
| Kafka/Streaming | ✅ Yes | Out-of-order, at-least-once delivery |
| Legacy/Batch | ❌ No | Already deduplicated |
| CDC with dedup | ❌ No | Connector handles dedup |

---

## Step-by-Step Testing

### Test Job Structure

Create a test job that runs each module independently:

```bash
# Run individual steps
databricks bundle run claims_cdc_test_job --only step1_create_tables
databricks bundle run claims_cdc_test_job --only step2_load_test_data
databricks bundle run claims_cdc_test_job --only step3_test_metadata
databricks bundle run claims_cdc_test_job --only step4_test_mapper
databricks bundle run claims_cdc_test_job --only step5_test_transformations
databricks bundle run claims_cdc_test_job --only step6_test_dedup
databricks bundle run claims_cdc_test_job --only step7_run_pipeline

# Run full test job
databricks bundle run claims_cdc_test_job
```

### Test Notebooks

| Notebook | Tests |
|----------|-------|
| `test_claims_metadata.py` | JSON loading, structure validation, accessor functions |
| `test_claims_mapper.py` | apply_mapping(), column renaming, defaults |
| `test_claims_transformations.py` | apply_transforms(), type conversions |
| `test_claims_dedup.py` | apply_dedup(), watermark, offset/logical dedup |

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

## Completed Enhancements

| Enhancement | Status | Description |
|-------------|--------|-------------|
| Multi-target support | ✅ DONE | Single pipeline writing to multiple targets |
| Mapper/Transforms refactoring | ✅ DONE | Sequential apply_mapping → apply_transforms |
| Epoch to timestamp | ✅ DONE | Convert epoch ms/s to TIMESTAMP |
| Transform registry | ✅ DONE | Extensible pattern for adding new transforms |
| Deduplication module | ✅ DONE | Watermark-based dedup for Kafka sources |
| Step-by-step test job | ✅ DONE | Module-by-module testing framework |

## Next Steps (Future Enhancements)

| Enhancement | Priority | Description |
|-------------|----------|-------------|
| Data quality rules | Medium | Add validation step in pipeline |
| Schema evolution | Low | Handle source schema changes gracefully |
| Monitoring dashboard | Low | Pipeline health and metrics |
| Custom transform support | Low | User-defined transforms from metadata |
