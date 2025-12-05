# Unified SCD Type 2 Pipeline Framework

A **metadata-driven** Databricks Lakeflow pipeline framework that merges multiple source paths into unified **SCD Type 2 streaming tables**.

## Overview

This framework implements a flexible **Unification Layer** supporting:
- **Many-to-One Pattern**: Multiple sources → Single unified target (e.g., CDC consolidation)
- **Many-to-Many Pattern**: Multiple sources → Multiple targets (e.g., workflow events)

```
Source Tables (Bronze/Raw Layer)
├── Source A    → Historical data
├── Source B    → Initial snapshot
└── Source C    → Real-time CDC stream
        ↓
    apply_mapping()     # Column renaming, defaults
        ↓
    apply_transforms()  # Type conversions
        ↓
    apply_dedup()       # Watermark + dedup (optional, for Kafka)
        ↓
    CDC Flows (per source)
        ↓
Target Tables (Unified Layer)
└── unified_entity_scd2   → Complete SCD2 history
```

## Architecture

### Why Multiple CDC Flows?

Per Databricks documentation, we use **separate `create_auto_cdc_flow()` calls** per source:

| Aspect | Union Approach | Multiple CDC Flows |
|--------|---------------|-------------------|
| Refresh | Full refresh on any change | Incremental per source |
| Checkpoints | Single checkpoint | Independent per flow |
| Failure isolation | All or nothing | Source-specific |

### Pipeline Flow

```
Source Table → mapper.apply_mapping() → transformations.apply_transforms() → dedup.apply_dedup() → View → CDC Flow → Target
               (rename columns)         (type conversions)                    (watermark + dedup)
               (set defaults)           (epoch→timestamp, etc.)               (for Kafka sources)
```

### SCD Type 2 Features

- **Primary Keys**: Configurable per target
- **Sequence By**: Any timestamp column for ordering
- **Delete Detection**: Configurable condition expression
- **Track History Except**: Columns updated without new version
- **Auto-managed columns**: `__START_AT`, `__END_AT`

## Project Structure

```
unified/
├── src/
│   ├── metadata/
│   │   └── stream/unified/{domain}/
│   │       └── {domain}_pipeline.json   # Metadata configuration
│   ├── data_setup/                      # Test data setup notebooks
│   ├── test_notebooks/                  # Step-by-step test notebooks
│   ├── mapper.py                # Column mapping (rename, defaults)
│   ├── transformations.py       # Type conversions (registry pattern)
│   ├── dedup.py                 # Watermark-based deduplication
│   ├── exceptions.py            # Custom exception classes
│   ├── metadata_loader.py       # Load & inject runtime config
│   ├── views.py                 # Dynamic view generation
│   └── pipeline.py              # Main orchestration
├── resources/
│   └── stream/unified/{domain}/
│       ├── {domain}_pipeline.yml  # Pipeline resource
│       ├── {domain}_job.yml       # Job resource
│       └── {domain}_test_job.yml  # Step-by-step test job (optional)
├── docs/
│   ├── design_document.md       # Technical design
│   └── implementation_plan.md   # Implementation phases
├── databricks.yml               # Bundle configuration
└── README.md                    # This file
```

## Quick Start

### Prerequisites

- Databricks CLI configured with workspace access
- Access to target Unity Catalog
- Source tables exist in raw data layer schema

### Deploy & Run

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to dev environment
databricks bundle deploy --target dev

# Run a specific pipeline
databricks bundle run {domain}_pipeline --refresh-all

# List available pipelines
databricks bundle summary
```

### Verify Results

Query the target table:
```sql
SELECT 
    source_system,
    COUNT(*) as total_records,
    SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) as current_records
FROM {catalog}.{schema}.unified_{entity}_scd2
GROUP BY source_system
```

## Module Architecture

### Core Modules

| Module | Responsibility |
|--------|----------------|
| `mapper.py` | Column renaming, default values (no type changes) |
| `transformations.py` | Type conversions using registry pattern |
| `dedup.py` | Watermark-based deduplication for Kafka/streaming sources |
| `exceptions.py` | Specific exception types for error handling |
| `metadata_loader.py` | Load JSON config, inject runtime variables |
| `views.py` | Create Lakeflow views for each source |
| `pipeline.py` | Create streaming tables and CDC flows |

### Transform Registry

| Transform | Description |
|-----------|-------------|
| `to_date` | Parse multiple date string formats |
| `to_timestamp` | Parse multiple timestamp formats |
| `epoch_to_timestamp` | Convert epoch milliseconds to TIMESTAMP |
| `epoch_to_timestamp_ms` | Explicit milliseconds conversion |
| `epoch_to_timestamp_s` | Explicit seconds conversion |
| `cast_string` | Cast to STRING |
| `cast_int` | Cast to INT |
| `cast_long` | Cast to LONG |
| `cast_boolean` | Normalize boolean values (Y/N, 1/0, etc.) |

### Deduplication Configuration

For Kafka/streaming sources with out-of-order data or at-least-once delivery:

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

| Layer | Purpose |
|-------|---------|
| **Watermark** | Tolerates late-arriving data within window |
| **Offset Dedup** | Removes Kafka retry duplicates (partition+offset) |
| **Logical Dedup** | Removes business key duplicates within window |

### Target Table Auto-Managed Columns

| Column | Type | Description |
|--------|------|-------------|
| `__START_AT` | TIMESTAMP | Version start (auto-managed by Lakeflow) |
| `__END_AT` | TIMESTAMP | Version end - NULL = current (auto-managed) |

## Adding New Pipelines

1. **Create metadata folder**: `src/metadata/stream/unified/{domain}/`
2. **Create metadata JSON**: `{domain}_pipeline.json`
3. **Create resources folder**: `resources/stream/unified/{domain}/`
4. **Create pipeline YAML**: `{domain}_pipeline.yml`
5. **Create job YAML**: `{domain}_job.yml`
6. **Update databricks.yml**: Add include pattern

See [Implementation Plan](docs/implementation_plan.md) for detailed steps.

## Step-by-Step Testing

For complex pipelines (especially with dedup), use the test job pattern to validate each module:

```bash
# Run individual test steps
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

## Development

### Local Testing

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest
```

### Deployment Targets

Configure in `databricks.yml`:

```yaml
targets:
  dev:
    mode: development
    variables:
      catalog: my_catalog
      schema: dev_schema
  prod:
    mode: production
    variables:
      catalog: my_catalog  
      schema: prod_schema
```

## Documentation

- [Design Document](docs/design_document.md) - Technical architecture and design decisions
- [Implementation Plan](docs/implementation_plan.md) - Phase-by-phase implementation details

## License

Internal use only.
