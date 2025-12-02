# Unified SCD Type 2 Pipeline

A Databricks Lakeflow pipeline that merges 3 source paths for the **same Customer entity** into a single **SCD Type 2 streaming table**.

## Overview

This pipeline implements **Stream 3 (Unification Layer)** of the data architecture:

```
Source Tables (ltc_insurance.raw_data_layer)
├── rdl_customer_hist_st    → Greenplum legacy history (oldest data)
├── rdl_customer_init_st    → SQL Server initial snapshot (baseline)
└── rdl_customer            → Kafka CDC stream (real-time changes)
                ↓
        3 x CDC Flows
                ↓
Target Table (ltc_insurance.unified_dev)
└── unified_customer_scd2   → Complete SCD2 history
```

## Architecture

### Why Multiple CDC Flows?

Per Databricks documentation, we use **3 separate `create_auto_cdc_flow()` calls** instead of `unionByName()`:

| Aspect | Union Approach | Multiple CDC Flows |
|--------|---------------|-------------------|
| Refresh | Full refresh on any change | Incremental per source |
| Checkpoints | Single checkpoint | Independent per flow |
| Failure isolation | All or nothing | Source-specific |

### SCD Type 2 Configuration

- **Primary Key:** `customer_id`
- **Sequence By:** `event_timestamp`
- **Delete Detection:** `is_deleted = true`
- **Auto-managed columns:** `__START_AT`, `__END_AT`

## Project Structure

```
unified/
├── src/
│   └── pipeline.py              # Main Lakeflow pipeline (Steps 1-4)
├── resources/
│   ├── unified.pipeline.yml     # Pipeline resource definition
│   └── unified.job.yml          # Job resource definition
├── docs/
│   ├── design_document.md       # Technical design
│   └── implementation_plan.md   # Implementation phases
├── databricks.yml               # Bundle configuration
└── README.md                    # This file
```

## Quick Start

### Prerequisites

- Databricks CLI configured with workspace access
- Access to `ltc_insurance` catalog
- Source tables exist in `raw_data_layer` schema

### Deploy & Run

```bash
# Deploy to dev environment
databricks bundle deploy --target dev

# Run the pipeline
databricks bundle run unified_pipeline
```

### Verify Results

Query the target table:
```sql
SELECT 
    source_system,
    COUNT(*) as total_records,
    SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) as current_records
FROM ltc_insurance.unified_dev.unified_customer_scd2
GROUP BY source_system
```

## Pipeline Details

### Source Views

| View | Source Table | Transformation |
|------|--------------|----------------|
| `gp_customer_v` | `rdl_customer_hist_st` | Excludes legacy SCD2 columns |
| `sql_customer_v` | `rdl_customer_init_st` | Passthrough |
| `cdc_customer_v` | `rdl_customer` | Adds `source_system` literal |

### CDC Flows

| Flow | Source View | Target |
|------|-------------|--------|
| `gp_to_unified_flow` | `gp_customer_v` | `unified_customer_scd2` |
| `sql_to_unified_flow` | `sql_customer_v` | `unified_customer_scd2` |
| `cdc_to_unified_flow` | `cdc_customer_v` | `unified_customer_scd2` |

### Target Table Schema

| Column | Type | SCD2 Role |
|--------|------|-----------|
| `customer_id` | STRING | Primary Key |
| `customer_name` | STRING | Tracked |
| `date_of_birth` | DATE | Tracked |
| `email` | STRING | Tracked |
| `phone` | STRING | Tracked |
| `state` | STRING | Tracked |
| `zip_code` | STRING | Tracked |
| `status` | STRING | Tracked |
| `last_login` | TIMESTAMP | Tracked |
| `session_count` | INT | Tracked |
| `page_views` | INT | Tracked |
| `is_deleted` | BOOLEAN | Delete indicator |
| `event_timestamp` | TIMESTAMP | Sequence column |
| `__START_AT` | TIMESTAMP | Version start (auto) |
| `__END_AT` | TIMESTAMP | Version end (auto) |

## Development

### Local Testing

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest
```

### Deployment Targets

| Target | Catalog | Schema | Mode |
|--------|---------|--------|------|
| `dev` | `ltc_insurance` | `unified_dev` | Development |
| `prod` | `ltc_insurance` | `unified_prod` | Production |

## Documentation

- [Design Document](docs/design_document.md) - Technical architecture and design decisions
- [Implementation Plan](docs/implementation_plan.md) - Phase-by-phase implementation details

## License

Internal use only.
