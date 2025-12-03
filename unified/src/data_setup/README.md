# Data Setup Scripts

This folder contains scripts to create and populate the source streaming tables for development and testing.

## Tables Created

| Table | Description |
|-------|-------------|
| `rdl_customer` | Kafka CDC stream (ongoing real-time changes) |
| `rdl_customer_init_st` | SQL Server initial snapshot (baseline state) |
| `rdl_customer_hist_st` | Greenplum legacy history (oldest data) |

## Scripts

| Script | Purpose |
|--------|---------|
| `01_create_customer_tables.py` | Creates the 3 source streaming tables with proper schema |
| `02_load_customer_data.py` | Loads sample customer data for testing |
| `reset_target.py` | Drops the unified target table (for full refresh) |

## Usage

Run the notebooks in order in Databricks:
1. `01_create_customer_tables.py` - Creates the streaming tables
2. `02_load_customer_data.py` - Loads sample data for testing

To reset the target table:
- Run `reset_target.py` to drop `unified_customer_scd2`
