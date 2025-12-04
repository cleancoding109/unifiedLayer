# Data Setup Scripts

This folder contains scripts to create and populate the source streaming tables for development and testing.

## Customer CDC Tables

| Table | Description |
|-------|-------------|
| `rdl_customer` | Kafka CDC stream (ongoing real-time changes) |
| `rdl_customer_init_st` | SQL Server initial snapshot (baseline state) |
| `rdl_customer_hist_st` | Greenplum legacy history (oldest data) |

## Pega Workflow Tables

| Table | Description |
|-------|-------------|
| `rdl_pega_uw_events_st` | Pega underwriting event stream (real-time from Kafka) |
| `rdl_pega_uw_bix_hist_st` | Pega BIX underwriting history (historical with SCD dates) |

## Scripts

| Script | Purpose |
|--------|---------|
| `01_create_customer_tables.py` | Creates the 3 customer source streaming tables |
| `02_load_customer_data.py` | Loads sample customer data for testing |
| `03_create_pega_tables.py` | Creates the Pega workflow source tables |
| `04_load_pega_data.py` | Loads sample Pega workflow data for testing |
| `check_target.py` | Query and inspect target tables |
| `reset_target.py` | Drops the unified target table (for full refresh) |

## Usage

Run the notebooks in order in Databricks:

### Customer Pipeline Setup
1. `01_create_customer_tables.py` - Creates the streaming tables
2. `02_load_customer_data.py` - Loads sample data for testing

### Pega Pipeline Setup
3. `03_create_pega_tables.py` - Creates the Pega workflow tables
4. `04_load_pega_data.py` - Loads sample Pega workflow data

### Utilities
- `check_target.py` - Inspect target table contents
- `reset_target.py` - Drop target table for full refresh
