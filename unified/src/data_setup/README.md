# Sample Data and Table Setup

This folder contains scripts to create and populate the source streaming tables for development and testing.

## Tables Created

| Table | Description |
|-------|-------------|
| `greenplum_history_st` | One-time load from Greenplum (historical data) |
| `sqlserver_initial_st` | One-time load from SQL Server (initial snapshot) |
| `kafka_cdc_st` | CDC events from Kafka (ongoing changes) |

## Usage

Run the notebooks in order:
1. `01_create_source_tables.py` - Creates the streaming tables
2. `02_load_sample_data.py` - Loads sample data for testing
