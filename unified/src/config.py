# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Module
# MAGIC 
# MAGIC Centralized configuration for the Unified SCD Type 2 Pipeline.

# COMMAND ----------

# =============================================================================
# SOURCE CONFIGURATION
# =============================================================================

# Source catalog and schema (where Bronze tables are located)
SOURCE_CATALOG = "ltc_insurance"
SOURCE_SCHEMA = "raw_data_layer"

# Source table names (fully qualified)
GP_HISTORY_TABLE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.rdl_customer_hist_st"
SQL_INITIAL_TABLE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.rdl_customer_init_st"
CDC_STREAM_TABLE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.rdl_customer"

# =============================================================================
# TARGET CONFIGURATION
# =============================================================================

# Target table name (catalog/schema set in pipeline YAML via bundle variables)
TARGET_TABLE = "unified_customer_scd2"

# =============================================================================
# SCD TYPE 2 CONFIGURATION
# =============================================================================

# Primary key(s) for SCD2 tracking
SCD2_KEYS = ["customer_id"]

# Columns to exclude from change detection
# (these columns changing won't trigger a new SCD2 version)
SCD2_EXCEPT_COLUMNS = [
    "source_system",        # Metadata - not business data
    "ingestion_timestamp",  # Metadata - not business data
    "_version",             # Internal versioning
]

# =============================================================================
# COLUMN GROUPS
# =============================================================================

# Columns that represent business data (tracked for SCD2 changes)
TRACKED_COLUMNS = [
    "customer_name",
    "date_of_birth",
    "email",
    "phone",
    "state",
    "zip_code",
    "status",
    "last_login",
    "session_count",
    "page_views",
]

# Columns for delete detection
DELETE_COLUMN = "is_deleted"
DELETE_CONDITION = "is_deleted = true"

# Sequence column for ordering records
SEQUENCE_COLUMN = "event_timestamp"
