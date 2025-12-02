# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Mapping Module
# MAGIC 
# MAGIC Defines the unified target schema and source-to-target column mappings.
# MAGIC 
# MAGIC ## Mapping Format:
# MAGIC ```python
# MAGIC "target_column": {
# MAGIC     "source_col": "source_column_name",  # Column name in source (None if missing)
# MAGIC     "transform": "transformation_type",   # Type conversion to apply
# MAGIC     "default": default_value              # Default if source column is missing
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC ## Supported Transforms:
# MAGIC - `None` - No transformation, direct pass-through
# MAGIC - `to_date` - Parse string to DATE (multiple formats)
# MAGIC - `to_timestamp` - Parse string to TIMESTAMP (multiple formats)
# MAGIC - `cast_string` - Cast to STRING
# MAGIC - `cast_int` - Cast to INT
# MAGIC - `cast_long` - Cast to LONG
# MAGIC - `cast_boolean` - Normalize boolean (handles Y/N, 1/0, true/false)

# COMMAND ----------

# =============================================================================
# UNIFIED TARGET SCHEMA
# =============================================================================
# The "golden" schema that all sources must conform to.
# This defines the final shape of the unified_customer_scd2 table.

TARGET_SCHEMA = {
    # Primary Key
    "customer_id": {
        "dtype": "STRING",
        "nullable": False,
        "description": "Unique customer identifier"
    },
    
    # Business Columns (tracked for SCD2 changes)
    "customer_name": {
        "dtype": "STRING",
        "nullable": True,
        "description": "Full customer name"
    },
    "date_of_birth": {
        "dtype": "DATE",
        "nullable": True,
        "description": "Customer date of birth"
    },
    "email": {
        "dtype": "STRING",
        "nullable": True,
        "description": "Primary email address"
    },
    "phone": {
        "dtype": "STRING",
        "nullable": True,
        "description": "Primary phone number"
    },
    "state": {
        "dtype": "STRING",
        "nullable": True,
        "description": "State code (2-letter)"
    },
    "zip_code": {
        "dtype": "STRING",
        "nullable": True,
        "description": "ZIP/Postal code"
    },
    "status": {
        "dtype": "STRING",
        "nullable": True,
        "description": "Customer status (active, inactive, etc.)"
    },
    "last_login": {
        "dtype": "TIMESTAMP",
        "nullable": True,
        "description": "Last login timestamp"
    },
    "session_count": {
        "dtype": "INT",
        "nullable": True,
        "description": "Total session count"
    },
    "page_views": {
        "dtype": "INT",
        "nullable": True,
        "description": "Total page views"
    },
    
    # Delete Detection
    "is_deleted": {
        "dtype": "BOOLEAN",
        "nullable": False,
        "description": "Soft delete flag"
    },
    
    # Sequencing & Metadata (excluded from SCD2 change tracking)
    "event_timestamp": {
        "dtype": "TIMESTAMP",
        "nullable": False,
        "description": "Event/change timestamp for SCD2 sequencing"
    },
    "ingestion_timestamp": {
        "dtype": "TIMESTAMP",
        "nullable": True,
        "description": "When record was ingested into Bronze"
    },
    "source_system": {
        "dtype": "STRING",
        "nullable": False,
        "description": "Origin system identifier"
    },
    "_version": {
        "dtype": "LONG",
        "nullable": True,
        "description": "Source record version"
    },
}

# COMMAND ----------

# =============================================================================
# GREENPLUM LEGACY MAPPING
# =============================================================================
# Greenplum uses abbreviated column names and legacy conventions.
# Also has legacy SCD2 columns (valid_from, valid_to, is_current) to exclude.

GP_COLUMN_MAPPING = {
    # Primary Key
    "customer_id":         {"source_col": "cust_id",           "transform": None},
    
    # Business Columns
    "customer_name":       {"source_col": "cust_name",         "transform": None},
    "date_of_birth":       {"source_col": "dob",               "transform": "to_date"},
    "email":               {"source_col": "email_addr",        "transform": None},
    "phone":               {"source_col": "phone_num",         "transform": None},
    "state":               {"source_col": "state_cd",          "transform": None},
    "zip_code":            {"source_col": "zip",               "transform": None},
    "status":              {"source_col": "cust_status",       "transform": None},
    "last_login":          {"source_col": "last_login_ts",     "transform": "to_timestamp"},
    "session_count":       {"source_col": "sess_cnt",          "transform": "cast_int"},
    "page_views":          {"source_col": "pg_views",          "transform": "cast_int"},
    
    # Delete Detection
    "is_deleted":          {"source_col": "is_deleted",        "transform": "cast_boolean"},
    
    # Metadata
    "event_timestamp":     {"source_col": "event_ts",          "transform": "to_timestamp"},
    "ingestion_timestamp": {"source_col": "ingest_ts",         "transform": "to_timestamp"},
    "source_system":       {"source_col": "src_sys",           "transform": None},
    "_version":            {"source_col": "version_num",       "transform": "cast_long"},
}

# Columns to EXCLUDE from Greenplum source (legacy SCD2 columns)
GP_EXCLUDE_COLUMNS = ["valid_from", "valid_to", "is_current"]

# COMMAND ----------

# =============================================================================
# SQL SERVER INITIAL MAPPING
# =============================================================================
# SQL Server uses PascalCase naming convention typical of .NET applications.

SQL_COLUMN_MAPPING = {
    # Primary Key (INT in source, STRING in target)
    "customer_id":         {"source_col": "CustomerID",        "transform": "cast_string"},
    
    # Business Columns
    "customer_name":       {"source_col": "CustomerName",      "transform": None},
    "date_of_birth":       {"source_col": "DateOfBirth",       "transform": "to_date"},
    "email":               {"source_col": "EmailAddress",      "transform": None},
    "phone":               {"source_col": "PhoneNumber",       "transform": None},
    "state":               {"source_col": "StateCode",         "transform": None},
    "zip_code":            {"source_col": "ZipCode",           "transform": None},
    "status":              {"source_col": "Status",            "transform": None},
    "last_login":          {"source_col": "LastLoginDate",     "transform": "to_timestamp"},
    "session_count":       {"source_col": "SessionCount",      "transform": "cast_int"},
    "page_views":          {"source_col": "PageViews",         "transform": "cast_int"},
    
    # Delete Detection (BIT in SQL Server)
    "is_deleted":          {"source_col": "IsDeleted",         "transform": "cast_boolean"},
    
    # Metadata
    "event_timestamp":     {"source_col": "ModifiedDate",      "transform": "to_timestamp"},
    "ingestion_timestamp": {"source_col": "IngestionTime",     "transform": "to_timestamp"},
    "source_system":       {"source_col": "SourceSystem",      "transform": None},
    "_version":            {"source_col": "RowVersion",        "transform": "cast_long"},
}

# COMMAND ----------

# =============================================================================
# KAFKA CDC MAPPING
# =============================================================================
# Kafka CDC uses snake_case (closest to target schema).
# Missing source_system column - needs default value.

CDC_COLUMN_MAPPING = {
    # Primary Key
    "customer_id":         {"source_col": "customer_id",         "transform": None},
    
    # Business Columns (direct mapping)
    "customer_name":       {"source_col": "customer_name",       "transform": None},
    "date_of_birth":       {"source_col": "date_of_birth",       "transform": None},
    "email":               {"source_col": "email",               "transform": None},
    "phone":               {"source_col": "phone",               "transform": None},
    "state":               {"source_col": "state",               "transform": None},
    "zip_code":            {"source_col": "zip_code",            "transform": None},
    "status":              {"source_col": "status",              "transform": None},
    "last_login":          {"source_col": "last_login",          "transform": None},
    "session_count":       {"source_col": "session_count",       "transform": None},
    "page_views":          {"source_col": "page_views",          "transform": None},
    
    # Delete Detection
    "is_deleted":          {"source_col": "is_deleted",          "transform": None},
    
    # Metadata
    "event_timestamp":     {"source_col": "event_timestamp",     "transform": None},
    "ingestion_timestamp": {"source_col": "ingestion_timestamp", "transform": None},
    "source_system":       {"source_col": None,                  "transform": None, "default": "kafka_cdc"},
    "_version":            {"source_col": "_version",            "transform": None},
}
