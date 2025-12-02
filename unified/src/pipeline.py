# Databricks notebook source
# MAGIC %md
# MAGIC # Unified SCD Type 2 Pipeline
# MAGIC 
# MAGIC This pipeline implements **Stream 3 (Unification Layer)** - merging 3 source paths
# MAGIC for the **same Customer entity** into a single SCD Type 2 streaming table.
# MAGIC 
# MAGIC ## Key Features:
# MAGIC - **Schema Mapping**: Handles different column names across sources
# MAGIC - **Type Conversion**: Normalizes data types to unified target schema
# MAGIC - **Default Values**: Fills missing columns with appropriate defaults
# MAGIC 
# MAGIC ## Sources (ltc_insurance.raw_data_layer):
# MAGIC - `rdl_customer_hist_st` - Greenplum legacy (different column names, legacy SCD2 cols)
# MAGIC - `rdl_customer_init_st` - SQL Server initial (different column names)
# MAGIC - `rdl_customer` - Kafka CDC stream (missing source_system column)
# MAGIC 
# MAGIC ## Target:
# MAGIC - `unified_customer_scd2` - Unified schema with SCD Type 2 tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Imports & Setup

# COMMAND ----------

# Lakeflow Declarative Pipeline Imports
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    TimestampType,
    IntegerType,
    BooleanType,
    LongType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source Configuration

# COMMAND ----------

# Source catalog and schema
SOURCE_CATALOG = "ltc_insurance"
SOURCE_SCHEMA = "raw_data_layer"

# Source table names
GP_HISTORY_TABLE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.rdl_customer_hist_st"
SQL_INITIAL_TABLE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.rdl_customer_init_st"
CDC_STREAM_TABLE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.rdl_customer"

# Target table name (catalog/schema set in pipeline YAML)
TARGET_TABLE = "unified_customer_scd2"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1.5: Schema Mapping Configuration
# MAGIC 
# MAGIC Each source has different column names and data types. We define explicit mappings
# MAGIC to normalize all sources to a unified target schema.
# MAGIC 
# MAGIC ### Mapping Format:
# MAGIC ```python
# MAGIC "target_column": {
# MAGIC     "source_col": "source_column_name",  # Column name in source (None if missing)
# MAGIC     "dtype": "target_data_type",         # Target data type for casting
# MAGIC     "default": default_value,            # Default if source column is missing
# MAGIC     "nullable": True/False               # Whether nulls are allowed
# MAGIC }
# MAGIC ```

# COMMAND ----------

# =============================================================================
# UNIFIED TARGET SCHEMA - The "golden" schema all sources must conform to
# =============================================================================
TARGET_SCHEMA = {
    "customer_id":         {"dtype": "STRING",    "nullable": False},
    "customer_name":       {"dtype": "STRING",    "nullable": True},
    "date_of_birth":       {"dtype": "DATE",      "nullable": True},
    "email":               {"dtype": "STRING",    "nullable": True},
    "phone":               {"dtype": "STRING",    "nullable": True},
    "state":               {"dtype": "STRING",    "nullable": True},
    "zip_code":            {"dtype": "STRING",    "nullable": True},
    "status":              {"dtype": "STRING",    "nullable": True},
    "last_login":          {"dtype": "TIMESTAMP", "nullable": True},
    "session_count":       {"dtype": "INT",       "nullable": True},
    "page_views":          {"dtype": "INT",       "nullable": True},
    "is_deleted":          {"dtype": "BOOLEAN",   "nullable": False},
    "event_timestamp":     {"dtype": "TIMESTAMP", "nullable": False},
    "ingestion_timestamp": {"dtype": "TIMESTAMP", "nullable": True},
    "source_system":       {"dtype": "STRING",    "nullable": False},
    "_version":            {"dtype": "LONG",      "nullable": True},
}

# COMMAND ----------

# =============================================================================
# GREENPLUM LEGACY MAPPING
# - Uses different column names (e.g., cust_id, cust_name, dob)
# - Has legacy SCD2 columns to exclude (valid_from, valid_to, is_current)
# - Dates stored as strings in legacy format
# =============================================================================
GP_COLUMN_MAPPING = {
    # target_column: source_column (or None if need to derive/default)
    "customer_id":         {"source_col": "cust_id",           "transform": None},
    "customer_name":       {"source_col": "cust_name",         "transform": None},
    "date_of_birth":       {"source_col": "dob",               "transform": "to_date"},  # String -> Date
    "email":               {"source_col": "email_addr",        "transform": None},
    "phone":               {"source_col": "phone_num",         "transform": None},
    "state":               {"source_col": "state_cd",          "transform": None},
    "zip_code":            {"source_col": "zip",               "transform": None},
    "status":              {"source_col": "cust_status",       "transform": None},
    "last_login":          {"source_col": "last_login_ts",     "transform": "to_timestamp"},
    "session_count":       {"source_col": "sess_cnt",          "transform": "cast_int"},
    "page_views":          {"source_col": "pg_views",          "transform": "cast_int"},
    "is_deleted":          {"source_col": "is_deleted",        "transform": "cast_boolean"},
    "event_timestamp":     {"source_col": "event_ts",          "transform": "to_timestamp"},
    "ingestion_timestamp": {"source_col": "ingest_ts",         "transform": "to_timestamp"},
    "source_system":       {"source_col": "src_sys",           "transform": None},
    "_version":            {"source_col": "version_num",       "transform": "cast_long"},
}

# Columns to EXCLUDE from Greenplum (legacy SCD2 columns already computed there)
GP_EXCLUDE_COLUMNS = ["valid_from", "valid_to", "is_current"]

# COMMAND ----------

# =============================================================================
# SQL SERVER INITIAL MAPPING
# - Uses slightly different naming conventions
# - May have different date formats
# =============================================================================
SQL_COLUMN_MAPPING = {
    "customer_id":         {"source_col": "CustomerID",        "transform": "cast_string"},
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
    "is_deleted":          {"source_col": "IsDeleted",         "transform": "cast_boolean"},
    "event_timestamp":     {"source_col": "ModifiedDate",      "transform": "to_timestamp"},
    "ingestion_timestamp": {"source_col": "IngestionTime",     "transform": "to_timestamp"},
    "source_system":       {"source_col": "SourceSystem",      "transform": None},
    "_version":            {"source_col": "RowVersion",        "transform": "cast_long"},
}

# COMMAND ----------

# =============================================================================
# KAFKA CDC MAPPING
# - Uses snake_case (closest to target)
# - Missing source_system column (need default)
# =============================================================================
CDC_COLUMN_MAPPING = {
    "customer_id":         {"source_col": "customer_id",       "transform": None},
    "customer_name":       {"source_col": "customer_name",     "transform": None},
    "date_of_birth":       {"source_col": "date_of_birth",     "transform": None},
    "email":               {"source_col": "email",             "transform": None},
    "phone":               {"source_col": "phone",             "transform": None},
    "state":               {"source_col": "state",             "transform": None},
    "zip_code":            {"source_col": "zip_code",          "transform": None},
    "status":              {"source_col": "status",            "transform": None},
    "last_login":          {"source_col": "last_login",        "transform": None},
    "session_count":       {"source_col": "session_count",     "transform": None},
    "page_views":          {"source_col": "page_views",        "transform": None},
    "is_deleted":          {"source_col": "is_deleted",        "transform": None},
    "event_timestamp":     {"source_col": "event_timestamp",   "transform": None},
    "ingestion_timestamp": {"source_col": "ingestion_timestamp", "transform": None},
    "source_system":       {"source_col": None,                "transform": None, "default": "kafka_cdc"},
    "_version":            {"source_col": "_version",          "transform": None},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Mapping Helper Function

# COMMAND ----------

def apply_schema_mapping(df: DataFrame, column_mapping: dict, source_name: str) -> DataFrame:
    """
    Apply column mapping and type conversions to normalize a source DataFrame.
    
    Args:
        df: Source DataFrame
        column_mapping: Dictionary mapping target columns to source columns + transforms
        source_name: Name of source for error messages
    
    Returns:
        DataFrame with unified schema (columns renamed, types converted, defaults applied)
    """
    select_exprs = []
    
    for target_col, mapping in column_mapping.items():
        source_col = mapping.get("source_col")
        transform = mapping.get("transform")
        default_val = mapping.get("default")
        target_type = TARGET_SCHEMA[target_col]["dtype"]
        
        # Case 1: Source column is missing - use default value
        if source_col is None:
            if default_val is not None:
                expr = F.lit(default_val).cast(target_type).alias(target_col)
            else:
                expr = F.lit(None).cast(target_type).alias(target_col)
            select_exprs.append(expr)
            continue
        
        # Case 2: Source column exists - apply transformation if needed
        col_expr = F.col(source_col)
        
        if transform == "to_date":
            # Handle various date formats
            col_expr = F.coalesce(
                F.to_date(col_expr, "yyyy-MM-dd"),
                F.to_date(col_expr, "MM/dd/yyyy"),
                F.to_date(col_expr, "dd-MMM-yyyy"),
                F.to_date(col_expr)  # Default parsing
            )
        elif transform == "to_timestamp":
            # Handle various timestamp formats
            col_expr = F.coalesce(
                F.to_timestamp(col_expr, "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp(col_expr, "MM/dd/yyyy HH:mm:ss"),
                F.to_timestamp(col_expr)  # Default parsing
            )
        elif transform == "cast_string":
            col_expr = col_expr.cast("STRING")
        elif transform == "cast_int":
            col_expr = col_expr.cast("INT")
        elif transform == "cast_long":
            col_expr = col_expr.cast("LONG")
        elif transform == "cast_boolean":
            # Handle various boolean representations
            col_expr = F.when(
                col_expr.isin(["true", "True", "TRUE", "1", "Y", "yes"]), True
            ).when(
                col_expr.isin(["false", "False", "FALSE", "0", "N", "no"]), False
            ).otherwise(col_expr.cast("BOOLEAN"))
        
        # Apply final cast to target type
        col_expr = col_expr.cast(target_type).alias(target_col)
        select_exprs.append(col_expr)
    
    return df.select(*select_exprs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD2 Configuration

# COMMAND ----------

# SCD Type 2 settings for all 3 CDC flows
SCD2_CONFIG = {
    "keys": ["customer_id"],
    "stored_as_scd_type": "2",
    "except_column_list": ["source_system", "ingestion_timestamp", "_version"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Source Views with Schema Mapping
# MAGIC 
# MAGIC Each view applies the column mapping to normalize the source to the unified target schema:
# MAGIC - **Column Renaming**: e.g., `cust_id` → `customer_id`
# MAGIC - **Type Conversion**: e.g., STRING dates → DATE type
# MAGIC - **Default Values**: e.g., missing `source_system` → "kafka_cdc"

# COMMAND ----------

# View 1: Greenplum Legacy History
# - Applies column mapping (cust_id -> customer_id, dob -> date_of_birth, etc.)
# - Converts legacy date/timestamp formats
# - Excludes legacy SCD2 columns (valid_from, valid_to, is_current)
@dp.view(
    name="gp_customer_v",
    comment="Greenplum legacy customer history - schema mapped and normalized"
)
def gp_customer_view():
    df = spark.readStream.table(GP_HISTORY_TABLE)
    return apply_schema_mapping(df, GP_COLUMN_MAPPING, "greenplum")

# COMMAND ----------

# View 2: SQL Server Initial Snapshot
# - Applies column mapping (CustomerID -> customer_id, CustomerName -> customer_name, etc.)
# - Handles PascalCase to snake_case conversion
# - Converts .NET date formats
@dp.view(
    name="sql_customer_v",
    comment="SQL Server initial customer snapshot - schema mapped and normalized"
)
def sql_customer_view():
    df = spark.readStream.table(SQL_INITIAL_TABLE)
    return apply_schema_mapping(df, SQL_COLUMN_MAPPING, "sqlserver")

# COMMAND ----------

# View 3: Kafka CDC Stream
# - Schema mostly matches (snake_case)
# - Adds default value for missing source_system column
@dp.view(
    name="cdc_customer_v",
    comment="Kafka CDC customer stream - schema mapped with source_system default"
)
def cdc_customer_view():
    df = spark.readStream.table(CDC_STREAM_TABLE)
    return apply_schema_mapping(df, CDC_COLUMN_MAPPING, "kafka_cdc")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Target Streaming Table
# MAGIC 
# MAGIC Create the unified SCD2 streaming table that will receive data from all 3 CDC flows.

# COMMAND ----------

# Create the target streaming table for SCD Type 2
dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Unified SCD Type 2 - complete customer history from Greenplum legacy to real-time Kafka CDC"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: CDC Flows
# MAGIC 
# MAGIC Create 3 separate `create_auto_cdc_flow()` calls - all targeting the same streaming table.
# MAGIC 
# MAGIC **Per Databricks docs:**
# MAGIC > "Use append flow processing instead of UNION allows you to update the target table 
# MAGIC > incrementally without running a full refresh."

# COMMAND ----------

# CDC Flow 1: Greenplum Legacy History (oldest data)
dp.create_auto_cdc_flow(
    name="gp_to_unified_flow",
    target=TARGET_TABLE,
    source="gp_customer_v",
    keys=["customer_id"],
    sequence_by=F.col("event_timestamp"),
    stored_as_scd_type="2",
    apply_as_deletes=F.expr("is_deleted = true"),
    except_column_list=["source_system", "ingestion_timestamp", "_version"]
)

# COMMAND ----------

# CDC Flow 2: SQL Server Initial Snapshot (baseline state)
dp.create_auto_cdc_flow(
    name="sql_to_unified_flow",
    target=TARGET_TABLE,
    source="sql_customer_v",
    keys=["customer_id"],
    sequence_by=F.col("event_timestamp"),
    stored_as_scd_type="2",
    apply_as_deletes=F.expr("is_deleted = true"),
    except_column_list=["source_system", "ingestion_timestamp", "_version"]
)

# COMMAND ----------

# CDC Flow 3: Kafka CDC Stream (ongoing real-time changes)
dp.create_auto_cdc_flow(
    name="cdc_to_unified_flow",
    target=TARGET_TABLE,
    source="cdc_customer_v",
    keys=["customer_id"],
    sequence_by=F.col("event_timestamp"),
    stored_as_scd_type="2",
    apply_as_deletes=F.expr("is_deleted = true"),
    except_column_list=["source_system", "ingestion_timestamp", "_version"]
)
