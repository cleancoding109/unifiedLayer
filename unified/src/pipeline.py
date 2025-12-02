# Databricks notebook source
# MAGIC %md
# MAGIC # Unified SCD Type 2 Pipeline
# MAGIC 
# MAGIC This pipeline implements **Stream 3 (Unification Layer)** - merging 3 source paths
# MAGIC for the **same Customer entity** into a single SCD Type 2 streaming table.
# MAGIC 
# MAGIC ## MVP Scope
# MAGIC - Streams 1 & 2 (Bronze Raw & Bronze Processed) are already built upstream
# MAGIC - This pipeline only handles the unification step
# MAGIC 
# MAGIC ## Sources (ltc_insurance.raw_data_layer):
# MAGIC - `rdl_customer_hist_st` - Greenplum legacy history (oldest data, one-time load)
# MAGIC - `rdl_customer_init_st` - SQL Server initial snapshot (baseline, one-time load)
# MAGIC - `rdl_customer` - Kafka CDC stream (ongoing real-time changes)
# MAGIC 
# MAGIC ## Target:
# MAGIC - `unified_customer_scd2` - Complete customer history from legacy to real-time
# MAGIC 
# MAGIC ## Architecture:
# MAGIC - 3 source views (normalize each source)
# MAGIC - 3 CDC flows (all target same streaming table)
# MAGIC - Per Databricks docs: "Use multiple CDC flows instead of UNION for incremental updates"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Imports & Setup

# COMMAND ----------

# Lakeflow Declarative Pipeline Imports
import databricks.sdk.runtime.pipelines as dp
from pyspark.sql import functions as F
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
# MAGIC ### Configuration

# COMMAND ----------

# Source catalog and schema (where source tables are located)
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
# MAGIC ### Common Column List
# MAGIC 
# MAGIC All 3 sources have aligned schemas. We select these common columns:

# COMMAND ----------

# Columns to include in all source views (order matters)
COMMON_COLUMNS = [
    "customer_id",       # Primary Key
    "customer_name",     # Track changes
    "date_of_birth",     # Track changes
    "email",             # Track changes
    "phone",             # Track changes
    "state",             # Track changes
    "zip_code",          # Track changes
    "status",            # Track changes
    "last_login",        # Track changes
    "session_count",     # Track changes
    "page_views",        # Track changes
    "is_deleted",        # Delete detection
    "event_timestamp",   # Sequence column
    "ingestion_timestamp",  # Exclude from tracking
    "source_system",     # Exclude from tracking
    "_version",          # Exclude from tracking
]

# Columns excluded from Greenplum source (legacy SCD2 columns)
GP_EXCLUDE_COLUMNS = ["valid_from", "valid_to", "is_current"]

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
# MAGIC ## Step 2: Source Views
# MAGIC 
# MAGIC Create 3 views that normalize each source to a common schema:
# MAGIC - `gp_customer_v` - Greenplum history (exclude legacy SCD2 columns)
# MAGIC - `sql_customer_v` - SQL Server initial (passthrough)
# MAGIC - `cdc_customer_v` - Kafka CDC (add source_system literal)

# COMMAND ----------

# View 1: Greenplum Legacy History
# Excludes: valid_from, valid_to, is_current (legacy SCD2 columns computed in Greenplum)
@dp.view(
    name="gp_customer_v",
    comment="Greenplum legacy customer history - normalized (excludes legacy SCD2 columns)"
)
def gp_customer_view():
    return (
        spark.readStream
        .table(GP_HISTORY_TABLE)
        .select(
            "customer_id",
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
            "is_deleted",
            "event_timestamp",
            "ingestion_timestamp",
            "source_system",
            "_version"
            # Excluded: valid_from, valid_to, is_current
        )
    )

# COMMAND ----------

# View 2: SQL Server Initial Snapshot
# Passthrough - no transformation needed (schema already matches)
@dp.view(
    name="sql_customer_v",
    comment="SQL Server initial customer snapshot - baseline state"
)
def sql_customer_view():
    return (
        spark.readStream
        .table(SQL_INITIAL_TABLE)
        .select(
            "customer_id",
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
            "is_deleted",
            "event_timestamp",
            "ingestion_timestamp",
            "source_system",
            "_version"
        )
    )

# COMMAND ----------

# View 3: Kafka CDC Stream
# Adds source_system column (missing in source table)
@dp.view(
    name="cdc_customer_v",
    comment="Kafka CDC customer stream - ongoing real-time changes"
)
def cdc_customer_view():
    return (
        spark.readStream
        .table(CDC_STREAM_TABLE)
        .select(
            "customer_id",
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
            "is_deleted",
            "event_timestamp",
            "ingestion_timestamp",
            F.lit("kafka_cdc").alias("source_system"),  # Add missing column
            "_version"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Target Streaming Table
# MAGIC 
# MAGIC Create the unified SCD2 streaming table that will receive data from all 3 CDC flows.
# MAGIC 
# MAGIC ```python
# MAGIC dp.create_streaming_table(
# MAGIC     name="unified_customer_scd2",
# MAGIC     comment="Unified SCD Type 2 - complete customer history from Greenplum to real-time CDC"
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC _TODO: Implement streaming table creation_

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
# MAGIC 
# MAGIC **Configuration for all flows:**
# MAGIC - `keys`: ["customer_id"]
# MAGIC - `sequence_by`: event_timestamp
# MAGIC - `stored_as_scd_type`: "2"
# MAGIC - `apply_as_deletes`: is_deleted = true
# MAGIC - `except_column_list`: ["source_system", "ingestion_timestamp", "_version"]
# MAGIC 
# MAGIC _TODO: Implement 3 CDC flows_
