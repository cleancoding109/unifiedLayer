# Databricks notebook source
# MAGIC %md
# MAGIC # Source Views Module
# MAGIC 
# MAGIC Defines the source views that normalize each input source to the unified schema.
# MAGIC Each view reads from a Bronze streaming table and applies schema mapping.
# MAGIC 
# MAGIC View names and source tables are loaded from metadata.

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./schema

# COMMAND ----------

# MAGIC %run ./transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 1: Greenplum Legacy History
# MAGIC 
# MAGIC - **Source**: Loaded from metadata
# MAGIC - **Characteristics**: Oldest historical data, one-time load
# MAGIC - **Transformations**: Column renaming, date parsing, excludes legacy SCD2 columns

# COMMAND ----------

@dp.view(
    name=GP_VIEW_NAME,
    comment=get_source_config("greenplum").get("description", "Greenplum source view")
)
def gp_customer_view():
    """Normalize Greenplum legacy data to unified schema."""
    df = spark.readStream.table(GP_HISTORY_TABLE)
    return apply_schema_mapping(df, GP_COLUMN_MAPPING, "greenplum")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 2: SQL Server Initial Snapshot
# MAGIC 
# MAGIC - **Source**: Loaded from metadata
# MAGIC - **Characteristics**: Baseline state, one-time load
# MAGIC - **Transformations**: PascalCase to snake_case, INT to STRING for IDs

# COMMAND ----------

@dp.view(
    name=SQL_VIEW_NAME,
    comment=get_source_config("sqlserver").get("description", "SQL Server source view")
)
def sql_customer_view():
    """Normalize SQL Server initial snapshot to unified schema."""
    df = spark.readStream.table(SQL_INITIAL_TABLE)
    return apply_schema_mapping(df, SQL_COLUMN_MAPPING, "sqlserver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 3: Kafka CDC Stream
# MAGIC 
# MAGIC - **Source**: Loaded from metadata
# MAGIC - **Characteristics**: Ongoing real-time changes
# MAGIC - **Transformations**: Adds default source_system value

# COMMAND ----------

@dp.view(
    name=CDC_VIEW_NAME,
    comment=get_source_config("kafka_cdc").get("description", "Kafka CDC source view")
)
def cdc_customer_view():
    """Normalize Kafka CDC stream to unified schema."""
    df = spark.readStream.table(CDC_STREAM_TABLE)
    return apply_schema_mapping(df, CDC_COLUMN_MAPPING, "kafka_cdc")
