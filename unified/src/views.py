# Databricks notebook source
# MAGIC %md
# MAGIC # Source Views Module
# MAGIC 
# MAGIC Defines the source views that normalize each input source to the unified schema.
# MAGIC Each view reads from a Bronze streaming table and applies schema mapping.

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
# MAGIC - **Source**: `rdl_customer_hist_st`
# MAGIC - **Characteristics**: Oldest historical data, one-time load
# MAGIC - **Schema Issues**:
# MAGIC   - Abbreviated column names (cust_id, dob, etc.)
# MAGIC   - Legacy SCD2 columns to exclude (valid_from, valid_to, is_current)
# MAGIC   - Dates stored as strings

# COMMAND ----------

@dp.view(
    name="gp_customer_v",
    comment="Greenplum legacy customer history - schema mapped and normalized"
)
def gp_customer_view():
    """
    Normalize Greenplum legacy data to unified schema.
    
    Transformations applied:
    - cust_id -> customer_id
    - dob -> date_of_birth (STRING to DATE)
    - event_ts -> event_timestamp (STRING to TIMESTAMP)
    - Excludes: valid_from, valid_to, is_current
    """
    df = spark.readStream.table(GP_HISTORY_TABLE)
    return apply_schema_mapping(df, GP_COLUMN_MAPPING, "greenplum")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 2: SQL Server Initial Snapshot
# MAGIC 
# MAGIC - **Source**: `rdl_customer_init_st`
# MAGIC - **Characteristics**: Baseline state, one-time load
# MAGIC - **Schema Issues**:
# MAGIC   - PascalCase column names (CustomerID, CustomerName, etc.)
# MAGIC   - CustomerID is INT, needs cast to STRING
# MAGIC   - .NET date formats

# COMMAND ----------

@dp.view(
    name="sql_customer_v",
    comment="SQL Server initial customer snapshot - schema mapped and normalized"
)
def sql_customer_view():
    """
    Normalize SQL Server initial snapshot to unified schema.
    
    Transformations applied:
    - CustomerID -> customer_id (INT to STRING)
    - CustomerName -> customer_name
    - DateOfBirth -> date_of_birth
    - ModifiedDate -> event_timestamp
    """
    df = spark.readStream.table(SQL_INITIAL_TABLE)
    return apply_schema_mapping(df, SQL_COLUMN_MAPPING, "sqlserver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 3: Kafka CDC Stream
# MAGIC 
# MAGIC - **Source**: `rdl_customer`
# MAGIC - **Characteristics**: Ongoing real-time changes
# MAGIC - **Schema Issues**:
# MAGIC   - Missing source_system column (needs default value)
# MAGIC   - Otherwise matches target schema (snake_case)

# COMMAND ----------

@dp.view(
    name="cdc_customer_v",
    comment="Kafka CDC customer stream - schema mapped with source_system default"
)
def cdc_customer_view():
    """
    Normalize Kafka CDC stream to unified schema.
    
    Transformations applied:
    - source_system -> defaults to "kafka_cdc" (column missing in source)
    - All other columns: direct mapping (schema already matches)
    """
    df = spark.readStream.table(CDC_STREAM_TABLE)
    return apply_schema_mapping(df, CDC_COLUMN_MAPPING, "kafka_cdc")
