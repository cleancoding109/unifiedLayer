# Databricks notebook source
# MAGIC %md
# MAGIC # Unified SCD Type 2 Pipeline
# MAGIC 
# MAGIC This is the main orchestration notebook for the Unified SCD Type 2 Pipeline.
# MAGIC It imports all modules and defines the target table and CDC flows.
# MAGIC 
# MAGIC ## Architecture:
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │                         UNIFIED SCD2 PIPELINE                           │
# MAGIC ├─────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                         │
# MAGIC │  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐       │
# MAGIC │  │   Greenplum     │   │   SQL Server    │   │   Kafka CDC     │       │
# MAGIC │  │  rdl_customer   │   │  rdl_customer   │   │  rdl_customer   │       │
# MAGIC │  │   _hist_st      │   │   _init_st      │   │                 │       │
# MAGIC │  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘       │
# MAGIC │           │                     │                     │                │
# MAGIC │           ▼                     ▼                     ▼                │
# MAGIC │  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐       │
# MAGIC │  │  gp_customer_v  │   │ sql_customer_v  │   │ cdc_customer_v  │       │
# MAGIC │  │  (schema map)   │   │  (schema map)   │   │  (schema map)   │       │
# MAGIC │  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘       │
# MAGIC │           │                     │                     │                │
# MAGIC │           │    CDC Flow 1       │    CDC Flow 2       │    CDC Flow 3  │
# MAGIC │           └─────────────────────┼─────────────────────┘                │
# MAGIC │                                 ▼                                      │
# MAGIC │                    ┌───────────────────────┐                           │
# MAGIC │                    │ unified_customer_scd2 │                           │
# MAGIC │                    │    (SCD Type 2)       │                           │
# MAGIC │                    │  __START_AT, __END_AT │                           │
# MAGIC │                    └───────────────────────┘                           │
# MAGIC │                                                                         │
# MAGIC └─────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC ## Module Structure:
# MAGIC - `config.py` - Source/target configuration, SCD2 settings
# MAGIC - `schema.py` - Target schema and column mappings
# MAGIC - `transformations.py` - Schema mapping transformation logic
# MAGIC - `views.py` - Source view definitions
# MAGIC - `pipeline.py` - **This file** - Target table and CDC flows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Modules

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./views

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Target Streaming Table
# MAGIC 
# MAGIC Create the unified SCD2 streaming table that receives data from all 3 CDC flows.
# MAGIC Lakeflow automatically adds `__START_AT` and `__END_AT` columns for SCD2 tracking.

# COMMAND ----------

dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Unified SCD Type 2 - complete customer history from Greenplum legacy to real-time Kafka CDC"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## CDC Flows
# MAGIC 
# MAGIC Create 3 separate CDC flows - all targeting the same streaming table.
# MAGIC 
# MAGIC **Why multiple flows instead of UNION?**
# MAGIC > Per Databricks docs: "Use append flow processing instead of UNION allows you to 
# MAGIC > update the target table incrementally without running a full refresh."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flow 1: Greenplum Legacy History
# MAGIC Oldest historical data - one-time load of legacy customer records.

# COMMAND ----------

dp.create_auto_cdc_flow(
    name=GP_FLOW_NAME,
    target=TARGET_TABLE,
    source=GP_VIEW_NAME,
    keys=SCD2_KEYS,
    sequence_by=F.col(SEQUENCE_COLUMN),
    stored_as_scd_type="2",
    apply_as_deletes=F.expr(DELETE_CONDITION),
    except_column_list=SCD2_EXCEPT_COLUMNS
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flow 2: SQL Server Initial Snapshot
# MAGIC Baseline state - one-time load of initial customer snapshot.

# COMMAND ----------

dp.create_auto_cdc_flow(
    name=SQL_FLOW_NAME,
    target=TARGET_TABLE,
    source=SQL_VIEW_NAME,
    keys=SCD2_KEYS,
    sequence_by=F.col(SEQUENCE_COLUMN),
    stored_as_scd_type="2",
    apply_as_deletes=F.expr(DELETE_CONDITION),
    except_column_list=SCD2_EXCEPT_COLUMNS
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flow 3: Kafka CDC Stream
# MAGIC Ongoing real-time changes - continuous ingestion of CDC events.

# COMMAND ----------

dp.create_auto_cdc_flow(
    name=CDC_FLOW_NAME,
    target=TARGET_TABLE,
    source=CDC_VIEW_NAME,
    keys=SCD2_KEYS,
    sequence_by=F.col(SEQUENCE_COLUMN),
    stored_as_scd_type="2",
    apply_as_deletes=F.expr(DELETE_CONDITION),
    except_column_list=SCD2_EXCEPT_COLUMNS
)
