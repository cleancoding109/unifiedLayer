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
# MAGIC - `metadata_loader.py` - Loads pipeline metadata from JSON, provides accessor functions
# MAGIC - `transformations.py` - Schema mapping transformation logic
# MAGIC - `views.py` - Source view definitions
# MAGIC - `pipeline.py` - **This file** - Target table and CDC flows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

try:
    import metadata_loader
    import views
except ImportError:
    from . import metadata_loader
    from . import views

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Target Streaming Table
# MAGIC 
# MAGIC Create the unified SCD2 streaming table that receives data from all 3 CDC flows.
# MAGIC Lakeflow automatically adds `__START_AT` and `__END_AT` columns for SCD2 tracking.

# COMMAND ----------

dp.create_streaming_table(
    name=metadata_loader.get_target_table_name(),
    comment="Unified SCD Type 2 - complete customer history from Greenplum legacy to real-time Kafka CDC"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## CDC Flows
# MAGIC 
# MAGIC Dynamically create CDC flows for all configured sources.
# MAGIC All flows target the same unified SCD2 table.

# COMMAND ----------

# Iterate through all configured sources and create CDC flows
for source_key, source_config in metadata_loader.get_all_sources().items():
    
    dp.create_auto_cdc_flow(
        name=source_config["flow_name"],
        target=metadata_loader.get_target_table_name(),
        source=source_config["view_name"],
        keys=metadata_loader.get_scd2_keys(),
        sequence_by=F.col(metadata_loader.get_sequence_column()),
        stored_as_scd_type="2",
        apply_as_deletes=F.expr(metadata_loader.get_delete_condition()),
        except_column_list=metadata_loader.get_except_columns()
    )


