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
# MAGIC ## Multi-Target Support
# MAGIC 
# MAGIC Loop through ALL targets defined in metadata and create:
# MAGIC 1. A streaming table for each enabled target
# MAGIC 2. CDC flows for each target's enabled source mappings
# MAGIC 
# MAGIC This supports the "many-to-many" pattern where one pipeline can write to multiple streaming tables.
# MAGIC Lakeflow automatically adds `__START_AT` and `__END_AT` columns for SCD2 tracking.

# COMMAND ----------

# Get all targets from metadata (supports multi-target pipelines)
all_targets = metadata_loader.get_all_targets()

for target_idx, target in enumerate(all_targets):
    # Skip disabled targets (if enabled flag exists and is False)
    if not target.get("enabled", True):
        continue
    
    target_name = metadata_loader.get_target_name(target_idx)
    
    # Create the streaming table for this target
    dp.create_streaming_table(
        name=target_name,
        comment=target.get("comment", f"SCD Type 2 streaming table for {target_name}")
    )
    
    # Create CDC flows for all enabled source mappings of this target
    # This allows sequential merging of sources one at a time:
    # 1. First enable greenplum -> deploy & run -> merge historical data
    # 2. Then enable sqlserver -> deploy & run -> merge initial snapshot
    # 3. Finally enable kafka_cdc -> deploy & run -> start real-time streaming
    for source_key, source_mapping in metadata_loader.get_enabled_source_mappings(target_idx).items():
        
        # Build kwargs for create_auto_cdc_flow
        cdc_flow_kwargs = {
            "name": source_mapping["flow_name"],
            "target": target_name,
            "source": source_mapping["view_name"],
            "keys": metadata_loader.get_scd2_keys(target_idx),
            "sequence_by": F.col(metadata_loader.get_sequence_column(target_idx)),
            "stored_as_scd_type": "2",
            "apply_as_deletes": F.expr(metadata_loader.get_delete_condition(target_idx)),
        }
        
        # Add except_column_list only if there are columns to exclude
        except_cols = metadata_loader.get_except_columns(target_idx)
        if except_cols:
            cdc_flow_kwargs["except_column_list"] = except_cols
        
        # Add track_history_except_column_list for columns that shouldn't trigger new history records
        # These columns are stored but changes don't create new SCD2 versions
        track_history_except = metadata_loader.get_track_history_except_columns(target_idx)
        if track_history_except:
            cdc_flow_kwargs["track_history_except_column_list"] = track_history_except
        
        dp.create_auto_cdc_flow(**cdc_flow_kwargs)


