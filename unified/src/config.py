# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Module
# MAGIC 
# MAGIC Loads configuration from metadata and provides convenient accessors.
# MAGIC This module bridges the metadata JSON with the pipeline code.

# COMMAND ----------

# MAGIC %run ./metadata_loader

# COMMAND ----------

# =============================================================================
# SOURCE CONFIGURATION (loaded from metadata)
# =============================================================================

# Source table fully-qualified names
GP_HISTORY_TABLE = get_source_table_fqn("greenplum")
SQL_INITIAL_TABLE = get_source_table_fqn("sqlserver")
CDC_STREAM_TABLE = get_source_table_fqn("kafka_cdc")

# Source view names
GP_VIEW_NAME = get_source_config("greenplum")["view_name"]
SQL_VIEW_NAME = get_source_config("sqlserver")["view_name"]
CDC_VIEW_NAME = get_source_config("kafka_cdc")["view_name"]

# Source flow names
GP_FLOW_NAME = get_source_config("greenplum")["flow_name"]
SQL_FLOW_NAME = get_source_config("sqlserver")["flow_name"]
CDC_FLOW_NAME = get_source_config("kafka_cdc")["flow_name"]

# =============================================================================
# TARGET CONFIGURATION (loaded from metadata)
# =============================================================================

TARGET_TABLE = get_target_table_name()

# =============================================================================
# SCD TYPE 2 CONFIGURATION (loaded from metadata)
# =============================================================================

SCD2_KEYS = get_scd2_keys()
SEQUENCE_COLUMN = get_sequence_column()
DELETE_CONDITION = get_delete_condition()
SCD2_EXCEPT_COLUMNS = get_except_columns()

# =============================================================================
# VALIDATION
# =============================================================================

# Validate metadata on module load
validate_metadata()

