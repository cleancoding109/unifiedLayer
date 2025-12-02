# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Configuration
# MAGIC Configuration settings for the Unified SCD2 Pipeline.
# MAGIC 
# MAGIC **Note:** In Lakeflow pipelines, catalog/schema come from the pipeline settings.
# MAGIC These defaults are used for local testing only.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog and Schema Configuration
# MAGIC 
# MAGIC These values are configured in the pipeline YAML and accessed via:
# MAGIC - `spark.conf.get("catalog")` within Lakeflow
# MAGIC - Widget parameters for standalone execution

# COMMAND ----------

# Default values for local development/testing
DEFAULT_SOURCE_CATALOG = "ltc_insurance"
DEFAULT_SOURCE_SCHEMA = "raw_data_layer"
DEFAULT_TARGET_CATALOG = "ltc_insurance"  # Overridden by pipeline config
DEFAULT_TARGET_SCHEMA = "unified_dev"     # Overridden by pipeline config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Tables Configuration

# COMMAND ----------

# Source table definitions
SOURCE_TABLES = {
    "customers": {
        "table_name": "rdl_customers",
        "entity_type": "customer",
        "id_column": "customer_id",
        "source_system": "greenplum",
    },
    "policies": {
        "table_name": "rdl_policies", 
        "entity_type": "policy",
        "id_column": "policy_id",
        "source_system": "sqlserver",
    },
    "claims": {
        "table_name": "rdl_claims",
        "entity_type": "claim", 
        "id_column": "claim_id",
        "source_system": "kafka",
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target Table Configuration

# COMMAND ----------

# Target table name
TARGET_TABLE_NAME = "unified_scd2_target"

# SCD2 Configuration
SCD2_CONFIG = {
    "keys": ["entity_type", "entity_id"],
    "sequence_by": "event_timestamp",
    "apply_as_deletes": "is_deleted = true",
    "track_history_except_columns": ["source_system"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_source_table_path(source_key: str, catalog: str = None, schema: str = None) -> str:
    """
    Get the fully qualified table path for a source.
    
    Args:
        source_key: Key from SOURCE_TABLES (customers, policies, claims)
        catalog: Override catalog (defaults to DEFAULT_SOURCE_CATALOG)
        schema: Override schema (defaults to DEFAULT_SOURCE_SCHEMA)
    
    Returns:
        Fully qualified table path (catalog.schema.table_name)
    """
    catalog = catalog or DEFAULT_SOURCE_CATALOG
    schema = schema or DEFAULT_SOURCE_SCHEMA
    table_name = SOURCE_TABLES[source_key]["table_name"]
    return f"{catalog}.{schema}.{table_name}"


def get_source_config(source_key: str) -> dict:
    """
    Get the full configuration for a source.
    
    Args:
        source_key: Key from SOURCE_TABLES
        
    Returns:
        Configuration dictionary for the source
    """
    return SOURCE_TABLES[source_key]
