# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata Loader Module
# MAGIC 
# MAGIC Loads pipeline configuration from the metadata JSON file.
# MAGIC This provides a single source of truth for all pipeline settings.

# COMMAND ----------

import json
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Metadata JSON

# COMMAND ----------

def _get_metadata_path() -> str:
    """
    Get the path to the metadata JSON file.
    
    In Databricks, notebooks are run from the workspace, so we need to
    construct the path relative to the current notebook location.
    """
    # When running in Databricks, use the notebook's directory
    # The %run magic handles relative paths, but for JSON we need absolute
    try:
        # Try to get the notebook path from Databricks context
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        # Go up one level from src/ to get to src/metadata/
        base_path = "/Workspace" + "/".join(notebook_path.split("/")[:-1])
        return f"{base_path}/metadata/pipeline_metadata.json"
    except:
        # Fallback for local development/testing
        return os.path.join(os.path.dirname(__file__), "metadata", "pipeline_metadata.json")


def load_metadata() -> dict:
    """
    Load the pipeline metadata from JSON file.
    
    Returns:
        dict: Complete pipeline metadata
    """
    try:
        # In Databricks, read from workspace file system
        metadata_path = _get_metadata_path()
        with open(metadata_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        # Fallback: try reading as a resource
        print(f"Warning: Could not load metadata from file: {e}")
        print("Using embedded metadata...")
        return _get_embedded_metadata()


def _get_embedded_metadata() -> dict:
    """
    Fallback embedded metadata in case file cannot be loaded.
    This ensures the pipeline can still run even if the JSON file is inaccessible.
    """
    return {
        "pipeline": {
            "name": "unified_scd2_pipeline",
            "version": "2.0.0",
            "catalog": "ltc_insurance",
            "target_schema": "unified_dev"
        },
        "target": {
            "table_name": "unified_customer_scd2",
            "keys": ["customer_id"],
            "sequence_by": "event_timestamp",
            "delete_condition": "is_deleted = true",
            "except_columns": ["source_system", "ingestion_timestamp", "_version"]
        },
        "sources": {
            "greenplum": {
                "table_name": "rdl_customer_hist_st",
                "catalog": "ltc_insurance",
                "schema": "raw_data_layer",
                "view_name": "gp_customer_v",
                "flow_name": "gp_to_unified_flow"
            },
            "sqlserver": {
                "table_name": "rdl_customer_init_st",
                "catalog": "ltc_insurance",
                "schema": "raw_data_layer",
                "view_name": "sql_customer_v",
                "flow_name": "sql_to_unified_flow"
            },
            "kafka_cdc": {
                "table_name": "rdl_customer",
                "catalog": "ltc_insurance",
                "schema": "raw_data_layer",
                "view_name": "cdc_customer_v",
                "flow_name": "cdc_to_unified_flow"
            }
        }
    }

# COMMAND ----------

# Load metadata on module import
METADATA = load_metadata()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessor Functions
# MAGIC 
# MAGIC Convenience functions to access specific parts of the metadata.

# COMMAND ----------

def get_pipeline_config() -> dict:
    """Get pipeline-level configuration."""
    return METADATA.get("pipeline", {})


def get_target_config() -> dict:
    """Get target table configuration."""
    return METADATA.get("target", {})


def get_source_config(source_name: str) -> dict:
    """
    Get configuration for a specific source.
    
    Args:
        source_name: One of 'greenplum', 'sqlserver', 'kafka_cdc'
    
    Returns:
        dict: Source configuration including table name, view name, column mapping
    """
    sources = METADATA.get("sources", {})
    if source_name not in sources:
        raise ValueError(f"Unknown source: {source_name}. Valid sources: {list(sources.keys())}")
    return sources[source_name]


def get_all_sources() -> dict:
    """Get configuration for all sources."""
    return METADATA.get("sources", {})


def get_target_schema() -> dict:
    """Get the unified target schema definition."""
    return METADATA.get("target_schema", {}).get("columns", {})


def get_column_mapping(source_name: str) -> dict:
    """
    Get the column mapping for a specific source.
    
    Args:
        source_name: One of 'greenplum', 'sqlserver', 'kafka_cdc'
    
    Returns:
        dict: Column mapping from source columns to target columns
    """
    source = get_source_config(source_name)
    return source.get("column_mapping", {})


def get_source_table_fqn(source_name: str) -> str:
    """
    Get the fully-qualified table name for a source.
    
    Args:
        source_name: One of 'greenplum', 'sqlserver', 'kafka_cdc'
    
    Returns:
        str: Fully-qualified table name (catalog.schema.table)
    """
    source = get_source_config(source_name)
    return f"{source['catalog']}.{source['schema']}.{source['table_name']}"


def get_scd2_keys() -> list:
    """Get the SCD2 key columns."""
    return get_target_config().get("keys", ["customer_id"])


def get_sequence_column() -> str:
    """Get the sequence column for SCD2 ordering."""
    return get_target_config().get("sequence_by", "event_timestamp")


def get_delete_condition() -> str:
    """Get the delete condition expression."""
    return get_target_config().get("delete_condition", "is_deleted = true")


def get_except_columns() -> list:
    """Get columns excluded from SCD2 change tracking."""
    return get_target_config().get("except_columns", [])


def get_target_table_name() -> str:
    """Get the target table name."""
    return get_target_config().get("table_name", "unified_customer_scd2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Functions

# COMMAND ----------

def validate_metadata() -> bool:
    """
    Validate that the metadata has all required fields.
    
    Returns:
        bool: True if valid, raises ValueError if invalid
    """
    errors = []
    
    # Check pipeline config
    pipeline = get_pipeline_config()
    if not pipeline.get("name"):
        errors.append("Missing pipeline.name")
    if not pipeline.get("catalog"):
        errors.append("Missing pipeline.catalog")
    
    # Check target config
    target = get_target_config()
    if not target.get("table_name"):
        errors.append("Missing target.table_name")
    if not target.get("keys"):
        errors.append("Missing target.keys")
    if not target.get("sequence_by"):
        errors.append("Missing target.sequence_by")
    
    # Check sources
    sources = get_all_sources()
    if not sources:
        errors.append("No sources defined")
    
    for source_name, source_config in sources.items():
        if not source_config.get("table_name"):
            errors.append(f"Missing {source_name}.table_name")
        if not source_config.get("column_mapping"):
            errors.append(f"Missing {source_name}.column_mapping")
    
    if errors:
        raise ValueError(f"Metadata validation failed:\n" + "\n".join(f"  - {e}" for e in errors))
    
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Metadata Summary

# COMMAND ----------

def print_metadata_summary():
    """Print a summary of the loaded metadata for debugging."""
    pipeline = get_pipeline_config()
    target = get_target_config()
    sources = get_all_sources()
    
    print("=" * 60)
    print(f"Pipeline: {pipeline.get('name')} v{pipeline.get('version')}")
    print("=" * 60)
    print(f"\nTarget Table: {target.get('table_name')}")
    print(f"  Keys: {target.get('keys')}")
    print(f"  Sequence By: {target.get('sequence_by')}")
    print(f"  Except Columns: {target.get('except_columns')}")
    
    print(f"\nSources ({len(sources)}):")
    for name, config in sources.items():
        fqn = f"{config.get('catalog')}.{config.get('schema')}.{config.get('table_name')}"
        print(f"  - {name}: {fqn}")
        print(f"      View: {config.get('view_name')}")
        print(f"      Flow: {config.get('flow_name')}")
        print(f"      Columns: {len(config.get('column_mapping', {}))}")
    print("=" * 60)
