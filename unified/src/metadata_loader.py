import json
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration from Spark Config
# MAGIC 
# MAGIC These values are injected by databricks.yml via the pipeline configuration.
# MAGIC They allow the same metadata to work across dev/prod environments.

# COMMAND ----------

def _get_spark_config() -> dict:
    """
    Get pipeline configuration from Spark config.
    
    These values are set in databricks.yml -> resources/pipeline/unified.pipeline.yml
    via the 'configuration' section and injected as Spark config at runtime.
    
    Returns:
        dict: Configuration with catalog, schema, source_catalog, source_schema
    """
    # Default values for local testing (when Spark is not available)
    defaults = {
        "catalog": "ltc_insurance",
        "schema": "unified_dev",
        "source_catalog": "ltc_insurance",
        "source_schema": "raw_data_layer",
    }
    
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        return {
            "catalog": spark.conf.get("pipeline.catalog", defaults["catalog"]),
            "schema": spark.conf.get("pipeline.schema", defaults["schema"]),
            "source_catalog": spark.conf.get("pipeline.source_catalog", defaults["source_catalog"]),
            "source_schema": spark.conf.get("pipeline.source_schema", defaults["source_schema"]),
        }
    except Exception:
        # Return defaults if Spark is not available (e.g., during local testing)
        return defaults


# Cached config to avoid repeated Spark lookups
_SPARK_CONFIG = None

def get_spark_config() -> dict:
    """Get cached Spark config."""
    global _SPARK_CONFIG
    if _SPARK_CONFIG is None:
        _SPARK_CONFIG = _get_spark_config()
    return _SPARK_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Metadata JSON

# COMMAND ----------

def _get_metadata_path() -> str:
    """
    Get the absolute path to the metadata JSON file.
    
    Tries multiple strategies to locate the file:
    1. importlib.resources (for installed packages)
    2. Relative to __file__ (for direct execution)
    3. Workspace path fallback (for DLT notebooks)
    
    Metadata path structure: metadata/stream/unified/customer/pipeline_metadata.json
    """
    # New nested path relative to metadata folder
    nested_path = os.path.join("stream", "unified", "customer", "pipeline_metadata.json")
    
    # Strategy 1: Try importlib.resources (Python 3.9+)
    try:
        import importlib.resources as pkg_resources
        # For Python 3.9+, use files()
        try:
            from importlib.resources import files
            pkg_path = files('src.metadata.stream.unified.customer').joinpath('pipeline_metadata.json')
            if hasattr(pkg_path, '_path'):
                return str(pkg_path._path)
            # For traversable objects
            return str(pkg_path)
        except (ImportError, TypeError, ModuleNotFoundError):
            pass
    except ImportError:
        pass
    
    # Strategy 2: Try relative to __file__
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        candidate = os.path.join(current_dir, "metadata", nested_path)
        if os.path.exists(candidate):
            return candidate
    except NameError:
        pass
    
    # Strategy 3: Try workspace path (for DLT notebooks)
    # The bundle deploys files to this location
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        bundle_path = spark.conf.get("bundle.sourcePath", "")
        if bundle_path:
            candidate = os.path.join(bundle_path, "metadata", nested_path)
            if os.path.exists(candidate):
                return candidate
    except Exception:
        pass
    
    # Strategy 4: Hardcoded fallback for the bundle deployment path
    fallback_paths = [
        f"/Workspace/Users/cleancoding109@gmail.com/.bundle/unified/dev/files/src/metadata/{nested_path}",
        f"/Workspace/Repos/unifiedLayer/unified/src/metadata/{nested_path}",
    ]
    for path in fallback_paths:
        if os.path.exists(path):
            return path
    
    # If all strategies fail, return the relative path and let load_metadata handle the error
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "metadata", nested_path)


def load_metadata() -> dict:
    """
    Load the pipeline metadata from JSON file and inject runtime configuration.
    
    Catalog and schema values are read from Spark config (set by databricks.yml)
    and injected into the metadata. This allows the same JSON to work across
    dev/prod environments.
    
    Returns:
        dict: Complete pipeline metadata with catalog/schema injected
    
    Raises:
        FileNotFoundError: If metadata file cannot be found
        json.JSONDecodeError: If metadata file is invalid JSON
    """
    metadata_path = _get_metadata_path()
    
    print(f"Loading metadata from: {metadata_path}")
    
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Critical Error: Metadata file not found at {metadata_path}")
        
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    # Inject runtime configuration from Spark config (set by databricks.yml)
    spark_config = get_spark_config()
    
    # Inject into pipeline config
    if "pipeline" in metadata:
        metadata["pipeline"]["catalog"] = spark_config["catalog"]
        metadata["pipeline"]["target_schema"] = spark_config["schema"]
    
    # Inject into each source config
    if "sources" in metadata:
        for source_name, source_config in metadata["sources"].items():
            source_config["catalog"] = spark_config["source_catalog"]
            source_config["schema"] = spark_config["source_schema"]
    
    print(f"Injected config - catalog: {spark_config['catalog']}, schema: {spark_config['schema']}")
    print(f"Injected config - source_catalog: {spark_config['source_catalog']}, source_schema: {spark_config['source_schema']}")
    
    return metadata

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


def get_enabled_sources() -> dict:
    """
    Get only enabled sources for processing.
    
    Sources can be enabled/disabled in the metadata JSON using the 'enabled' flag.
    This allows sequential merging of sources one at a time.
    
    Returns:
        dict: Only sources where enabled=True (or enabled not specified, defaults to True)
    """
    all_sources = get_all_sources()
    enabled = {
        key: config 
        for key, config in all_sources.items() 
        if config.get("enabled", True)
    }
    print(f"Enabled sources: {list(enabled.keys())} (out of {list(all_sources.keys())})")
    return enabled


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


def get_track_history_except_columns() -> list:
    """
    Get columns excluded from SCD2 history tracking.
    
    These columns are included in the target table but changes to them
    do NOT create new history records (updated in place like SCD Type 1).
    """
    return get_target_config().get("track_history_except_columns", [])


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
    
    Note: catalog and schema are injected at runtime from Spark config,
    so they are not validated here.
    
    Returns:
        bool: True if valid, raises ValueError if invalid
    """
    errors = []
    
    # Check pipeline config
    pipeline = get_pipeline_config()
    if not pipeline.get("name"):
        errors.append("Missing pipeline.name")
    # Note: catalog is injected at runtime, not required in JSON
    
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
        # Note: catalog and schema are injected at runtime
    
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
