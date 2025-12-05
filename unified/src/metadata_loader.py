import json
import os

try:
    from exceptions import (
        MetadataError,
        MetadataFileNotFoundError,
        MetadataParseError,
        MetadataValidationError,
        SourceNotFoundError,
        TargetNotFoundError,
        SourceMappingNotFoundError,
        SparkConfigError,
    )
except ImportError:
    from .exceptions import (
        MetadataError,
        MetadataFileNotFoundError,
        MetadataParseError,
        MetadataValidationError,
        SourceNotFoundError,
        TargetNotFoundError,
        SourceMappingNotFoundError,
        SparkConfigError,
    )

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
        dict: Configuration with catalog, schema, source_catalog, source_schema, metadata_path
    """
    # Default values for local testing (when Spark is not available)
    defaults = {
        "catalog": "ltc_insurance",
        "schema": "unified_dev",
        "source_catalog": "ltc_insurance",
        "source_schema": "raw_data_layer",
        "metadata_path": "stream/unified/customer_cdc/customer_cdc_pipeline.json",  # Default for backward compatibility
    }
    
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        return {
            "catalog": spark.conf.get("pipeline.catalog", defaults["catalog"]),
            "schema": spark.conf.get("pipeline.schema", defaults["schema"]),
            "source_catalog": spark.conf.get("pipeline.source_catalog", defaults["source_catalog"]),
            "source_schema": spark.conf.get("pipeline.source_schema", defaults["source_schema"]),
            "metadata_path": spark.conf.get("pipeline.metadata_path", defaults["metadata_path"]),
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
    
    The metadata path is passed via pipeline.metadata_path Spark config.
    This is a relative path from src/metadata folder.
    
    Tries multiple strategies to resolve the absolute path:
    1. Relative to __file__ (for direct execution)
    2. Workspace path using bundle.sourcePath (for DLT notebooks)
    3. Hardcoded fallback paths
    """
    # Get metadata_path from Spark config (relative path from metadata folder)
    spark_config = get_spark_config()
    metadata_path = spark_config.get("metadata_path", "stream/unified/customer_cdc/customer_cdc_pipeline.json")
    
    print(f"Looking for metadata at relative path: {metadata_path}")
    
    # Strategy 1: Try relative to __file__ (for direct execution / tests)
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        candidate = os.path.join(current_dir, "metadata", metadata_path)
        if os.path.exists(candidate):
            print(f"Found metadata at: {candidate}")
            return candidate
    except NameError:
        pass
    
    # Strategy 2: Try workspace path (for DLT notebooks)
    # The bundle deploys files to this location
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        bundle_path = spark.conf.get("bundle.sourcePath", "")
        if bundle_path:
            candidate = os.path.join(bundle_path, "metadata", metadata_path)
            if os.path.exists(candidate):
                print(f"Found metadata at: {candidate}")
                return candidate
    except Exception:
        pass
    
    # Strategy 3: Fallback paths for bundle deployment
    # Note: In dev mode, workspace.current_user.short_name is appended to the path
    fallback_paths = [
        f"/Workspace/Shared/.bundle/unified/dev/files/src/metadata/{metadata_path}",
        f"/Workspace/Shared/.bundle/unified/prod/files/src/metadata/{metadata_path}",
    ]
    for path in fallback_paths:
        if os.path.exists(path):
            print(f"Found metadata at fallback: {path}")
            return path
    
    # If all strategies fail, return the relative path and let load_metadata handle the error
    try:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "metadata", metadata_path)
    except NameError:
        return metadata_path


def load_metadata() -> dict:
    """
    Load the pipeline metadata from JSON file and inject runtime configuration.
    
    Catalog and schema values are read from Spark config (set by databricks.yml)
    and injected into the metadata. This allows the same JSON to work across
    dev/prod environments.
    
    Returns:
        dict: Complete pipeline metadata with catalog/schema injected
    
    Raises:
        MetadataFileNotFoundError: If metadata file cannot be found
        MetadataParseError: If metadata file is invalid JSON
    """
    metadata_path = _get_metadata_path()
    
    print(f"Loading metadata from: {metadata_path}")
    
    if not os.path.exists(metadata_path):
        # Collect all searched paths for error message
        searched_paths = _get_searched_paths()
        raise MetadataFileNotFoundError(
            file_path=metadata_path,
            searched_paths=searched_paths
        )
    
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
    except json.JSONDecodeError as e:
        raise MetadataParseError(
            file_path=metadata_path,
            parse_error=str(e)
        )
    except IOError as e:
        raise MetadataFileNotFoundError(
            file_path=metadata_path,
            searched_paths=[f"IO Error: {str(e)}"]
        )
    
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


def _get_searched_paths() -> list:
    """Get list of paths that were searched for metadata file."""
    spark_config = get_spark_config()
    metadata_path = spark_config.get("metadata_path", "stream/unified/customer_cdc/customer_cdc_pipeline.json")
    
    paths = []
    
    # Strategy 1: Relative to __file__
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        paths.append(os.path.join(current_dir, "metadata", metadata_path))
    except NameError:
        paths.append(f"(relative to __file__)/metadata/{metadata_path}")
    
    # Strategy 2: Workspace path
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        bundle_path = spark.conf.get("bundle.sourcePath", "")
        if bundle_path:
            paths.append(os.path.join(bundle_path, "metadata", metadata_path))
    except Exception:
        paths.append(f"(bundle.sourcePath)/metadata/{metadata_path}")
    
    # Strategy 3: Fallback paths
    paths.extend([
        f"/Workspace/Shared/.bundle/unified/dev/files/src/metadata/{metadata_path}",
        f"/Workspace/Shared/.bundle/unified/prod/files/src/metadata/{metadata_path}",
    ])
    
    return paths

# COMMAND ----------

# Load metadata on module import
METADATA = load_metadata()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessor Functions
# MAGIC 
# MAGIC Convenience functions to access specific parts of the metadata.
# MAGIC Supports both old `target` (singular) and new `targets[]` (array) structures.

# COMMAND ----------

def get_pipeline_config() -> dict:
    """Get pipeline-level configuration."""
    return METADATA.get("pipeline", {})


# ============================================================================
# Source-level accessors (shared definitions - same for old and new structure)
# ============================================================================

def get_all_sources() -> dict:
    """Get configuration for all sources (shared properties only)."""
    return METADATA.get("sources", {})


def get_source_config(source_name: str) -> dict:
    """
    Get shared configuration for a specific source.
    
    Args:
        source_name: Source identifier (e.g., 'greenplum', 'kafka_cdc', 'pega_event_stream')
    
    Returns:
        dict: Source configuration (table_name, description, source_system_value, catalog, schema)
    
    Raises:
        SourceNotFoundError: If source_name is not defined in metadata
    """
    sources = get_all_sources()
    if source_name not in sources:
        raise SourceNotFoundError(
            source_name=source_name,
            available_sources=list(sources.keys())
        )
    return sources[source_name]


def get_source_table_fqn(source_name: str) -> str:
    """
    Get the fully-qualified table name for a source.
    
    Args:
        source_name: Source identifier
    
    Returns:
        str: Fully-qualified table name (catalog.schema.table)
    """
    source = get_source_config(source_name)
    return f"{source['catalog']}.{source['schema']}.{source['table_name']}"


# ============================================================================
# Target-level accessors (NEW - supports multiple targets via targets[] array)
# ============================================================================

def _has_new_structure() -> bool:
    """Check if metadata uses new targets[] array structure."""
    return "targets" in METADATA and isinstance(METADATA["targets"], list)


def get_all_targets() -> list:
    """Get all target configurations (new structure)."""
    if _has_new_structure():
        return METADATA.get("targets", [])
    # Backward compatibility: wrap old target in a list
    old_target = METADATA.get("target", {})
    if old_target:
        return [old_target]
    return []


def get_target(index: int = 0) -> dict:
    """
    Get a specific target by index (default: first target).
    
    Args:
        index: Target index (default 0 for single-target pipelines)
    
    Returns:
        dict: Target configuration including schema, transforms, source_mappings
    
    Raises:
        TargetNotFoundError: If no targets are defined or index is out of range
    """
    targets = get_all_targets()
    if not targets:
        raise TargetNotFoundError(
            target_index=index,
            num_targets=0
        )
    if index >= len(targets):
        raise TargetNotFoundError(
            target_index=index,
            num_targets=len(targets)
        )
    return targets[index]


def get_target_config() -> dict:
    """
    Get target table configuration.
    
    DEPRECATED: Use get_target(index) for new targets[] structure.
    Kept for backward compatibility with old target (singular) structure.
    """
    # For backward compatibility, return old structure if exists
    if "target" in METADATA:
        return METADATA.get("target", {})
    # Otherwise return first target from new structure
    return get_target(0)


def get_target_name(index: int = 0) -> str:
    """Get the target table name."""
    target = get_target(index)
    # New structure uses 'name', old uses 'table_name'
    return target.get("name") or target.get("table_name", "")


def get_target_schema(index: int = 0) -> dict:
    """Get the target schema definition."""
    return get_target(index).get("schema", {})


def get_transforms(index: int = 0) -> dict:
    """Get the transform definitions for data type conversions."""
    return get_target(index).get("transforms", {})


def get_scd2_keys(index: int = 0) -> list:
    """Get the SCD2 key columns."""
    return get_target(index).get("keys", [])


def get_sequence_column(index: int = 0) -> str:
    """Get the sequence column for SCD2 ordering."""
    return get_target(index).get("sequence_by", "event_timestamp")


def get_delete_condition(index: int = 0) -> str:
    """Get the delete condition expression."""
    return get_target(index).get("delete_condition", "is_deleted = true")


def get_except_columns(index: int = 0) -> list:
    """Get columns excluded from SCD2 change tracking."""
    return get_target(index).get("except_columns", [])


def get_track_history_except_columns(index: int = 0) -> list:
    """
    Get columns excluded from SCD2 history tracking.
    
    These columns are included in the target table but changes to them
    do NOT create new history records (updated in place like SCD Type 1).
    """
    return get_target(index).get("track_history_except_columns", [])


# ============================================================================
# Source Mapping accessors (NEW - per-target source configurations)
# ============================================================================

def get_source_mappings(target_index: int = 0) -> dict:
    """
    Get all source mappings for a target.
    
    In new structure: targets[index].source_mappings
    In old structure: returns sources dict (column_mapping was in sources)
    """
    if _has_new_structure():
        return get_target(target_index).get("source_mappings", {})
    # Backward compatibility: old structure had mappings in sources
    return get_all_sources()


def get_source_mapping(source_name: str, target_index: int = 0) -> dict:
    """
    Get the source mapping for a specific source within a target.
    
    Args:
        source_name: Source identifier
        target_index: Target index (default 0)
    
    Returns:
        dict: Source mapping including view_name, flow_name, column_mapping
    
    Raises:
        SourceMappingNotFoundError: If source_name is not mapped in the target
    """
    mappings = get_source_mappings(target_index)
    if source_name not in mappings:
        raise SourceMappingNotFoundError(
            source_name=source_name,
            target_index=target_index,
            available_mappings=list(mappings.keys())
        )
    return mappings[source_name]


def get_enabled_source_mappings(target_index: int = 0) -> dict:
    """
    Get only enabled source mappings for a target.
    
    In new structure: filters targets[index].source_mappings by enabled flag
    In old structure: filters sources by enabled flag
    
    Returns:
        dict: Only source mappings where enabled=True (or not specified)
    """
    mappings = get_source_mappings(target_index)
    enabled = {
        key: config 
        for key, config in mappings.items() 
        if config.get("enabled", True)
    }
    print(f"Enabled source mappings for target[{target_index}]: {list(enabled.keys())}")
    return enabled


def get_enabled_sources() -> dict:
    """
    Get only enabled sources for processing.
    
    DEPRECATED: Use get_enabled_source_mappings(target_index) for new structure.
    Kept for backward compatibility.
    """
    return get_enabled_source_mappings(0)


def get_column_mapping(source_name: str, target_index: int = 0) -> dict:
    """
    Get the column mapping for a specific source.
    
    In new structure: targets[index].source_mappings[source].column_mapping
    In old structure: sources[source].column_mapping
    
    Args:
        source_name: Source identifier
        target_index: Target index (default 0)
    
    Returns:
        dict: Column mapping from source columns to target columns
    """
    mapping = get_source_mapping(source_name, target_index)
    return mapping.get("column_mapping", {})


def get_full_source_config(source_name: str, target_index: int = 0) -> dict:
    """
    Get complete source configuration by merging shared + target-specific.
    
    Merges:
    - Shared source properties (table_name, description, source_system_value, catalog, schema)
    - Target-specific mapping (view_name, flow_name, column_mapping, exclude_columns)
    
    This is the config needed by views.py and pipeline.py
    
    Args:
        source_name: Source identifier
        target_index: Target index (default 0)
    
    Returns:
        dict: Complete merged configuration for the source
    """
    # Get shared properties
    shared = get_source_config(source_name).copy()
    
    # Get target-specific mapping
    mapping = get_source_mapping(source_name, target_index)
    
    # Merge them (mapping properties override shared if duplicated)
    full_config = {
        **shared,
        **mapping,
    }
    
    return full_config


def get_target_table_name() -> str:
    """
    Get the target table name.
    
    DEPRECATED: Use get_target_name(index) for new structure.
    """
    return get_target_name(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Functions

# COMMAND ----------

def validate_metadata() -> bool:
    """
    Validate that the metadata has all required fields.
    
    Supports both old (target) and new (targets[]) structures.
    Note: catalog and schema are injected at runtime from Spark config.
    
    Returns:
        bool: True if valid
    
    Raises:
        MetadataValidationError: If validation fails with list of errors
    """
    errors = []
    
    # Check pipeline config
    pipeline = get_pipeline_config()
    if not pipeline.get("name"):
        errors.append("Missing pipeline.name")
    
    # Check targets (new structure) or target (old structure)
    targets = get_all_targets()
    if not targets:
        errors.append("No targets defined (missing 'targets' array or 'target' object)")
    
    for idx, target in enumerate(targets):
        prefix = f"targets[{idx}]" if _has_new_structure() else "target"
        
        # Target name
        target_name = target.get("name") or target.get("table_name")
        if not target_name:
            errors.append(f"Missing {prefix}.name or {prefix}.table_name")
        
        if not target.get("keys"):
            errors.append(f"Missing {prefix}.keys")
        if not target.get("sequence_by"):
            errors.append(f"Missing {prefix}.sequence_by")
        
        # Check source mappings (new structure) or column_mapping in sources (old)
        if _has_new_structure():
            source_mappings = target.get("source_mappings", {})
            if not source_mappings:
                errors.append(f"Missing {prefix}.source_mappings")
            for src_name, mapping in source_mappings.items():
                if not mapping.get("column_mapping"):
                    errors.append(f"Missing {prefix}.source_mappings.{src_name}.column_mapping")
    
    # Check sources (shared definitions)
    sources = get_all_sources()
    if not sources:
        errors.append("No sources defined")
    
    for source_name, source_config in sources.items():
        if not source_config.get("table_name"):
            errors.append(f"Missing sources.{source_name}.table_name")
        # Old structure: column_mapping in sources
        if not _has_new_structure() and not source_config.get("column_mapping"):
            errors.append(f"Missing sources.{source_name}.column_mapping")
    
    if errors:
        raise MetadataValidationError(errors=errors)
    
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Metadata Summary

# COMMAND ----------

def print_metadata_summary():
    """Print a summary of the loaded metadata for debugging."""
    pipeline = get_pipeline_config()
    targets = get_all_targets()
    sources = get_all_sources()
    
    print("=" * 60)
    print(f"Pipeline: {pipeline.get('name')} v{pipeline.get('version')}")
    print(f"Structure: {'NEW (targets[])' if _has_new_structure() else 'OLD (target)'}")
    print("=" * 60)
    
    print(f"\nSources (shared definitions): {len(sources)}")
    for name, config in sources.items():
        fqn = f"{config.get('catalog')}.{config.get('schema')}.{config.get('table_name')}"
        print(f"  - {name}: {fqn}")
    
    print(f"\nTargets: {len(targets)}")
    for idx, target in enumerate(targets):
        target_name = target.get("name") or target.get("table_name")
        print(f"\n  Target[{idx}]: {target_name}")
        print(f"    Keys: {target.get('keys')}")
        print(f"    Sequence By: {target.get('sequence_by')}")
        print(f"    Except Columns: {target.get('except_columns', [])}")
        
        # Source mappings
        if _has_new_structure():
            mappings = target.get("source_mappings", {})
            print(f"    Source Mappings: {len(mappings)}")
            for src_name, mapping in mappings.items():
                enabled = "✓" if mapping.get("enabled", True) else "✗"
                print(f"      [{enabled}] {src_name}:")
                print(f"          View: {mapping.get('view_name')}")
                print(f"          Flow: {mapping.get('flow_name')}")
                print(f"          Columns: {len(mapping.get('column_mapping', {}))}")
        else:
            # Old structure: show sources with their mappings
            for src_name, config in sources.items():
                enabled = "✓" if config.get("enabled", True) else "✗"
                print(f"      [{enabled}] {src_name}:")
                print(f"          View: {config.get('view_name')}")
                print(f"          Flow: {config.get('flow_name')}")
                print(f"          Columns: {len(config.get('column_mapping', {}))}")
    
    print("\n" + "=" * 60)
