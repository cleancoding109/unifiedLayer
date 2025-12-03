# Databricks notebook source
# MAGIC %md
# MAGIC # Test Pipeline - New Metadata Structure
# MAGIC 
# MAGIC This notebook tests the new `targets[]` metadata structure step by step.
# MAGIC 
# MAGIC ## New Structure:
# MAGIC ```
# MAGIC {
# MAGIC   "pipeline": {...},
# MAGIC   "sources": {                         <- Shared source definitions
# MAGIC     "source_a": {"table_name": ...},
# MAGIC     "source_b": {"table_name": ...}
# MAGIC   },
# MAGIC   "targets": [                         <- Array of targets
# MAGIC     {
# MAGIC       "name": "unified_test_scd2",
# MAGIC       "source_mappings": {             <- Per-target source configurations
# MAGIC         "source_a": {"view_name": ..., "column_mapping": {...}},
# MAGIC         "source_b": {"view_name": ..., "column_mapping": {...}}
# MAGIC       },
# MAGIC       "schema": {...},
# MAGIC       "transforms": {...}
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Metadata Using New Structure

# COMMAND ----------

import json
import os

# Get metadata path from Spark config
def get_spark_config():
    defaults = {
        "catalog": "ltc_insurance",
        "schema": "unified_dev",
        "source_catalog": "ltc_insurance", 
        "source_schema": "raw_data_layer",
        "metadata_path": "test/test_pipeline.json",
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
        return defaults

spark_config = get_spark_config()
print(f"Spark Config: {spark_config}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Find and Load Metadata File

# COMMAND ----------

def find_metadata_file(relative_path):
    """Find metadata file using multiple strategies."""
    
    # Strategy 1: Bundle workspace path
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        bundle_path = spark.conf.get("bundle.sourcePath", "")
        if bundle_path:
            candidate = os.path.join(bundle_path, "metadata", relative_path)
            if os.path.exists(candidate):
                return candidate
    except Exception:
        pass
    
    # Strategy 2: Fallback paths for bundle deployment
    # Note: In dev mode, workspace.current_user.short_name is appended to the path
    fallback_paths = [
        f"/Workspace/Shared/.bundle/unified/dev/files/src/metadata/{relative_path}",
        f"/Workspace/Shared/.bundle/unified/prod/files/src/metadata/{relative_path}",
    ]
    for path in fallback_paths:
        if os.path.exists(path):
            return path
    
    raise FileNotFoundError(f"Could not find metadata file: {relative_path}")

metadata_path = find_metadata_file(spark_config["metadata_path"])
print(f"Found metadata at: {metadata_path}")

with open(metadata_path, 'r') as f:
    METADATA = json.load(f)

print(f"\nLoaded metadata keys: {list(METADATA.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: New Accessor Functions for targets[] Structure

# COMMAND ----------

# ============================================================================
# Pipeline-level accessors
# ============================================================================

def get_pipeline_config():
    """Get pipeline-level configuration."""
    return METADATA.get("pipeline", {})

# ============================================================================
# Source-level accessors (shared definitions)
# ============================================================================

def get_all_sources():
    """Get all source definitions (shared properties only)."""
    return METADATA.get("sources", {})

def get_source(source_name):
    """Get shared configuration for a specific source."""
    sources = get_all_sources()
    if source_name not in sources:
        raise ValueError(f"Unknown source: {source_name}. Valid: {list(sources.keys())}")
    return sources[source_name]

def get_source_table_fqn(source_name):
    """Get fully-qualified table name for a source."""
    source = get_source(source_name)
    return f"{spark_config['source_catalog']}.{spark_config['source_schema']}.{source['table_name']}"

# ============================================================================
# Target-level accessors (NEW - supports multiple targets)
# ============================================================================

def get_all_targets():
    """Get all target configurations."""
    return METADATA.get("targets", [])

def get_target(index=0):
    """Get a specific target by index (default: first target)."""
    targets = get_all_targets()
    if not targets:
        raise ValueError("No targets defined in metadata")
    if index >= len(targets):
        raise ValueError(f"Target index {index} out of range. Available: {len(targets)}")
    return targets[index]

def get_target_name(index=0):
    """Get the target table name."""
    return get_target(index).get("name")

def get_target_schema(index=0):
    """Get the target schema definition."""
    return get_target(index).get("schema", {})

def get_target_transforms(index=0):
    """Get the target transform definitions."""
    return get_target(index).get("transforms", {})

def get_scd2_keys(index=0):
    """Get SCD2 key columns for a target."""
    return get_target(index).get("keys", [])

def get_sequence_column(index=0):
    """Get sequence column for SCD2 ordering."""
    return get_target(index).get("sequence_by", "event_timestamp")

def get_delete_condition(index=0):
    """Get delete condition expression."""
    return get_target(index).get("delete_condition", "is_deleted = true")

def get_except_columns(index=0):
    """Get columns excluded from SCD2 change tracking."""
    return get_target(index).get("except_columns", [])

def get_track_history_except_columns(index=0):
    """Get columns excluded from SCD2 history tracking."""
    return get_target(index).get("track_history_except_columns", [])

# ============================================================================
# Source Mapping accessors (NEW - per-target source configurations)
# ============================================================================

def get_source_mappings(index=0):
    """Get all source mappings for a target."""
    return get_target(index).get("source_mappings", {})

def get_source_mapping(source_name, target_index=0):
    """Get the source mapping for a specific source within a target."""
    mappings = get_source_mappings(target_index)
    if source_name not in mappings:
        raise ValueError(f"No mapping for source '{source_name}' in target {target_index}")
    return mappings[source_name]

def get_enabled_source_mappings(target_index=0):
    """Get only enabled source mappings for a target."""
    mappings = get_source_mappings(target_index)
    enabled = {
        key: config 
        for key, config in mappings.items() 
        if config.get("enabled", True)
    }
    print(f"Enabled sources for target[{target_index}]: {list(enabled.keys())}")
    return enabled

def get_column_mapping(source_name, target_index=0):
    """Get column mapping for a source within a target."""
    mapping = get_source_mapping(source_name, target_index)
    return mapping.get("column_mapping", {})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test the Accessor Functions

# COMMAND ----------

print("=" * 60)
print("TESTING NEW ACCESSOR FUNCTIONS")
print("=" * 60)

# Pipeline config
pipeline = get_pipeline_config()
print(f"\n1. Pipeline: {pipeline.get('name')} v{pipeline.get('version')}")

# Sources (shared)
print(f"\n2. Sources (shared definitions):")
for name, cfg in get_all_sources().items():
    fqn = get_source_table_fqn(name)
    print(f"   - {name}: {fqn}")

# Targets
print(f"\n3. Targets: {len(get_all_targets())}")
for i, target in enumerate(get_all_targets()):
    print(f"   Target[{i}]: {target.get('name')}")
    print(f"      Keys: {target.get('keys')}")
    print(f"      Source mappings: {list(target.get('source_mappings', {}).keys())}")

# Source mappings for target[0]
print(f"\n4. Enabled Source Mappings for target[0]:")
for source_name, mapping in get_enabled_source_mappings(0).items():
    print(f"   - {source_name}:")
    print(f"      view_name: {mapping.get('view_name')}")
    print(f"      flow_name: {mapping.get('flow_name')}")
    print(f"      column_mapping keys: {list(mapping.get('column_mapping', {}).keys())}")

# Column mapping example
print(f"\n5. Column mapping for 'test_source_a' -> target[0]:")
col_map = get_column_mapping("test_source_a", 0)
for target_col, mapping in col_map.items():
    print(f"   {target_col} <- {mapping.get('source_col')} (transform: {mapping.get('transform')})")

print("\n" + "=" * 60)
print("ALL ACCESSOR TESTS PASSED!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Combined Source Config (Merging Shared + Mapping)

# COMMAND ----------

def get_full_source_config(source_name, target_index=0):
    """
    Get complete source configuration by merging:
    - Shared source properties (table_name, description, source_system_value)
    - Target-specific mapping (view_name, flow_name, column_mapping)
    - Runtime config (catalog, schema)
    
    This is the config needed by views.py and pipeline.py
    """
    # Get shared properties
    shared = get_source(source_name).copy()
    
    # Get target-specific mapping
    mapping = get_source_mapping(source_name, target_index)
    
    # Merge them
    full_config = {
        **shared,
        **mapping,
        "catalog": spark_config["source_catalog"],
        "schema": spark_config["source_schema"],
    }
    
    return full_config

# Test it
print("Full source config for 'test_source_a':")
full_cfg = get_full_source_config("test_source_a", 0)
for key, value in full_cfg.items():
    if key != "column_mapping":
        print(f"  {key}: {value}")
print(f"  column_mapping: ({len(full_cfg.get('column_mapping', {}))} columns)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Build the Pipeline Flow (Dry Run)
# MAGIC 
# MAGIC This shows how the pipeline.py would iterate over the new structure.

# COMMAND ----------

print("=" * 60)
print("PIPELINE FLOW SIMULATION")
print("=" * 60)

# For each target (we only have 1 in this test)
for target_idx, target in enumerate(get_all_targets()):
    if not target.get("enabled", True):
        print(f"\nSkipping disabled target: {target.get('name')}")
        continue
        
    print(f"\n>>> Processing Target[{target_idx}]: {target.get('name')}")
    print(f"    Keys: {get_scd2_keys(target_idx)}")
    print(f"    Sequence by: {get_sequence_column(target_idx)}")
    
    # Create streaming table for this target
    print(f"\n    [DRY RUN] dp.create_streaming_table(name='{get_target_name(target_idx)}')")
    
    # For each enabled source mapping
    for source_name, mapping in get_enabled_source_mappings(target_idx).items():
        # Get full config (shared + mapping)
        full_cfg = get_full_source_config(source_name, target_idx)
        
        print(f"\n    >>> Source: {source_name}")
        print(f"        Table: {get_source_table_fqn(source_name)}")
        print(f"        View: {mapping.get('view_name')}")
        print(f"        Flow: {mapping.get('flow_name')}")
        
        # This would create the view
        print(f"        [DRY RUN] Create view '{mapping.get('view_name')}' with column mapping")
        
        # This would create the CDC flow
        print(f"        [DRY RUN] dp.create_auto_cdc_flow(")
        print(f"            name='{mapping.get('flow_name')}',")
        print(f"            target='{get_target_name(target_idx)}',")
        print(f"            source='{mapping.get('view_name')}',")
        print(f"            keys={get_scd2_keys(target_idx)},")
        print(f"            sequence_by='{get_sequence_column(target_idx)}',")
        print(f"            stored_as_scd_type='2'")
        print(f"        )")

print("\n" + "=" * 60)
print("PIPELINE FLOW SIMULATION COMPLETE!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC The new structure works! Key changes:
# MAGIC 
# MAGIC 1. **`get_enabled_sources()`** → **`get_enabled_source_mappings(target_index)`**
# MAGIC    - Now gets mappings from `targets[index].source_mappings`
# MAGIC 
# MAGIC 2. **Column mapping location changed**:
# MAGIC    - OLD: `sources.greenplum.column_mapping`
# MAGIC    - NEW: `targets[0].source_mappings.greenplum.column_mapping`
# MAGIC 
# MAGIC 3. **New helper: `get_full_source_config(source_name, target_index)`**
# MAGIC    - Merges shared source properties with target-specific mappings
# MAGIC    - Used by views.py to build the transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create a DLT Table to Validate Pipeline Runs
# MAGIC 
# MAGIC This creates a simple test table that proves the metadata was loaded correctly.

# COMMAND ----------

import dlt

@dlt.table(
    name="metadata_test_results",
    comment="Test table to validate metadata structure loading"
)
def metadata_test_results():
    """Create a test table with metadata summary."""
    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
    
    # Build test results from metadata
    pipeline_cfg = get_pipeline_config()
    target = get_target(0)
    sources = get_all_sources()
    mappings = get_enabled_source_mappings(0)
    
    rows = [
        Row(
            test_name="pipeline_name",
            test_value=pipeline_cfg.get("name", ""),
            passed=bool(pipeline_cfg.get("name"))
        ),
        Row(
            test_name="target_name",
            test_value=get_target_name(0),
            passed=bool(get_target_name(0))
        ),
        Row(
            test_name="sources_count",
            test_value=str(len(sources)),
            passed=len(sources) > 0
        ),
        Row(
            test_name="enabled_sources_count",
            test_value=str(len(mappings)),
            passed=len(mappings) > 0
        ),
        Row(
            test_name="scd2_keys",
            test_value=",".join(get_scd2_keys(0)),
            passed=len(get_scd2_keys(0)) > 0
        ),
        Row(
            test_name="schema_columns_count",
            test_value=str(len(get_target_schema(0))),
            passed=len(get_target_schema(0)) > 0
        ),
    ]
    
    # Add test for each source mapping
    for source_name, mapping in mappings.items():
        col_mapping = mapping.get("column_mapping", {})
        rows.append(Row(
            test_name=f"source_{source_name}_columns",
            test_value=str(len(col_mapping)),
            passed=len(col_mapping) > 0
        ))
    
    return spark.createDataFrame(rows)
