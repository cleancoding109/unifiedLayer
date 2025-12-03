from pyspark import pipelines as dp
from pyspark.sql import SparkSession

try:
    import metadata_loader
    import transformations
except ImportError:
    from . import metadata_loader
    from . import transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic View Generation
# MAGIC 
# MAGIC Instead of hardcoding views for each source, we iterate through the
# MAGIC configured sources in the metadata and generate views dynamically.
# MAGIC This allows the pipeline to scale to any number of sources without code changes.

# COMMAND ----------

def _create_source_view(source_key: str, source_mapping: dict, target_index: int = 0):
    """
    Factory function to create and register a Lakeflow view.
    Using a factory ensures proper closure capture for the source configuration.
    
    Args:
        source_key: Source identifier (e.g., 'greenplum', 'kafka_cdc')
        source_mapping: Target-specific source mapping (view_name, flow_name, column_mapping)
        target_index: Target index for multi-target pipelines (default 0)
    """
    # Get full source config (merges shared + target-specific)
    full_config = metadata_loader.get_full_source_config(source_key, target_index)
    
    view_name = source_mapping["view_name"]
    description = full_config.get("description", f"Source view for {source_key}")
    
    # Construct fully qualified table name from shared source config
    table_fqn = metadata_loader.get_source_table_fqn(source_key)
    
    @dp.view(name=view_name, comment=description)
    def _dynamic_view_impl():
        # Get Spark session
        spark = SparkSession.builder.getOrCreate()
        
        # Read from the source table
        df = spark.readStream.table(table_fqn)
        # Apply schema mapping using the target-specific column mapping
        return transformations.apply_schema_mapping(
            df, 
            metadata_loader.get_column_mapping(source_key, target_index), 
            source_key,
            target_index
        )
    
    return _dynamic_view_impl



# COMMAND ----------

# Iterate through only ENABLED source mappings and create views
# This allows sequential merging of sources one at a time
for key, source_mapping in metadata_loader.get_enabled_source_mappings(0).items():
    _create_source_view(key, source_mapping, target_index=0)


