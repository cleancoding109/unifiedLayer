from pyspark import pipelines as dp
from pyspark.sql import SparkSession

try:
    import metadata_loader
    import mapper
    import transformations
    import dedup
    from exceptions import (
        ViewCreationError,
        SourceTableNotFoundError,
        MappingError,
        TransformError,
        DedupError,
    )
except ImportError:
    from . import metadata_loader
    from . import mapper
    from . import transformations
    from . import dedup
    from .exceptions import (
        ViewCreationError,
        SourceTableNotFoundError,
        MappingError,
        TransformError,
        DedupError,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic View Generation
# MAGIC 
# MAGIC Instead of hardcoding views for each source, we iterate through the
# MAGIC configured sources in the metadata and generate views dynamically.
# MAGIC This allows the pipeline to scale to any number of sources without code changes.
# MAGIC
# MAGIC ## Pipeline Flow:
# MAGIC ```
# MAGIC Source Table → apply_mapping() → apply_transforms() → apply_dedup() → View Output
# MAGIC                 (rename cols)     (type conversions)   (watermark+dedup)
# MAGIC ```

# COMMAND ----------

def _create_source_view(source_key: str, source_mapping: dict, target_index: int = 0):
    """
    Factory function to create and register a Lakeflow view.
    Using a factory ensures proper closure capture for the source configuration.
    
    Args:
        source_key: Source identifier (e.g., 'greenplum', 'kafka_cdc')
        source_mapping: Target-specific source mapping (view_name, flow_name, column_mapping)
        target_index: Target index for multi-target pipelines (default 0)
    
    Raises:
        ViewCreationError: If view creation fails
        SourceTableNotFoundError: If source table cannot be read
        MappingError: If column mapping fails
        TransformError: If transformation fails
        DedupError: If deduplication fails
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
        
        # Read from the source table with error handling
        try:
            df = spark.readStream.table(table_fqn)
        except Exception as e:
            raise SourceTableNotFoundError(
                table_fqn=table_fqn,
                source_name=source_key
            ) from e
        
        # Get column mapping for this source/target combination
        column_mapping = metadata_loader.get_column_mapping(source_key, target_index)
        
        # Step 1: Apply mapping (rename columns, set defaults)
        try:
            df_mapped = mapper.apply_mapping(
                df, 
                column_mapping, 
                source_key,
                target_index
            )
        except MappingError:
            raise  # Re-raise specific mapping errors
        except Exception as e:
            raise ViewCreationError(
                view_name=view_name,
                source_name=source_key,
                error=f"Mapping failed: {str(e)}"
            ) from e
        
        # Step 2: Apply transforms (type conversions, epoch_to_timestamp, etc.)
        try:
            df_transformed = transformations.apply_transforms(
                df_mapped,
                column_mapping,
                target_index
            )
        except TransformError:
            raise  # Re-raise specific transform errors
        except Exception as e:
            raise ViewCreationError(
                view_name=view_name,
                source_name=source_key,
                error=f"Transformation failed: {str(e)}"
            ) from e
        
        # Step 3: Apply deduplication (watermark + dropDuplicates)
        # Only applied if dedup_config.enabled = true in source_mapping
        # This handles:
        #   - Out-of-order Kafka events (via watermark)
        #   - Duplicate messages from producer retries (via offset dedup)
        #   - Logical duplicates (via business key dedup)
        dedup_config = source_mapping.get("dedup_config", {})
        try:
            df_deduped = dedup.apply_dedup(df_transformed, dedup_config)
        except DedupError:
            raise  # Re-raise specific dedup errors
        except Exception as e:
            raise ViewCreationError(
                view_name=view_name,
                source_name=source_key,
                error=f"Deduplication failed: {str(e)}"
            ) from e
        
        return df_deduped
    
    return _dynamic_view_impl



# COMMAND ----------

# Multi-Target View Generation:
# Loop through all targets and create views for each target's enabled source mappings.
# This supports the "many-to-many" pattern where one pipeline can have multiple targets.
# 
# Note: View names are target-specific (defined in source_mappings), so even if the
# same source (e.g., pega_workflow) is used by multiple targets, each target gets
# its own view with the appropriate column mapping for that target.

all_targets = metadata_loader.get_all_targets()

for target_idx, target in enumerate(all_targets):
    # Skip disabled targets (if enabled flag exists and is False)
    if not target.get("enabled", True):
        continue
    
    # Create views for each enabled source mapping of this target
    for key, source_mapping in metadata_loader.get_enabled_source_mappings(target_idx).items():
        _create_source_view(key, source_mapping, target_index=target_idx)


