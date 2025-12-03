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

def _create_source_view(source_key: str, source_config: dict):
    """
    Factory function to create and register a Lakeflow view.
    Using a factory ensures proper closure capture for the source_config.
    """
    view_name = source_config["view_name"]
    description = source_config.get("description", f"Source view for {source_key}")
    
    # Construct fully qualified table name
    table_fqn = f"{source_config['catalog']}.{source_config['schema']}.{source_config['table_name']}"
    
    @dp.view(name=view_name, comment=description)
    def _dynamic_view_impl():
        # Get Spark session
        spark = SparkSession.builder.getOrCreate()
        
        # Read from the source table
        df = spark.readStream.table(table_fqn)
        # Apply schema mapping using the specific source configuration
        return transformations.apply_schema_mapping(df, metadata_loader.get_column_mapping(source_key), source_key)
    
    return _dynamic_view_impl



# COMMAND ----------

# Iterate through only ENABLED sources and create views
# This allows sequential merging of sources one at a time
for key, source_data in metadata_loader.get_enabled_sources().items():
    _create_source_view(key, source_data)


