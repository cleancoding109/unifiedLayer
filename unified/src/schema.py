# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Mapping Module
# MAGIC 
# MAGIC Loads schema definitions and column mappings from metadata.
# MAGIC Provides the target schema and per-source column mappings.

# COMMAND ----------

# MAGIC %run ./metadata_loader

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target Schema
# MAGIC 
# MAGIC The unified "golden" schema that all sources conform to.

# COMMAND ----------

# Load target schema from metadata
TARGET_SCHEMA = get_target_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Column Mappings
# MAGIC 
# MAGIC Each source has different column names and data types.
# MAGIC These mappings define how to transform source columns to target columns.

# COMMAND ----------

# Load column mappings from metadata
GP_COLUMN_MAPPING = get_column_mapping("greenplum")
SQL_COLUMN_MAPPING = get_column_mapping("sqlserver")
CDC_COLUMN_MAPPING = get_column_mapping("kafka_cdc")

# Columns to exclude from Greenplum (legacy SCD2 columns)
GP_EXCLUDE_COLUMNS = get_source_config("greenplum").get("exclude_columns", [])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Documentation
# MAGIC 
# MAGIC Print schema information for debugging.

# COMMAND ----------

def print_schema_summary():
    """Print a summary of the schema configuration."""
    print("=" * 60)
    print("TARGET SCHEMA")
    print("=" * 60)
    for col_name, col_def in TARGET_SCHEMA.items():
        nullable = "NULL" if col_def.get("nullable", True) else "NOT NULL"
        print(f"  {col_name}: {col_def['dtype']} {nullable}")
    
    print("\n" + "=" * 60)
    print("COLUMN MAPPINGS")
    print("=" * 60)
    
    for source_name, mapping in [("Greenplum", GP_COLUMN_MAPPING), 
                                   ("SQL Server", SQL_COLUMN_MAPPING), 
                                   ("Kafka CDC", CDC_COLUMN_MAPPING)]:
        print(f"\n{source_name}:")
        for target_col, src_def in mapping.items():
            src_col = src_def.get("source_col") or f"DEFAULT({src_def.get('default', 'NULL')})"
            transform = src_def.get("transform") or "-"
            print(f"  {src_col:25} -> {target_col:20} [{transform}]")

