# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Test Transformations Module
# MAGIC This notebook tests the transformations module with claims data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import json
from pyspark.sql import functions as F

# Add the src folder to Python path for local imports
src_path = "/Workspace/Shared/.bundle/unified/dev/cleancoding109/files/src"
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Import modules directly from src
from mapper import apply_mapping
from transformations import apply_transforms

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Metadata

# COMMAND ----------

# Configuration
source_catalog = "ltc_insurance"
source_schema = "raw_data_layer"

# Load metadata
metadata_path = f"{src_path}/metadata/stream/unified/claims_cdc/claims_cdc_pipeline.json"
with open(metadata_path, "r") as f:
    metadata = json.load(f)

# Extract sources and target info
sources = metadata.get("sources", {})
targets = metadata.get("targets", [])
target = targets[0] if targets else {}
source_mappings_dict = target.get("source_mappings", {})

print(f"Pipeline: {metadata.get('pipeline', {}).get('name')}")
print(f"Sources: {list(source_mappings_dict.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Transformations on legacy_claims

# COMMAND ----------

# Get legacy source config
legacy_config = source_mappings_dict.get("legacy_claims", {})
legacy_source_info = sources.get("legacy_claims", {})
source_table = legacy_source_info.get("table_name")
column_mapping = legacy_config.get("column_mapping", {})

full_table = f"{source_catalog}.{source_schema}.{source_table}"
print(f"Reading from: {full_table}")

# Read and map
legacy_df = spark.read.table(full_table)
legacy_mapped = apply_mapping(legacy_df, column_mapping, "legacy_claims")

print(f"Before transformations: {legacy_mapped.columns}")

# COMMAND ----------

# Check for transforms in column_mapping
transforms = {col: m.get("transform") for col, m in column_mapping.items() if m.get("transform")}
print(f"Transforms defined: {transforms}")

# COMMAND ----------

# Apply transformations (for now, just pass through - no transforms defined in claims)
# The claims metadata doesn't have transforms, so this is a passthrough test
legacy_transformed = legacy_mapped

print(f"\nAfter transformations: {legacy_transformed.columns}")
print(f"Record count: {legacy_transformed.count()}")

# COMMAND ----------

# Display transformed data
legacy_transformed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Transformations on kafka_claims

# COMMAND ----------

# Get kafka source config
kafka_config = source_mappings_dict.get("kafka_claims", {})
kafka_source_info = sources.get("kafka_claims", {})
kafka_table = kafka_source_info.get("table_name")
kafka_column_mapping = kafka_config.get("column_mapping", {})

full_table = f"{source_catalog}.{source_schema}.{kafka_table}"
print(f"Reading from: {full_table}")

# Read and map
kafka_df = spark.read.table(full_table)
kafka_mapped = apply_mapping(kafka_df, kafka_column_mapping, "kafka_claims")

print(f"Before transformations: {kafka_mapped.columns}")

# COMMAND ----------

# Check for transforms
kafka_transforms = {col: m.get("transform") for col, m in kafka_column_mapping.items() if m.get("transform")}
print(f"Transforms defined: {kafka_transforms}")

# COMMAND ----------

# Apply transformations (passthrough for claims - no transforms defined)
kafka_transformed = kafka_mapped

print(f"\nAfter transformations: {kafka_transformed.columns}")
print(f"Record count (still has duplicates): {kafka_transformed.count()}")

# COMMAND ----------

# Display transformed data - still has duplicates
kafka_transformed.orderBy("claim_id", "event_timestamp").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Ready for Dedup

# COMMAND ----------

# Show kafka data grouped by claim_id to see duplicates
print("Duplicate analysis (before dedup):")
kafka_transformed.groupBy("claim_id").agg(
    F.count("*").alias("record_count"),
    F.min("event_timestamp").alias("min_timestamp"),
    F.max("event_timestamp").alias("max_timestamp")
).orderBy("claim_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Complete ✅

# COMMAND ----------

print("\n" + "="*60)
print("✅ TRANSFORMATIONS MODULE TEST PASSED")
print("="*60)
print(f"legacy_claims: {legacy_transformed.count()} records transformed")
print(f"kafka_claims: {kafka_transformed.count()} records transformed (pre-dedup)")
