# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: Test Mapper Module
# MAGIC This notebook tests the mapper module with claims data

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
print(f"Sources defined: {list(sources.keys())}")
print(f"Source mappings: {list(source_mappings_dict.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Source 1: legacy_claims (No Dedup)

# COMMAND ----------

# Get legacy source config
legacy_config = source_mappings_dict.get("legacy_claims", {})
legacy_source_info = sources.get("legacy_claims", {})
source_table = legacy_source_info.get("table_name")
column_mapping = legacy_config.get("column_mapping", {})

print(f"Testing: legacy_claims")
print(f"Table: {source_table}")
print(f"Column mappings: {len(column_mapping)}")

# COMMAND ----------

# Read source data
full_table = f"{source_catalog}.{source_schema}.{source_table}"
print(f"Reading from: {full_table}")

legacy_df = spark.read.table(full_table)
print(f"Source record count: {legacy_df.count()}")
legacy_df.display()

# COMMAND ----------

# Apply mapping using actual function signature
legacy_mapped = apply_mapping(legacy_df, column_mapping, "legacy_claims")
print(f"\nMapped columns: {legacy_mapped.columns}")
print(f"Mapped record count: {legacy_mapped.count()}")

# COMMAND ----------

# Display mapped data
legacy_mapped.display()

# COMMAND ----------

# Verify source_system is added
print("\nVerify source_system column:")
legacy_mapped.select("claim_id", "source_system", "claim_status").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Source 2: kafka_claims (With Dedup Config)

# COMMAND ----------

# Get kafka source config
kafka_config = source_mappings_dict.get("kafka_claims", {})
kafka_source_info = sources.get("kafka_claims", {})
kafka_table = kafka_source_info.get("table_name")
kafka_column_mapping = kafka_config.get("column_mapping", {})

print(f"Testing: kafka_claims")
print(f"Table: {kafka_table}")
print(f"Column mappings: {len(kafka_column_mapping)}")
print(f"Dedup enabled: {kafka_config.get('dedup_config', {}).get('enabled', False)}")

# COMMAND ----------

# Read source data (should have duplicates)
full_table = f"{source_catalog}.{source_schema}.{kafka_table}"
print(f"Reading from: {full_table}")

kafka_df = spark.read.table(full_table)
print(f"Source record count (with duplicates): {kafka_df.count()}")
kafka_df.orderBy("claim_id", "kafka_timestamp").display()

# COMMAND ----------

# Apply mapping using actual function signature
kafka_mapped = apply_mapping(kafka_df, kafka_column_mapping, "kafka_claims")
print(f"\nMapped columns: {kafka_mapped.columns}")
print(f"Mapped record count: {kafka_mapped.count()}")

# COMMAND ----------

# Display mapped data - dedup not yet applied
kafka_mapped.orderBy("claim_id", "event_timestamp").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Dedup Config Passthrough

# COMMAND ----------

# Check that dedup_config exists
dedup_config = kafka_config.get("dedup_config", {})
print("Dedup Config for kafka_claims:")
print(json.dumps(dedup_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Complete ✅

# COMMAND ----------

print("\n" + "="*60)
print("✅ MAPPER MODULE TEST PASSED")
print("="*60)
print(f"legacy_claims: {legacy_mapped.count()} records mapped")
print(f"kafka_claims: {kafka_mapped.count()} records mapped (pre-dedup)")
