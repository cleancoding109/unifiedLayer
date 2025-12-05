# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6: Test Dedup Module
# MAGIC This notebook tests the dedup module with Kafka claims data
# MAGIC 
# MAGIC ## Test Scenarios:
# MAGIC - CLM-001: Normal (1 record) - Should pass through
# MAGIC - CLM-002: Kafka duplicate (producer retry, same offset) - 2→1
# MAGIC - CLM-003: Logical duplicate (different partition, same business key) - 2→1
# MAGIC - CLM-004: Out-of-order events (different timestamps) - 2→2 (both kept)
# MAGIC - CLM-005: Normal sequence (3 records, different timestamps) - 3→3
# MAGIC - CLM-006: Legacy only (not in Kafka) - N/A

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
from dedup import apply_dedup, get_dedup_stats, validate_dedup_config

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
# MAGIC ## Load and Prepare Kafka Data

# COMMAND ----------

# Get kafka source config
kafka_config = source_mappings_dict.get("kafka_claims", {})
kafka_source_info = sources.get("kafka_claims", {})
kafka_table = kafka_source_info.get("table_name")
kafka_column_mapping = kafka_config.get("column_mapping", {})
dedup_config = kafka_config.get("dedup_config", {})

full_table = f"{source_catalog}.{source_schema}.{kafka_table}"
print(f"Reading from: {full_table}")

# Read source
kafka_df = spark.read.table(full_table)
print(f"Source records: {kafka_df.count()}")

# Apply mapping (no transforms needed for claims)
kafka_mapped = apply_mapping(kafka_df, kafka_column_mapping, "kafka_claims")
print(f"After mapping: {kafka_mapped.count()} records")

# COMMAND ----------

# Show data before dedup
print("BEFORE DEDUP:")
kafka_mapped.orderBy("claim_id", "event_timestamp").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Dedup Config

# COMMAND ----------

print("Dedup Configuration:")
print(json.dumps(dedup_config, indent=2))

# COMMAND ----------

# Validate config
warnings = validate_dedup_config(dedup_config)
print(f"\nConfig warnings: {len(warnings)}")
for warning in warnings:
    print(f"  ⚠️ {warning}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Dedup

# COMMAND ----------

# Apply dedup
kafka_deduped = apply_dedup(kafka_mapped, dedup_config)

print(f"\nAFTER DEDUP: {kafka_deduped.count()} records")

# COMMAND ----------

# Show deduplicated data
print("AFTER DEDUP:")
kafka_deduped.orderBy("claim_id", "event_timestamp").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Dedup Results

# COMMAND ----------

# Get dedup statistics - show duplicates before dedup
dedup_keys = dedup_config.get("dedup_keys", [])
print(f"\nDedup keys: {dedup_keys}")

# Check for duplicates before dedup
if dedup_keys:
    print("\nDuplicates in source data (before dedup):")
    stats = get_dedup_stats(kafka_mapped, dedup_keys)
    stats.display()
else:
    print("No dedup keys configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Expected Results

# COMMAND ----------

# Expected results by claim_id
expected_counts = {
    "CLM-001": 1,  # Normal
    "CLM-002": 1,  # Kafka duplicate removed (2→1)
    "CLM-003": 1,  # Logical duplicate removed (2→1)
    "CLM-004": 2,  # Out-of-order kept (different timestamps)
    "CLM-005": 3,  # Normal sequence
}

print("\nVerification:")
print("="*60)

# Get actual counts
actual_counts = kafka_deduped.groupBy("claim_id").count().collect()
actual_dict = {row["claim_id"]: row["count"] for row in actual_counts}

all_passed = True
for claim_id, expected in expected_counts.items():
    actual = actual_dict.get(claim_id, 0)
    status = "✅" if actual == expected else "❌"
    if actual != expected:
        all_passed = False
    print(f"{status} {claim_id}: Expected {expected}, Got {actual}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare With Legacy (No Dedup)

# COMMAND ----------

# Get legacy source config
legacy_config = source_mappings_dict.get("legacy_claims", {})
legacy_source_info = sources.get("legacy_claims", {})
legacy_table = legacy_source_info.get("table_name")
legacy_column_mapping = legacy_config.get("column_mapping", {})
legacy_dedup_config = legacy_config.get("dedup_config", {})

full_table = f"{source_catalog}.{source_schema}.{legacy_table}"
print(f"Reading from: {full_table}")

# Read and map
legacy_df = spark.read.table(full_table)
legacy_mapped = apply_mapping(legacy_df, legacy_column_mapping, "legacy_claims")

print(f"Legacy dedup enabled: {legacy_dedup_config.get('enabled', False)}")

# Apply dedup (should pass through unchanged)
legacy_deduped = apply_dedup(legacy_mapped, legacy_dedup_config)

print(f"\nLegacy before: {legacy_mapped.count()}")
print(f"Legacy after: {legacy_deduped.count()}")
print(f"✅ Legacy unchanged: {legacy_mapped.count() == legacy_deduped.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Complete

# COMMAND ----------

print("\n" + "="*60)
if all_passed:
    print("✅ DEDUP MODULE TEST PASSED")
else:
    print("❌ DEDUP MODULE TEST FAILED - Check results above")
print("="*60)
print(f"Kafka claims: {kafka_mapped.count()} → {kafka_deduped.count()} records")
print(f"Duplicates removed: {kafka_mapped.count() - kafka_deduped.count()}")
