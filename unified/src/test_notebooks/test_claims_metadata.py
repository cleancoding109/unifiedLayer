# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Test Metadata Loading
# MAGIC This notebook tests loading and validating the claims_cdc_pipeline.json metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import json
from pprint import pprint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Metadata JSON

# COMMAND ----------

# Path configuration
src_path = "/Workspace/Shared/.bundle/unified/dev/cleancoding109/files/src"
metadata_path = f"{src_path}/metadata/stream/unified/claims_cdc/claims_cdc_pipeline.json"

print(f"Loading metadata from: {metadata_path}")

# COMMAND ----------

# Read the metadata file
with open(metadata_path, "r") as f:
    metadata = json.load(f)

print("✅ Metadata loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Pipeline Config

# COMMAND ----------

pipeline_config = metadata.get("pipeline", {})
print("Pipeline Configuration:")
print(f"  - name: {pipeline_config.get('name')}")
print(f"  - version: {pipeline_config.get('version')}")
print(f"  - description: {pipeline_config.get('description')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Sources

# COMMAND ----------

sources = metadata.get("sources", {})
print(f"\nNumber of sources: {len(sources)}")

for source_name, source_info in sources.items():
    print(f"\n--- Source: {source_name} ---")
    print(f"  - table_name: {source_info.get('table_name')}")
    print(f"  - description: {source_info.get('description')}")
    print(f"  - source_system_value: {source_info.get('source_system_value')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Target Config

# COMMAND ----------

targets = metadata.get("targets", [])
print(f"\nNumber of targets: {len(targets)}")

for target in targets:
    print(f"\n--- Target: {target.get('name')} ---")
    print(f"  - scd_type: {target.get('scd_type')}")
    print(f"  - keys: {target.get('keys')}")
    print(f"  - sequence_by: {target.get('sequence_by')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Source Mappings

# COMMAND ----------

target = targets[0] if targets else {}
source_mappings = target.get("source_mappings", {})
print(f"\nSource mappings in target: {list(source_mappings.keys())}")

for source_name, mapping in source_mappings.items():
    print(f"\n--- {source_name} ---")
    print(f"  - enabled: {mapping.get('enabled')}")
    print(f"  - view_name: {mapping.get('view_name')}")
    print(f"  - flow_name: {mapping.get('flow_name')}")
    
    # Check dedup_config
    dedup_config = mapping.get("dedup_config", {})
    if dedup_config.get("enabled"):
        print(f"  - dedup_config: ENABLED")
        print(f"    - watermark_column: {dedup_config.get('watermark_column')}")
        print(f"    - watermark_delay: {dedup_config.get('watermark_delay')}")
        print(f"    - dedup_keys: {dedup_config.get('dedup_keys')}")
        print(f"    - offset_dedup: {dedup_config.get('offset_dedup')}")
    else:
        print(f"  - dedup_config: DISABLED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Column Mappings

# COMMAND ----------

for source_name, mapping in source_mappings.items():
    column_mapping = mapping.get("column_mapping", {})
    print(f"\n{source_name} - Column Mappings ({len(column_mapping)} columns):")
    
    for i, (target_col, col_info) in enumerate(column_mapping.items()):
        if i < 5:  # Show first 5
            source_col = col_info.get("source_col") or f"(default: {col_info.get('default')})"
            print(f"  {source_col} -> {target_col}")
    
    if len(column_mapping) > 5:
        print(f"  ... and {len(column_mapping) - 5} more columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Complete ✅

# COMMAND ----------

print("\n" + "="*60)
print("✅ METADATA LOADING TEST PASSED")
print("="*60)
print(f"Pipeline: {pipeline_config.get('name')}")
print(f"Sources: {list(sources.keys())}")
print(f"Targets: {[t.get('name') for t in targets]}")
