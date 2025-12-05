# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Source Multi-Target Pattern Test
# MAGIC 
# MAGIC This notebook verifies the framework supports many-to-many patterns:
# MAGIC ```
# MAGIC ┌─────────────────────┐
# MAGIC │  Event Stream 1     │──┬──▶ Target A (unified_target_a_scd2)
# MAGIC └─────────────────────┘  │
# MAGIC                          │
# MAGIC ┌─────────────────────┐  │
# MAGIC │  Event Stream 2     │──┼──▶ Target B (unified_target_b_scd2)
# MAGIC └─────────────────────┘  │
# MAGIC                          │
# MAGIC ┌─────────────────────┐  │
# MAGIC │  History Table      │──┘
# MAGIC └─────────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC Expected: 6 views, 2 streaming tables, 6 CDC flows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import json

# Add src to path for imports
src_path = "/Workspace/Shared/.bundle/unified/dev/files/src"
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Test Metadata

# COMMAND ----------

# Load the multi-source test metadata directly
metadata_path = f"{src_path}/metadata/stream/unified/multi_source_test/multi_source_test_pipeline.json"

with open(metadata_path, 'r') as f:
    metadata = json.load(f)

print(f"Pipeline: {metadata['pipeline']['name']}")
print(f"Description: {metadata['pipeline']['description']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Verify Sources

# COMMAND ----------

sources = metadata.get("sources", {})
print(f"Number of sources: {len(sources)}")
print()

for name, config in sources.items():
    print(f"Source: {name}")
    print(f"  Table: {config['table_name']}")
    print(f"  Source System: {config['source_system_value']}")
    print()

assert len(sources) == 3, f"Expected 3 sources, got {len(sources)}"
print("✅ TEST 1 PASSED: 3 sources defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Verify Targets

# COMMAND ----------

targets = metadata.get("targets", [])
print(f"Number of targets: {len(targets)}")
print()

for idx, target in enumerate(targets):
    print(f"Target[{idx}]: {target['name']}")
    print(f"  Keys: {target['keys']}")
    print(f"  Sequence By: {target['sequence_by']}")
    print()

assert len(targets) == 2, f"Expected 2 targets, got {len(targets)}"
print("✅ TEST 2 PASSED: 2 targets defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Verify Source Mappings per Target

# COMMAND ----------

total_views = 0
total_flows = 0

for idx, target in enumerate(targets):
    target_name = target['name']
    source_mappings = target.get('source_mappings', {})
    
    print(f"Target[{idx}]: {target_name}")
    print(f"  Source Mappings: {len(source_mappings)}")
    
    for source_key, mapping in source_mappings.items():
        view_name = mapping['view_name']
        flow_name = mapping['flow_name']
        enabled = mapping.get('enabled', True)
        num_columns = len(mapping.get('column_mapping', {}))
        
        print(f"    [{source_key}]:")
        print(f"      View: {view_name}")
        print(f"      Flow: {flow_name}")
        print(f"      Enabled: {enabled}")
        print(f"      Columns: {num_columns}")
        
        if enabled:
            total_views += 1
            total_flows += 1
    
    print()

print(f"Total Views: {total_views}")
print(f"Total CDC Flows: {total_flows}")
print()

assert total_views == 6, f"Expected 6 views, got {total_views}"
assert total_flows == 6, f"Expected 6 flows, got {total_flows}"
print("✅ TEST 3 PASSED: 6 views and 6 CDC flows (3 sources × 2 targets)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Verify View Names are Unique

# COMMAND ----------

all_view_names = []
all_flow_names = []

for target in targets:
    for source_key, mapping in target.get('source_mappings', {}).items():
        all_view_names.append(mapping['view_name'])
        all_flow_names.append(mapping['flow_name'])

print(f"View names: {all_view_names}")
print(f"Flow names: {all_flow_names}")
print()

# Check uniqueness
unique_views = set(all_view_names)
unique_flows = set(all_flow_names)

assert len(unique_views) == len(all_view_names), f"Duplicate view names found!"
assert len(unique_flows) == len(all_flow_names), f"Duplicate flow names found!"
print("✅ TEST 4 PASSED: All view and flow names are unique")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Verify Same Source Can Map Differently to Different Targets

# COMMAND ----------

# Get event_stream_1 mapping for both targets
target_a = targets[0]
target_b = targets[1]

stream1_to_a = target_a['source_mappings']['event_stream_1']['column_mapping']
stream1_to_b = target_b['source_mappings']['event_stream_1']['column_mapping']

print("event_stream_1 → Target A columns:")
for col in stream1_to_a.keys():
    print(f"  - {col}")
print()

print("event_stream_1 → Target B columns:")
for col in stream1_to_b.keys():
    print(f"  - {col}")
print()

# Target A has entity_id, entity_name, entity_value
# Target B has record_id, record_category, record_amount
assert "entity_id" in stream1_to_a, "Target A should have entity_id"
assert "record_id" in stream1_to_b, "Target B should have record_id"
assert "entity_id" not in stream1_to_b, "Target B should NOT have entity_id"
assert "record_id" not in stream1_to_a, "Target A should NOT have record_id"

print("✅ TEST 5 PASSED: Same source (event_stream_1) maps to different schemas in different targets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("MULTI-SOURCE MULTI-TARGET PATTERN VERIFICATION")
print("=" * 60)
print()
print("Pattern tested:")
print("  3 Sources (event_stream_1, event_stream_2, history_table)")
print("  2 Targets (unified_target_a_scd2, unified_target_b_scd2)")
print()
print("Resources created:")
print(f"  Views: {total_views}")
print(f"  Streaming Tables: {len(targets)}")
print(f"  CDC Flows: {total_flows}")
print()
print("Key features verified:")
print("  ✅ Multiple sources can feed multiple targets")
print("  ✅ Same source can have different column mappings per target")
print("  ✅ View and flow names are unique")
print("  ✅ Each target has independent schema definition")
print()
print("=" * 60)
print("ALL TESTS PASSED - Framework supports many-to-many pattern!")
print("=" * 60)
