# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Data Ready for Pipeline
# MAGIC Verifies that all source tables have data and are ready for the unified SCD2 pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Hardcoded configuration (spark.conf.get doesn't work with serverless)
catalog = "ltc_insurance"
schema = "raw_data_layer"

print(f"Verifying data in: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Tables and Record Counts

# COMMAND ----------

tables = {
    "rdl_customers": "Customers (Greenplum)",
    "rdl_policies": "Policies (SQL Server)",
    "rdl_claims": "Claims (Kafka CDC)"
}

counts = {}
for table, desc in tables.items():
    try:
        count = spark.table(f"{catalog}.{schema}.{table}").count()
        counts[table] = count
        status = "✅" if count > 0 else "⚠️"
        print(f"{status} {table}: {count} records - {desc}")
    except Exception as e:
        print(f"❌ {table}: ERROR - {str(e)}")
        counts[table] = -1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Required Columns

# COMMAND ----------

required_columns = {
    "rdl_customers": ["customer_id", "event_timestamp", "is_deleted"],
    "rdl_policies": ["policy_id", "event_timestamp", "is_deleted"],
    "rdl_claims": ["claim_id", "event_timestamp", "is_deleted"]
}

for table, req_cols in required_columns.items():
    df = spark.table(f"{catalog}.{schema}.{table}")
    existing_cols = df.columns
    missing = [c for c in req_cols if c not in existing_cols]
    if missing:
        print(f"⚠️ {table}: Missing columns: {missing}")
    else:
        print(f"✅ {table}: All required columns present")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Timestamp Ranges

# COMMAND ----------

from pyspark.sql import functions as F

print("Timestamp ranges per source:")
print("-" * 60)

for table in tables.keys():
    df = spark.table(f"{catalog}.{schema}.{table}")
    if "event_timestamp" in df.columns:
        stats = df.agg(
            F.min("event_timestamp").alias("min_ts"),
            F.max("event_timestamp").alias("max_ts")
        ).collect()[0]
        print(f"{table}:")
        print(f"  Min: {stats['min_ts']}")
        print(f"  Max: {stats['max_ts']}")
    else:
        print(f"{table}: No event_timestamp column")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Delete Flags

# COMMAND ----------

print("Delete flag distribution:")
print("-" * 60)

for table in tables.keys():
    df = spark.table(f"{catalog}.{schema}.{table}")
    if "is_deleted" in df.columns:
        delete_counts = df.groupBy("is_deleted").count().collect()
        print(f"{table}:")
        for row in delete_counts:
            print(f"  is_deleted={row['is_deleted']}: {row['count']} records")
    else:
        print(f"{table}: No is_deleted column")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_records = sum(c for c in counts.values() if c > 0)
all_ok = all(c > 0 for c in counts.values())

print(f"""
╔══════════════════════════════════════════════════════════════════╗
║                    DATA VERIFICATION COMPLETE                     ║
╠══════════════════════════════════════════════════════════════════╣
║  Catalog: {catalog:<54} ║
║  Schema:  {schema:<54} ║
╠══════════════════════════════════════════════════════════════════╣
║  Tables:                                                         ║
║    • rdl_customers: {counts.get('rdl_customers', 0):>8} records (Greenplum)             ║
║    • rdl_policies:  {counts.get('rdl_policies', 0):>8} records (SQL Server)             ║
║    • rdl_claims:    {counts.get('rdl_claims', 0):>8} records (Kafka CDC)               ║
║                     ────────                                     ║
║    Total:           {total_records:>8} records                               ║
╠══════════════════════════════════════════════════════════════════╣
║  Status: {'✅ Ready for unified SCD2 pipeline' if all_ok else '⚠️ Some issues detected':<42} ║
╚══════════════════════════════════════════════════════════════════╝
""")
