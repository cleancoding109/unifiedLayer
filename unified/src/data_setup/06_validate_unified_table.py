# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Unified SCD2 Table
# MAGIC 
# MAGIC This notebook validates the output of the unification pipeline.

# COMMAND ----------

# Configuration
TARGET_TABLE = "ltc_insurance.unified_dev.unified_customer_scd2"
GP_SOURCE = "ltc_insurance.raw_data_layer.rdl_customer_hist_st"
SQL_SOURCE = "ltc_insurance.raw_data_layer.rdl_customer_init_st"
CDC_SOURCE = "ltc_insurance.raw_data_layer.rdl_customer"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check Record Counts by Source

# COMMAND ----------

print("=== Record Counts by Source System ===")
df = spark.sql(f"""
    SELECT 
        source_system,
        COUNT(*) as total_records,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN __END_AT IS NOT NULL THEN 1 ELSE 0 END) as historical_records
    FROM {TARGET_TABLE}
    GROUP BY source_system
    ORDER BY source_system
""")
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sample SCD2 Records

# COMMAND ----------

print("=== Sample SCD2 Records ===")
df = spark.sql(f"""
    SELECT 
        customer_id,
        customer_name,
        status,
        source_system,
        event_timestamp,
        __START_AT,
        __END_AT,
        CASE WHEN __END_AT IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END as record_status
    FROM {TARGET_TABLE}
    ORDER BY customer_id, __START_AT
    LIMIT 20
""")
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Customers with Multiple Versions

# COMMAND ----------

print("=== Customers with Multiple Versions (SCD2 Working) ===")
df = spark.sql(f"""
    SELECT 
        customer_id,
        COUNT(*) as version_count
    FROM {TARGET_TABLE}
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    ORDER BY version_count DESC
    LIMIT 10
""")
if df.count() > 0:
    df.show(truncate=False)
else:
    print("No customers with multiple versions found (this is expected if no updates occurred)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Timestamp Ranges by Source

# COMMAND ----------

print("=== Timestamp Ranges by Source ===")
df = spark.sql(f"""
    SELECT 
        source_system,
        MIN(event_timestamp) as earliest_event,
        MAX(event_timestamp) as latest_event,
        MIN(__START_AT) as earliest_scd2_start,
        MAX(__START_AT) as latest_scd2_start
    FROM {TARGET_TABLE}
    GROUP BY source_system
    ORDER BY earliest_event
""")
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delete Flag Check

# COMMAND ----------

print("=== Delete Flag Distribution ===")
df = spark.sql(f"""
    SELECT 
        is_deleted,
        COUNT(*) as count
    FROM {TARGET_TABLE}
    GROUP BY is_deleted
""")
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Source vs Target Comparison

# COMMAND ----------

print("=== Source vs Target Record Counts ===")
df = spark.sql(f"""
    SELECT 'rdl_customer_hist_st (Greenplum)' as source, COUNT(*) as count FROM {GP_SOURCE}
    UNION ALL
    SELECT 'rdl_customer_init_st (SQL Init)' as source, COUNT(*) as count FROM {SQL_SOURCE}
    UNION ALL
    SELECT 'rdl_customer (Kafka CDC)' as source, COUNT(*) as count FROM {CDC_SOURCE}
    UNION ALL
    SELECT 'unified_customer_scd2 (Target)' as source, COUNT(*) as count FROM {TARGET_TABLE}
""")
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary Statistics

# COMMAND ----------

print("=== Summary Statistics ===")
df = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(DISTINCT source_system) as source_systems,
        SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN is_deleted = true THEN 1 ELSE 0 END) as deleted_records
    FROM {TARGET_TABLE}
""")
df.show(truncate=False)

# COMMAND ----------

print("✅ Validation Complete!")
