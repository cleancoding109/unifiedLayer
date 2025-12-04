# Databricks notebook source
# MAGIC %md
# MAGIC # Create Claims Source Tables
# MAGIC 
# MAGIC Creates two source tables for testing dedup functionality:
# MAGIC 1. `rdl_claims_legacy_st` - Legacy claims system (batch, no duplicates)
# MAGIC 2. `rdl_claims_kafka_st` - Kafka CDC claims stream (with Kafka metadata for dedup)

# COMMAND ----------

catalog = "ltc_insurance"
schema = "raw_data_layer"
print(f"Creating claims tables in: {catalog}.{schema}")

# COMMAND ----------

# Create rdl_claims_legacy_st (Legacy claims - batch source, no Kafka metadata)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.rdl_claims_legacy_st (
    claim_id STRING,
    policy_id STRING,
    customer_id STRING,
    claim_type STRING,
    claim_amount DECIMAL(12,2),
    claim_status STRING,
    filed_date DATE,
    processed_date DATE,
    adjuster_id STRING,
    description STRING,
    is_deleted BOOLEAN,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("Created rdl_claims_legacy_st (Legacy Claims)")

# COMMAND ----------

# Create rdl_claims_kafka_st (Kafka CDC claims - with Kafka metadata for dedup testing)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.rdl_claims_kafka_st (
    claim_id STRING,
    policy_id STRING,
    customer_id STRING,
    claim_type STRING,
    claim_amount DECIMAL(12,2),
    claim_status STRING,
    filed_date DATE,
    processed_date DATE,
    adjuster_id STRING,
    description STRING,
    is_deleted BOOLEAN,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    -- Kafka metadata columns for dedup
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("Created rdl_claims_kafka_st (Kafka Claims with dedup columns)")

# COMMAND ----------

# Verify tables exist
print("\nTables created:")
for t in ["rdl_claims_legacy_st", "rdl_claims_kafka_st"]:
    try:
        count = spark.table(f"{catalog}.{schema}.{t}").count()
        print(f"  ✓ {t}: {count} records")
    except Exception as e:
        print(f"  ✗ {t}: ERROR - {e}")
