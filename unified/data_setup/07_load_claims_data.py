# Databricks notebook source
# MAGIC %md
# MAGIC # Load Claims Test Data
# MAGIC 
# MAGIC ## Test Scenarios for Dedup:
# MAGIC 
# MAGIC | Scenario | Claim | Description |
# MAGIC |----------|-------|-------------|
# MAGIC | Normal | CLM-001 | Single event, no duplicates |
# MAGIC | Kafka Duplicate | CLM-002 | Same event, different offsets (producer retry) |
# MAGIC | Logical Duplicate | CLM-003 | Same claim_id + event_timestamp from different partitions |
# MAGIC | Out-of-Order | CLM-004 | Events arriving out of sequence |
# MAGIC | Update Sequence | CLM-005 | Multiple status updates in order |
# MAGIC | Legacy Only | CLM-006 | Exists only in legacy system |

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType, LongType, BooleanType, DecimalType
from decimal import Decimal

catalog = "ltc_insurance"
schema = "raw_data_layer"
print(f"Loading claims data into: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Legacy Claims Data (Batch Source - No Duplicates)

# COMMAND ----------

# Clear existing data
spark.sql(f"TRUNCATE TABLE {catalog}.{schema}.rdl_claims_legacy_st")

legacy_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("policy_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("claim_amount", DecimalType(12, 2), True),
    StructField("claim_status", StringType(), True),
    StructField("filed_date", StringType(), True),
    StructField("processed_date", StringType(), True),
    StructField("adjuster_id", StringType(), True),
    StructField("description", StringType(), True),
    StructField("is_deleted", BooleanType(), False),
    StructField("event_timestamp", StringType(), False),
])

legacy_data = [
    # Historical claims from legacy system
    ("CLM-001", "POL-101", "CUST-001", "medical", Decimal("5000.00"), "approved", "2024-01-15", "2024-01-20", "ADJ-01", "Medical expense claim", False, "2024-01-20 10:00:00"),
    ("CLM-002", "POL-102", "CUST-002", "property", Decimal("15000.00"), "processing", "2024-02-01", None, "ADJ-02", "Property damage claim", False, "2024-02-01 14:00:00"),
    ("CLM-003", "POL-103", "CUST-003", "auto", Decimal("8500.00"), "filed", "2024-03-10", None, "ADJ-01", "Auto accident claim", False, "2024-03-10 09:00:00"),
    ("CLM-006", "POL-106", "CUST-006", "liability", Decimal("25000.00"), "denied", "2024-01-05", "2024-01-25", "ADJ-03", "Liability claim - denied", False, "2024-01-25 16:00:00"),
]

df_legacy = spark.createDataFrame(legacy_data, legacy_schema)
df_legacy = (df_legacy
    .withColumn("filed_date", to_date("filed_date"))
    .withColumn("processed_date", to_date("processed_date"))
    .withColumn("event_timestamp", to_timestamp("event_timestamp"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

df_legacy.write.mode("append").saveAsTable(f"{catalog}.{schema}.rdl_claims_legacy_st")
print(f"Loaded {df_legacy.count()} legacy claims")
df_legacy.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Kafka Claims Data (With Duplicates & Out-of-Order Events)

# COMMAND ----------

# Clear existing data
spark.sql(f"TRUNCATE TABLE {catalog}.{schema}.rdl_claims_kafka_st")

kafka_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("policy_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("claim_amount", DecimalType(12, 2), True),
    StructField("claim_status", StringType(), True),
    StructField("filed_date", StringType(), True),
    StructField("processed_date", StringType(), True),
    StructField("adjuster_id", StringType(), True),
    StructField("description", StringType(), True),
    StructField("is_deleted", BooleanType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", StringType(), True),
])

kafka_data = [
    # Scenario 1: Normal - Single event (CLM-001 update from Kafka)
    ("CLM-001", "POL-101", "CUST-001", "medical", Decimal("5500.00"), "paid", "2024-01-15", "2024-01-22", "ADJ-01", "Medical expense claim - PAID", False, "2024-01-22 11:00:00", 0, 100, "2024-01-22 11:00:00"),
    
    # Scenario 2: Kafka Duplicate - Same event, different offsets (CLM-002 producer retry)
    ("CLM-002", "POL-102", "CUST-002", "property", Decimal("15000.00"), "approved", "2024-02-01", "2024-02-15", "ADJ-02", "Property damage - APPROVED", False, "2024-02-15 10:00:00", 0, 200, "2024-02-15 10:00:00"),
    ("CLM-002", "POL-102", "CUST-002", "property", Decimal("15000.00"), "approved", "2024-02-01", "2024-02-15", "ADJ-02", "Property damage - APPROVED", False, "2024-02-15 10:00:00", 0, 201, "2024-02-15 10:00:01"),  # DUPLICATE - retry
    
    # Scenario 3: Logical Duplicate - Same claim_id + event_timestamp from different partitions (CLM-003)
    ("CLM-003", "POL-103", "CUST-003", "auto", Decimal("8500.00"), "processing", "2024-03-10", None, "ADJ-01", "Auto claim - PROCESSING", False, "2024-03-15 09:00:00", 0, 300, "2024-03-15 09:00:00"),
    ("CLM-003", "POL-103", "CUST-003", "auto", Decimal("8500.00"), "processing", "2024-03-10", None, "ADJ-01", "Auto claim - PROCESSING", False, "2024-03-15 09:00:00", 1, 50, "2024-03-15 09:00:01"),  # DUPLICATE - different partition
    
    # Scenario 4: Out-of-Order - Events arriving out of sequence (CLM-004)
    # Event 2 arrives BEFORE Event 1
    ("CLM-004", "POL-104", "CUST-004", "medical", Decimal("3000.00"), "approved", "2024-04-01", "2024-04-10", "ADJ-02", "Medical claim - APPROVED", False, "2024-04-10 14:00:00", 0, 401, "2024-04-05 10:00:00"),  # Newer event arrives first
    ("CLM-004", "POL-104", "CUST-004", "medical", Decimal("3000.00"), "processing", "2024-04-01", None, "ADJ-02", "Medical claim - PROCESSING", False, "2024-04-05 10:00:00", 0, 400, "2024-04-10 14:00:00"),  # Older event arrives second (out of order)
    
    # Scenario 5: Update Sequence - Multiple status updates in order (CLM-005)
    ("CLM-005", "POL-105", "CUST-005", "property", Decimal("12000.00"), "filed", "2024-05-01", None, "ADJ-03", "Property claim - FILED", False, "2024-05-01 09:00:00", 0, 500, "2024-05-01 09:00:00"),
    ("CLM-005", "POL-105", "CUST-005", "property", Decimal("12000.00"), "processing", "2024-05-01", None, "ADJ-03", "Property claim - PROCESSING", False, "2024-05-05 11:00:00", 0, 501, "2024-05-05 11:00:00"),
    ("CLM-005", "POL-105", "CUST-005", "property", Decimal("11500.00"), "approved", "2024-05-01", "2024-05-10", "ADJ-03", "Property claim - APPROVED (reduced)", False, "2024-05-10 15:00:00", 0, 502, "2024-05-10 15:00:00"),
]

df_kafka = spark.createDataFrame(kafka_data, kafka_schema)
df_kafka = (df_kafka
    .withColumn("filed_date", to_date("filed_date"))
    .withColumn("processed_date", to_date("processed_date"))
    .withColumn("event_timestamp", to_timestamp("event_timestamp"))
    .withColumn("kafka_timestamp", to_timestamp("kafka_timestamp"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

df_kafka.write.mode("append").saveAsTable(f"{catalog}.{schema}.rdl_claims_kafka_st")
print(f"Loaded {df_kafka.count()} kafka claims (including duplicates)")
df_kafka.orderBy("claim_id", "kafka_offset").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of Test Data
# MAGIC 
# MAGIC ### Legacy Claims (4 records - no duplicates)
# MAGIC | Claim | Status | Description |
# MAGIC |-------|--------|-------------|
# MAGIC | CLM-001 | approved | Will be updated by Kafka |
# MAGIC | CLM-002 | processing | Will be updated by Kafka |
# MAGIC | CLM-003 | filed | Will be updated by Kafka |
# MAGIC | CLM-006 | denied | Legacy only - no Kafka update |
# MAGIC 
# MAGIC ### Kafka Claims (11 records - with 4 duplicates)
# MAGIC | Claim | Records | Scenario |
# MAGIC |-------|---------|----------|
# MAGIC | CLM-001 | 1 | Normal update |
# MAGIC | CLM-002 | 2 | Kafka duplicate (same offset retry) |
# MAGIC | CLM-003 | 2 | Logical duplicate (different partition) |
# MAGIC | CLM-004 | 2 | Out-of-order events |
# MAGIC | CLM-005 | 3 | Normal sequence of updates |
# MAGIC 
# MAGIC ### Expected After Dedup (7 unique Kafka records)
# MAGIC - CLM-002: 2 → 1 (offset dedup)
# MAGIC - CLM-003: 2 → 1 (logical dedup)
# MAGIC - Others unchanged

# COMMAND ----------

# Final counts
print("\n=== Final Data Summary ===")
legacy_count = spark.table(f"{catalog}.{schema}.rdl_claims_legacy_st").count()
kafka_count = spark.table(f"{catalog}.{schema}.rdl_claims_kafka_st").count()
print(f"Legacy claims: {legacy_count} records (no duplicates)")
print(f"Kafka claims: {kafka_count} records (with duplicates)")
print(f"\nExpected after dedup: {kafka_count - 4} Kafka records (remove 2 offset + 2 logical dups)")
