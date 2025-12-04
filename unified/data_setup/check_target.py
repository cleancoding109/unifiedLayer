# Databricks notebook source
# Check target table schema and data

# COMMAND ----------

# Show table schema
spark.sql("DESCRIBE TABLE ltc_insurance.unified_dev.unified_customer_scd2").show(truncate=False)

# COMMAND ----------

# Show sample data
spark.sql("SELECT * FROM ltc_insurance.unified_dev.unified_customer_scd2 LIMIT 5").show(truncate=False)

# COMMAND ----------

# Count records
count = spark.sql("SELECT COUNT(*) as cnt FROM ltc_insurance.unified_dev.unified_customer_scd2").collect()[0]['cnt']
print(f"Total records: {count}")
