# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Source Tables Exist
# MAGIC Verifies that the source tables exist in the raw_data_layer schema.
# MAGIC 
# MAGIC **Note:** Tables are already created and managed by the upstream data ingestion pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Hardcoded configuration (spark.conf.get doesn't work with serverless)
catalog = "ltc_insurance"
schema = "raw_data_layer"

print(f"Verifying tables in: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables Exist

# COMMAND ----------

expected_tables = ["rdl_customers", "rdl_policies", "rdl_claims"]
missing_tables = []

for table in expected_tables:
    try:
        df = spark.table(f"{catalog}.{schema}.{table}")
        count = df.count()
        print(f"✅ {table}: {count} records")
    except Exception as e:
        print(f"❌ {table}: MISSING - {str(e)}")
        missing_tables.append(table)

if missing_tables:
    raise Exception(f"Missing required tables: {missing_tables}")
else:
    print("\n✅ All source tables verified!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check CDF Enabled

# COMMAND ----------

for table in expected_tables:
    props = spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.{table}").filter("col_name = 'Table Properties'").collect()
    if props:
        table_props = props[0]["data_type"]
        cdf_enabled = "delta.enableChangeDataFeed=true" in table_props.lower()
        status = "✅" if cdf_enabled else "⚠️"
        print(f"{status} {table}: CDF enabled = {cdf_enabled}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Schema

# COMMAND ----------

for table in expected_tables:
    print(f"\n=== {table} ===")
    spark.table(f"{catalog}.{schema}.{table}").printSchema()
