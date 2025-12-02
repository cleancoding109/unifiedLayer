# Databricks notebook source
# MAGIC %md
# MAGIC # Preview Source Data
# MAGIC Previews the existing data in the raw_data_layer tables.
# MAGIC 
# MAGIC **Note:** Data is already loaded by upstream ingestion pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Hardcoded configuration (spark.conf.get doesn't work with serverless)
catalog = "ltc_insurance"
schema = "raw_data_layer"

print(f"Previewing data in: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customers Data (from Greenplum)

# COMMAND ----------

customers_df = spark.table(f"{catalog}.{schema}.rdl_customers")
print(f"Total customers: {customers_df.count()}")
display(customers_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Policies Data (from SQL Server)

# COMMAND ----------

policies_df = spark.table(f"{catalog}.{schema}.rdl_policies")
print(f"Total policies: {policies_df.count()}")
display(policies_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Claims Data (from Kafka CDC)

# COMMAND ----------

claims_df = spark.table(f"{catalog}.{schema}.rdl_claims")
print(f"Total claims: {claims_df.count()}")
display(claims_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

from pyspark.sql import functions as F

print(f"""
Data Summary
============
Catalog: {catalog}
Schema:  {schema}

Tables:
  • rdl_customers: {customers_df.count()} records
  • rdl_policies:  {policies_df.count()} records
  • rdl_claims:    {claims_df.count()} records
""")
