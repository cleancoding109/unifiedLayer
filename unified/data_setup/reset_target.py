# Databricks notebook source
# Standalone reset script - no dependencies
# Hardcode values for now to bypass config issues in standalone mode
catalog = "ltc_insurance"
schema = "unified_dev"
target_table = "unified_customer_scd2"
fqn = f"{catalog}.{schema}.{target_table}"

print(f"Dropping table {fqn}...")
try:
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")
    print("Table dropped.")
except Exception as e:
    print(f"Error dropping table: {e}")


