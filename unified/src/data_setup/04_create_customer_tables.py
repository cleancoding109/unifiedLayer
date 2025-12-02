# Databricks notebook source
# Create Customer Source Tables

catalog = "ltc_insurance"
schema = "raw_data_layer"
print(f"Creating tables in: {catalog}.{schema}")

# COMMAND ----------

# Create rdl_customer
spark.sql("""
CREATE TABLE IF NOT EXISTS ltc_insurance.raw_data_layer.rdl_customer (
    customer_id STRING,
    customer_name STRING,
    date_of_birth DATE,
    email STRING,
    phone STRING,
    state STRING,
    zip_code STRING,
    status STRING,
    last_login TIMESTAMP,
    session_count INT,
    page_views INT,
    is_deleted BOOLEAN,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    _version BIGINT
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("Created rdl_customer")

# COMMAND ----------

# Create rdl_customer_init_st
spark.sql("""
CREATE TABLE IF NOT EXISTS ltc_insurance.raw_data_layer.rdl_customer_init_st (
    customer_id STRING,
    customer_name STRING,
    date_of_birth DATE,
    email STRING,
    phone STRING,
    state STRING,
    zip_code STRING,
    status STRING,
    last_login TIMESTAMP,
    session_count INT,
    page_views INT,
    is_deleted BOOLEAN,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_system STRING,
    _version BIGINT
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("Created rdl_customer_init_st")

# COMMAND ----------

# Create rdl_customer_hist_st
spark.sql("""
CREATE TABLE IF NOT EXISTS ltc_insurance.raw_data_layer.rdl_customer_hist_st (
    customer_id STRING,
    customer_name STRING,
    date_of_birth DATE,
    email STRING,
    phone STRING,
    state STRING,
    zip_code STRING,
    status STRING,
    last_login TIMESTAMP,
    session_count INT,
    page_views INT,
    is_deleted BOOLEAN,
    event_timestamp TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    ingestion_timestamp TIMESTAMP,
    source_system STRING,
    _version BIGINT
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("Created rdl_customer_hist_st")

# COMMAND ----------

# Verify
for t in ["rdl_customer", "rdl_customer_init_st", "rdl_customer_hist_st"]:
    c = spark.table(f"ltc_insurance.raw_data_layer.{t}").count()
    print(f"{t}: {c} records")
