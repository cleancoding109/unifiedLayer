# Databricks notebook source
# Create Pega Underwriting Workflow Source Tables

catalog = "ltc_insurance"
schema = "raw_data_layer"
print(f"Creating Pega tables in: {catalog}.{schema}")

# COMMAND ----------

# Create rdl_pega_uw_events_st (Pega Event Stream - real-time workflow events)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.rdl_pega_uw_events_st (
    case_id STRING,
    workflow_type STRING,
    step_name STRING,
    step_status STRING,
    assigned_to STRING,
    applicant_id STRING,
    policy_number STRING,
    decision STRING,
    decision_reason STRING,
    risk_score INT,
    premium_amount DECIMAL(10,2),
    is_deleted BOOLEAN,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("Created rdl_pega_uw_events_st (Pega Event Stream)")

# COMMAND ----------

# Create rdl_pega_uw_bix_hist_st (Pega BIX History - historical workflow data with SCD dates)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.rdl_pega_uw_bix_hist_st (
    case_id STRING,
    workflow_type STRING,
    step_name STRING,
    step_status STRING,
    assigned_to STRING,
    applicant_id STRING,
    policy_number STRING,
    decision STRING,
    decision_reason STRING,
    risk_score INT,
    premium_amount DECIMAL(10,2),
    is_deleted BOOLEAN,
    event_timestamp TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("Created rdl_pega_uw_bix_hist_st (Pega BIX History)")

# COMMAND ----------

# Verify tables created
for t in ["rdl_pega_uw_events_st", "rdl_pega_uw_bix_hist_st"]:
    c = spark.table(f"{catalog}.{schema}.{t}").count()
    print(f"{t}: {c} records")
