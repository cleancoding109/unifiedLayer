# Databricks notebook source
# Load Sample Pega Underwriting Workflow Events

from pyspark.sql.functions import lit, current_timestamp, to_timestamp
from datetime import datetime, timedelta

catalog = "ltc_insurance"
schema = "raw_data_layer"
print(f"Loading Pega test data into: {catalog}.{schema}")

# COMMAND ----------

# Clear existing data for fresh test
spark.sql(f"TRUNCATE TABLE {catalog}.{schema}.rdl_pega_uw_events_st")
spark.sql(f"TRUNCATE TABLE {catalog}.{schema}.rdl_pega_uw_bix_hist_st")
print("Cleared existing data")

# COMMAND ----------

# Sample underwriting workflow events for Pega Event Stream
# Simulates real-time workflow events coming from Kafka

pega_stream_data = [
    # Case 001: New Business Application - complete workflow progression
    ("CASE-001", "NewBusiness", "InitialReview", "Completed", "TeamA", "APPL-101", None, None, None, None, None, False, "2024-01-15 09:00:00"),
    ("CASE-001", "NewBusiness", "MedicalReview", "Completed", "MedicalTeam", "APPL-101", None, None, None, 72, None, False, "2024-01-16 14:30:00"),
    ("CASE-001", "NewBusiness", "FinalDecision", "Completed", "SeniorUW", "APPL-101", "POL-2024-001", "Approved", "Standard Risk", 72, 1250.00, False, "2024-01-18 11:00:00"),
    
    # Case 002: Referred for additional review
    ("CASE-002", "NewBusiness", "InitialReview", "Completed", "TeamB", "APPL-102", None, None, None, None, None, False, "2024-01-17 10:15:00"),
    ("CASE-002", "NewBusiness", "MedicalReview", "InProgress", "MedicalTeam", "APPL-102", None, "Refer", "Elevated Risk Score", 89, None, False, "2024-01-18 16:45:00"),
    
    # Case 003: Declined application
    ("CASE-003", "NewBusiness", "InitialReview", "Completed", "TeamA", "APPL-103", None, None, None, None, None, False, "2024-01-19 08:30:00"),
    ("CASE-003", "NewBusiness", "FinalDecision", "Completed", "SeniorUW", "APPL-103", None, "Declined", "High Risk - Pre-existing Condition", 95, None, False, "2024-01-19 15:00:00"),
    
    # Case 004: Renewal workflow
    ("CASE-004", "Renewal", "RenewalReview", "InProgress", "RenewalTeam", "APPL-104", "POL-2023-050", None, None, 45, 980.00, False, "2024-01-20 09:00:00"),
    
    # Case 005: Cancelled/Deleted case
    ("CASE-005", "NewBusiness", "InitialReview", "Cancelled", "TeamB", "APPL-105", None, None, "Applicant Withdrew", None, None, True, "2024-01-20 11:30:00"),
]

stream_df = spark.createDataFrame(pega_stream_data, [
    "case_id", "workflow_type", "step_name", "step_status", "assigned_to", 
    "applicant_id", "policy_number", "decision", "decision_reason", 
    "risk_score", "premium_amount", "is_deleted", "event_ts_str"
])

stream_df = (stream_df
    .withColumn("event_timestamp", to_timestamp("event_ts_str", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .drop("event_ts_str")
)

stream_df.write.mode("append").saveAsTable(f"{catalog}.{schema}.rdl_pega_uw_events_st")
print(f"Loaded {stream_df.count()} records into rdl_pega_uw_events_st")

# COMMAND ----------

# Sample BIX History data - historical workflow records with SCD dates
# This simulates the historical extract from Pega BIX with valid_from/valid_to

pega_bix_data = [
    # Case 006: Historical case with full SCD history from BIX
    ("CASE-006", "NewBusiness", "InitialReview", "Completed", "TeamC", "APPL-106", None, None, None, None, None, False, 
     "2023-12-01 10:00:00", "2023-12-01 10:00:00", "2023-12-02 14:00:00", False),
    ("CASE-006", "NewBusiness", "MedicalReview", "Completed", "MedicalTeam", "APPL-106", None, None, None, 55, None, False, 
     "2023-12-02 14:00:00", "2023-12-02 14:00:00", "2023-12-05 09:30:00", False),
    ("CASE-006", "NewBusiness", "FinalDecision", "Completed", "SeniorUW", "APPL-106", "POL-2023-120", "Approved", "Preferred Risk", 55, 875.00, False, 
     "2023-12-05 09:30:00", "2023-12-05 09:30:00", None, True),
    
    # Case 007: Historical case that was declined
    ("CASE-007", "NewBusiness", "InitialReview", "Completed", "TeamA", "APPL-107", None, None, None, None, None, False, 
     "2023-11-15 08:00:00", "2023-11-15 08:00:00", "2023-11-16 10:00:00", False),
    ("CASE-007", "NewBusiness", "FinalDecision", "Completed", "SeniorUW", "APPL-107", None, "Declined", "Failed Medical Requirements", 92, None, False, 
     "2023-11-16 10:00:00", "2023-11-16 10:00:00", None, True),
    
    # Case 008: Historical renewal with rate increase
    ("CASE-008", "Renewal", "RenewalReview", "Completed", "RenewalTeam", "APPL-108", "POL-2022-080", None, None, 60, 1100.00, False, 
     "2023-10-01 09:00:00", "2023-10-01 09:00:00", "2023-10-05 16:00:00", False),
    ("CASE-008", "Renewal", "FinalDecision", "Completed", "SeniorUW", "APPL-108", "POL-2022-080", "Approved", "Rate Increase - Age Factor", 60, 1320.00, False, 
     "2023-10-05 16:00:00", "2023-10-05 16:00:00", None, True),
    
    # Case 009: Current case also in BIX (overlap scenario)
    ("CASE-001", "NewBusiness", "InitialReview", "Completed", "TeamA", "APPL-101", None, None, None, None, None, False, 
     "2024-01-15 09:00:00", "2024-01-15 09:00:00", "2024-01-16 14:30:00", False),
]

bix_df = spark.createDataFrame(pega_bix_data, [
    "case_id", "workflow_type", "step_name", "step_status", "assigned_to", 
    "applicant_id", "policy_number", "decision", "decision_reason", 
    "risk_score", "premium_amount", "is_deleted", "event_ts_str",
    "valid_from_str", "valid_to_str", "is_current"
])

bix_df = (bix_df
    .withColumn("event_timestamp", to_timestamp("event_ts_str", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("valid_from", to_timestamp("valid_from_str", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("valid_to", to_timestamp("valid_to_str", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .drop("event_ts_str", "valid_from_str", "valid_to_str")
)

bix_df.write.mode("append").saveAsTable(f"{catalog}.{schema}.rdl_pega_uw_bix_hist_st")
print(f"Loaded {bix_df.count()} records into rdl_pega_uw_bix_hist_st")

# COMMAND ----------

# Verify data loaded
print("\n=== Data Summary ===")
for t in ["rdl_pega_uw_events_st", "rdl_pega_uw_bix_hist_st"]:
    df = spark.table(f"{catalog}.{schema}.{t}")
    print(f"\n{t}: {df.count()} records")
    df.select("case_id", "step_name", "step_status", "decision").show(truncate=False)

# COMMAND ----------

# Show workflow progression for a case
print("\n=== CASE-001 Workflow Progression (from Event Stream) ===")
spark.sql(f"""
    SELECT case_id, step_name, step_status, decision, risk_score, premium_amount, event_timestamp
    FROM {catalog}.{schema}.rdl_pega_uw_events_st
    WHERE case_id = 'CASE-001'
    ORDER BY event_timestamp
""").show(truncate=False)

print("\n=== CASE-006 Workflow History (from BIX with SCD dates) ===")
spark.sql(f"""
    SELECT case_id, step_name, step_status, decision, risk_score, premium_amount, 
           event_timestamp, valid_from, valid_to, is_current
    FROM {catalog}.{schema}.rdl_pega_uw_bix_hist_st
    WHERE case_id = 'CASE-006'
    ORDER BY event_timestamp
""").show(truncate=False)
