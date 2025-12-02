# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Definitions for Unified SCD2 Pipeline
# MAGIC Defines the schema for the unified target table and intermediate views.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    BooleanType,
    MapType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unified Target Schema
# MAGIC This schema is used for the unified SCD2 streaming table that merges all sources.

# COMMAND ----------

# Unified schema for the target SCD2 table
UNIFIED_SCHEMA = StructType([
    StructField("entity_type", StringType(), nullable=False),
    StructField("entity_id", StringType(), nullable=False),
    StructField("data", MapType(StringType(), StringType()), nullable=True),
    StructField("is_deleted", BooleanType(), nullable=True),
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("source_system", StringType(), nullable=True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Lists

# COMMAND ----------

# Primary key columns for SCD2
SCD2_KEYS = ["entity_type", "entity_id"]

# Sequencing column for SCD2 ordering
SEQUENCE_BY_COLUMN = "event_timestamp"

# Columns to exclude from history tracking
EXCEPT_COLUMNS = ["source_system"]

# Delete detection expression
APPLY_AS_DELETES = "is_deleted = true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source-Specific Columns

# COMMAND ----------

# Columns for each source that will be packed into the 'data' MAP column
SOURCE_DATA_COLUMNS = {
    "customers": [
        "customer_name", "date_of_birth", "email", "phone", 
        "state", "zip_code", "status", "last_login", 
        "session_count", "page_views"
    ],
    "policies": [
        "customer_id", "policy_status", "coverage_type", 
        "premium_amount", "effective_timestamp", "expiration_timestamp"
    ],
    "claims": [
        "policy_id", "claim_amount", "diagnosis_code", 
        "provider_id", "service_date", "filing_date"
    ]
}

# Mapping from source to entity_id column
SOURCE_ID_COLUMNS = {
    "customers": "customer_id",
    "policies": "policy_id", 
    "claims": "claim_id"
}

# Entity type for each source
SOURCE_ENTITY_TYPES = {
    "customers": "customer",
    "policies": "policy",
    "claims": "claim"
}

# Source system identifiers
SOURCE_SYSTEMS = {
    "customers": "greenplum",
    "policies": "sqlserver",
    "claims": "kafka"
}
