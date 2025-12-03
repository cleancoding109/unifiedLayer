# Databricks notebook source
# Load Sample Data into Customer Tables

catalog = "ltc_insurance"
schema = "raw_data_layer"
print(f"Loading data into: {catalog}.{schema}")

# COMMAND ----------

# Check if data already exists
existing_count = spark.table(f"{catalog}.{schema}.rdl_customer").count()
print(f"Existing records in rdl_customer: {existing_count}")

if existing_count > 0:
    print("Data already loaded, skipping...")
else:
    # Load using SQL INSERT
    spark.sql("""
    INSERT INTO ltc_insurance.raw_data_layer.rdl_customer 
    (customer_id, customer_name, date_of_birth, email, phone, state, zip_code, status, last_login, session_count, page_views, is_deleted, event_timestamp, ingestion_timestamp, _version)
    VALUES
    ('C001', 'John Smith', DATE'1985-03-15', 'john.smith@email.com', '555-0101', 'CA', '90210', 'active', TIMESTAMP'2025-11-01 10:30:00', 45, 230, false, TIMESTAMP'2025-11-01 10:30:00', current_timestamp(), 1),
    ('C002', 'Jane Doe', DATE'1990-07-22', 'jane.doe@email.com', '555-0102', 'NY', '10001', 'active', TIMESTAMP'2025-11-15 14:20:00', 32, 180, false, TIMESTAMP'2025-11-15 14:20:00', current_timestamp(), 1),
    ('C003', 'Bob Wilson', DATE'1978-12-08', 'bob.wilson@email.com', '555-0103', 'TX', '75001', 'inactive', TIMESTAMP'2025-09-20 09:00:00', 12, 45, false, TIMESTAMP'2025-09-20 09:00:00', current_timestamp(), 1),
    ('C004', 'Alice Brown', DATE'1995-05-30', 'alice.brown@email.com', '555-0104', 'FL', '33101', 'active', TIMESTAMP'2025-11-28 16:45:00', 78, 420, false, TIMESTAMP'2025-11-28 16:45:00', current_timestamp(), 1),
    ('C005', 'Charlie Davis', DATE'1982-09-12', 'charlie.davis@email.com', '555-0105', 'WA', '98101', 'suspended', TIMESTAMP'2025-10-05 11:15:00', 5, 20, false, TIMESTAMP'2025-10-05 11:15:00', current_timestamp(), 1)
    """)
    print("Loaded rdl_customer")

# COMMAND ----------

# Load rdl_customer_init_st
existing_init = spark.table(f"{catalog}.{schema}.rdl_customer_init_st").count()
print(f"Existing records in rdl_customer_init_st: {existing_init}")

if existing_init > 0:
    print("Data already loaded, skipping...")
else:
    spark.sql("""
    INSERT INTO ltc_insurance.raw_data_layer.rdl_customer_init_st 
    (customer_id, customer_name, date_of_birth, email, phone, state, zip_code, status, last_login, session_count, page_views, is_deleted, event_timestamp, ingestion_timestamp, source_system, _version)
    VALUES
    ('C001', 'John Smith', DATE'1985-03-15', 'john.smith@email.com', '555-0101', 'CA', '90210', 'active', TIMESTAMP'2025-01-01 00:00:00', 10, 50, false, TIMESTAMP'2025-01-01 00:00:00', current_timestamp(), 'greenplum_init', 1),
    ('C002', 'Jane Doe', DATE'1990-07-22', 'jane.doe@email.com', '555-0102', 'NY', '10001', 'active', TIMESTAMP'2025-01-01 00:00:00', 5, 25, false, TIMESTAMP'2025-01-01 00:00:00', current_timestamp(), 'greenplum_init', 1),
    ('C003', 'Bob Wilson', DATE'1978-12-08', 'bob.wilson@email.com', '555-0103', 'TX', '75001', 'active', TIMESTAMP'2025-01-01 00:00:00', 3, 10, false, TIMESTAMP'2025-01-01 00:00:00', current_timestamp(), 'greenplum_init', 1),
    ('C004', 'Alice Brown', DATE'1995-05-30', 'alice.brown@email.com', '555-0104', 'FL', '33101', 'pending', TIMESTAMP'2025-01-01 00:00:00', 0, 0, false, TIMESTAMP'2025-01-01 00:00:00', current_timestamp(), 'greenplum_init', 1),
    ('C005', 'Charlie Davis', DATE'1982-09-12', 'charlie.davis@email.com', '555-0105', 'WA', '98101', 'active', TIMESTAMP'2025-01-01 00:00:00', 2, 8, false, TIMESTAMP'2025-01-01 00:00:00', current_timestamp(), 'greenplum_init', 1)
    """)
    print("Loaded rdl_customer_init_st")

# COMMAND ----------

# Load rdl_customer_hist_st
existing_hist = spark.table(f"{catalog}.{schema}.rdl_customer_hist_st").count()
print(f"Existing records in rdl_customer_hist_st: {existing_hist}")

if existing_hist > 0:
    print("Data already loaded, skipping...")
else:
    spark.sql("""
    INSERT INTO ltc_insurance.raw_data_layer.rdl_customer_hist_st 
    (customer_id, customer_name, date_of_birth, email, phone, state, zip_code, status, last_login, session_count, page_views, is_deleted, event_timestamp, valid_from, valid_to, is_current, ingestion_timestamp, source_system, _version)
    VALUES
    ('C001', 'John Smith', DATE'1985-03-15', 'john.smith@email.com', '555-0101', 'CA', '90210', 'pending', TIMESTAMP'2025-01-01 00:00:00', 0, 0, false, TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-03-15 10:00:00', false, current_timestamp(), 'greenplum_hist', 1),
    ('C001', 'John Smith', DATE'1985-03-15', 'john.smith@email.com', '555-0101', 'CA', '90210', 'active', TIMESTAMP'2025-03-15 10:00:00', 10, 50, false, TIMESTAMP'2025-03-15 10:00:00', TIMESTAMP'2025-03-15 10:00:00', TIMESTAMP'2025-11-01 10:30:00', false, current_timestamp(), 'greenplum_hist', 2),
    ('C001', 'John Smith', DATE'1985-03-15', 'john.smith@email.com', '555-0101', 'CA', '90210', 'active', TIMESTAMP'2025-11-01 10:30:00', 45, 230, false, TIMESTAMP'2025-11-01 10:30:00', TIMESTAMP'2025-11-01 10:30:00', NULL, true, current_timestamp(), 'greenplum_hist', 3),
    ('C002', 'Jane Doe', DATE'1990-07-22', 'jane.doe@email.com', '555-0102', 'NY', '10001', 'pending', TIMESTAMP'2025-01-01 00:00:00', 0, 0, false, TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-02-01 09:00:00', false, current_timestamp(), 'greenplum_hist', 1),
    ('C002', 'Jane Doe', DATE'1990-07-22', 'jane.doe@email.com', '555-0102', 'NY', '10001', 'active', TIMESTAMP'2025-02-01 09:00:00', 15, 80, false, TIMESTAMP'2025-02-01 09:00:00', TIMESTAMP'2025-02-01 09:00:00', NULL, true, current_timestamp(), 'greenplum_hist', 2),
    ('C003', 'Bob Wilson', DATE'1978-12-08', 'bob.wilson@email.com', '555-0103', 'TX', '75001', 'active', TIMESTAMP'2025-01-01 00:00:00', 3, 10, false, TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-09-20 09:00:00', false, current_timestamp(), 'greenplum_hist', 1),
    ('C003', 'Bob Wilson', DATE'1978-12-08', 'bob.wilson@email.com', '555-0103', 'TX', '75001', 'inactive', TIMESTAMP'2025-09-20 09:00:00', 12, 45, false, TIMESTAMP'2025-09-20 09:00:00', TIMESTAMP'2025-09-20 09:00:00', NULL, true, current_timestamp(), 'greenplum_hist', 2),
    ('C006', 'Deleted User', DATE'1975-01-01', 'deleted@email.com', '555-0106', 'NV', '89101', 'active', TIMESTAMP'2025-01-01 00:00:00', 5, 20, false, TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-01-01 00:00:00', TIMESTAMP'2025-06-01 12:00:00', false, current_timestamp(), 'greenplum_hist', 1),
    ('C006', 'Deleted User', DATE'1975-01-01', 'deleted@email.com', '555-0106', 'NV', '89101', 'deleted', TIMESTAMP'2025-06-01 12:00:00', 5, 20, true, TIMESTAMP'2025-06-01 12:00:00', TIMESTAMP'2025-06-01 12:00:00', NULL, true, current_timestamp(), 'greenplum_hist', 2)
    """)
    print("Loaded rdl_customer_hist_st")

# COMMAND ----------

# Verify final counts
print("\nFinal Data Summary:")
for t in ["rdl_customer", "rdl_customer_init_st", "rdl_customer_hist_st"]:
    c = spark.table(f"{catalog}.{schema}.{t}").count()
    print(f"  {t}: {c} records")
