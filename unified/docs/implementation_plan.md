# Unified SCD Type 2 Pipeline - Implementation Plan

## Project Overview

Building a **Unification Pipeline (Stream 3)** using Databricks Lakeflow SDP to merge 3 source paths 
for the **same Customer entity** into a single SCD Type 2 streaming table.

---

## MVP Scope

| In Scope | Out of Scope |
|----------|--------------|
| Stream 3: Unification Layer | Stream 1: SQL Server → Confluent → Bronze Raw |
| Customer entity only | Stream 2: Bronze Raw → Flatten → Bronze Processed |
| 3 sources → 1 SCD2 target | Other entities (Policies, Claims) |
| Lakeflow SDP implementation | Data quality/validation rules |

---

## Architecture

### Data Flow

```
+----------------------------------+
|         SOURCE LAYER             |
|   (Already exists - upstream)    |
+----------------------------------+
           |
           v
+-------------------------------+     +-------------------------------+     +-------------------------------+
| rdl_customer_hist_st          |     | rdl_customer_init_st          |     | rdl_customer                  |
| (Greenplum Legacy History)    |     | (SQL Server Initial Snapshot) |     | (Kafka CDC Stream)            |
| Load: One-time                |     | Load: One-time                |     | Load: Continuous              |
| Data: Oldest (10+ years)      |     | Data: Baseline state          |     | Data: Real-time changes       |
+-------------------------------+     +-------------------------------+     +-------------------------------+
           |                                    |                                    |
           v                                    v                                    v
+-------------------------------+     +-------------------------------+     +-------------------------------+
| gp_customer_v                 |     | sql_customer_v                |     | cdc_customer_v                |
| (View - exclude legacy cols)  |     | (View - passthrough)          |     | (View - add source_system)    |
+-------------------------------+     +-------------------------------+     +-------------------------------+
           |                                    |                                    |
           +------------------------------------+------------------------------------+
                                               |
                                               v
                              +----------------------------------------+
                              |        3 x create_auto_cdc_flow()      |
                              |    (Multiple flows, same target)       |
                              +----------------------------------------+
                                               |
                                               v
                              +----------------------------------------+
                              |         unified_customer_scd2          |
                              |         (SCD Type 2 Target)            |
                              |                                        |
                              |  - Complete customer history           |
                              |  - From Greenplum legacy to real-time  |
                              |  - Auto-managed __START_AT, __END_AT   |
                              +----------------------------------------+
```

### Why Multiple CDC Flows (Not Union)?

Per Databricks documentation:
> "Use append flow processing instead of UNION allows you to update the target table incrementally 
> without running a full refresh."

| Aspect | Union Approach | Multiple CDC Flows |
|--------|---------------|-------------------|
| Refresh | Full refresh on any change | Incremental per source |
| Checkpoints | Single checkpoint | Independent per flow |
| Failure isolation | All or nothing | Source-specific |
| Scalability | Limited | Better |

---

## Implementation Phases

### Phase 1: Project Setup ✅ COMPLETE
- [x] Project structure created
- [x] GitHub repository linked
- [x] databricks.yml configured
- [x] Pipeline and job YAML resources
- [x] Source tables verified in catalog

### Phase 2: Step 1 - Imports & Setup ✅ COMPLETE
- [x] Import statements (`from pyspark import pipelines as dp`)
- [x] Configuration constants
- [x] Schema definitions

### Phase 3: Step 2 - Source Views ✅ COMPLETE
- [x] `gp_customer_v` - Greenplum history (exclude `valid_from`, `valid_to`, `is_current`)
- [x] `sql_customer_v` - SQL Server initial (passthrough)
- [x] `cdc_customer_v` - Kafka CDC (add `source_system` literal)

### Phase 4: Step 3 - Target Table ✅ COMPLETE
- [x] Create `unified_customer_scd2` streaming table

### Phase 5: Step 4 - CDC Flows ✅ COMPLETE
- [x] CDC flow: `gp_customer_v` → `unified_customer_scd2`
- [x] CDC flow: `sql_customer_v` → `unified_customer_scd2`
- [x] CDC flow: `cdc_customer_v` → `unified_customer_scd2`

### Phase 6: Testing & Validation ✅ COMPLETE
- [x] Deploy pipeline to dev environment
- [x] Verify all 3 CDC flows completed successfully
- [x] Target table created with correct SCD2 structure
- [x] __START_AT and __END_AT columns auto-managed

### Phase 7: Documentation & Cleanup ✅ COMPLETE
- [x] Update README with usage instructions
- [x] Update design docs with final implementation
- [x] Clean up unused placeholder files
- [x] Final review

---

## Source Table Details

### rdl_customer_hist_st (Greenplum)

| Column | Type | Action |
|--------|------|--------|
| customer_id | STRING | Keep (PK) |
| customer_name | STRING | Keep |
| date_of_birth | DATE | Keep |
| email | STRING | Keep |
| phone | STRING | Keep |
| state | STRING | Keep |
| zip_code | STRING | Keep |
| status | STRING | Keep |
| last_login | TIMESTAMP | Keep |
| session_count | INT | Keep |
| page_views | INT | Keep |
| is_deleted | BOOLEAN | Keep (delete detection) |
| event_timestamp | TIMESTAMP | Keep (sequence_by) |
| valid_from | TIMESTAMP | **EXCLUDE** (legacy SCD2) |
| valid_to | TIMESTAMP | **EXCLUDE** (legacy SCD2) |
| is_current | BOOLEAN | **EXCLUDE** (legacy SCD2) |
| ingestion_timestamp | TIMESTAMP | Keep (exclude from tracking) |
| source_system | STRING | Keep (exclude from tracking) |
| _version | BIGINT | Keep (exclude from tracking) |

### rdl_customer_init_st (SQL Server)

| Column | Type | Action |
|--------|------|--------|
| customer_id | STRING | Keep (PK) |
| customer_name | STRING | Keep |
| date_of_birth | DATE | Keep |
| email | STRING | Keep |
| phone | STRING | Keep |
| state | STRING | Keep |
| zip_code | STRING | Keep |
| status | STRING | Keep |
| last_login | TIMESTAMP | Keep |
| session_count | INT | Keep |
| page_views | INT | Keep |
| is_deleted | BOOLEAN | Keep (delete detection) |
| event_timestamp | TIMESTAMP | Keep (sequence_by) |
| ingestion_timestamp | TIMESTAMP | Keep (exclude from tracking) |
| source_system | STRING | Keep (exclude from tracking) |
| _version | BIGINT | Keep (exclude from tracking) |

### rdl_customer (Kafka CDC)

| Column | Type | Action |
|--------|------|--------|
| customer_id | STRING | Keep (PK) |
| customer_name | STRING | Keep |
| date_of_birth | DATE | Keep |
| email | STRING | Keep |
| phone | STRING | Keep |
| state | STRING | Keep |
| zip_code | STRING | Keep |
| status | STRING | Keep |
| last_login | TIMESTAMP | Keep |
| session_count | INT | Keep |
| page_views | INT | Keep |
| is_deleted | BOOLEAN | Keep (delete detection) |
| event_timestamp | TIMESTAMP | Keep (sequence_by) |
| ingestion_timestamp | TIMESTAMP | Keep (exclude from tracking) |
| source_system | STRING | **ADD** as `F.lit("kafka_cdc")` |
| _version | BIGINT | Keep (exclude from tracking) |

---

## CDC Flow Configuration

All 3 flows use the same configuration:

```python
{
    "keys": ["customer_id"],
    "sequence_by": F.col("event_timestamp"),
    "stored_as_scd_type": "2",
    "apply_as_deletes": F.expr("is_deleted = true"),
    "except_column_list": ["source_system", "ingestion_timestamp", "_version"]
}
```

---

## Timeline

| Phase | Description | Status | ETA |
|-------|-------------|--------|-----|
| 1 | Project Setup | ✅ Complete | Done |
| 2 | Step 1: Imports & Setup | ✅ Complete | Done |
| 3 | Step 2: Source Views | ✅ Complete | Done |
| 4 | Step 3: Target Table | ✅ Complete | Done |
| 5 | Step 4: CDC Flows | ✅ Complete | Done |
| 6 | Testing & Validation | ✅ Complete | Done |
| 7 | Documentation | ✅ Complete | Done |

---

## Dependencies

| Dependency | Status |
|------------|--------|
| Databricks workspace access | ✅ |
| Catalog: `ltc_insurance` | ✅ |
| Schema: `raw_data_layer` | ✅ |
| Table: `rdl_customer_hist_st` | ✅ |
| Table: `rdl_customer_init_st` | ✅ |
| Table: `rdl_customer` | ✅ |
| Target schema: `unified_dev` | ⬜ TBD |
| Serverless compute | ✅ Required |

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Timestamp ordering issues | High | Verify `event_timestamp` values across sources |
| Schema drift | Medium | Use explicit column selection in views |
| Duplicate customer_id across sources | Low | Same entity - expected behavior |
| CDC flow conflicts | Low | Databricks handles multi-flow targeting |
