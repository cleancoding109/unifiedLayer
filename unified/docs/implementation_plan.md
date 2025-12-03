# Unified SCD Type 2 Pipeline - Implementation Plan

## Project Overview

Building a **Unification Pipeline (Stream 3)** using Databricks Lakeflow SDP to merge 3 source paths 
for the **same Customer entity** into a single SCD Type 2 streaming table.

**Key Features:**
- Metadata-driven configuration (single source of truth)
- Modular architecture with separation of concerns
- Schema mapping with column renaming and type conversion
- Multi-flow CDC targeting same SCD2 table

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

### Phase 8: Modular Refactoring ✅ COMPLETE
- [x] Created `transformations.py` - Schema mapping transformation logic
- [x] Created `views.py` - Source view definitions
- [x] Refactored `pipeline.py` - Main orchestration only

### Phase 9: Metadata-Driven Configuration ✅ COMPLETE
- [x] Created `metadata_loader.py` - Loads and validates JSON metadata
- [x] Updated `pipeline_metadata.json` with full schema mappings
- [x] All modules now load configuration from metadata
- [x] Validation on load to catch missing required fields

### Phase 10: Project Structure Refactoring ✅ COMPLETE
- [x] Removed redundant `config.py` (thin wrapper around metadata_loader)
- [x] Removed redundant `schema.py` (only used for TARGET_SCHEMA lookup)
- [x] Centralized environment variables in `databricks.yml`
- [x] Removed hardcoded catalog/schema from `pipeline_metadata.json`
- [x] `metadata_loader.py` now injects catalog/schema from Spark config at runtime
- [x] Reorganized resources to nested structure: `stream/unified/customer/`
- [x] Reorganized metadata to nested structure: `stream/unified/customer/`
- [x] All 50 unit tests passing

---

## Module Architecture

```
src/
├── metadata/
│   └── stream/unified/customer/
│       └── pipeline_metadata.json   # Single source of truth
├── metadata_loader.py               # Loads & validates metadata, injects runtime config
├── transformations.py               # Type conversion logic
├── views.py                         # Source view definitions
└── pipeline.py                      # Main orchestration

resources/
├── stream/unified/customer/
│   ├── unified.pipeline.yml         # Pipeline resource definition
│   └── unified.job.yml              # Job resource definition
└── test/
    └── *.job.yml                    # Test job definitions
```

### Module Dependencies

```
databricks.yml (variables per environment)
         │
         ▼
unified.pipeline.yml (passes to Spark config)
         │
         ▼
pipeline_metadata.json
         │
         ▼
  metadata_loader.py (injects catalog/schema from Spark config)
         │
         ▼
     views.py ──► transformations.py
         │
         ▼
    pipeline.py
```

### Environment Configuration

Variables are defined per environment in `databricks.yml`:

```yaml
targets:
  dev:
    variables:
      catalog: ltc_insurance
      schema: unified_dev
      source_catalog: ltc_insurance
      source_schema: raw_data_layer
  prod:
    variables:
      catalog: ltc_insurance
      schema: unified_prod
      source_catalog: ltc_insurance
      source_schema: raw_data_layer
```

These are passed to the pipeline via Spark config and injected into metadata at runtime.

---

## Source Table Details

### rdl_customer_hist_st (Greenplum)

**Real-world schema differences:** Uses abbreviated column names.

| Source Column | Target Column | Transform |
|---------------|---------------|----------|
| cust_id | customer_id | - |
| cust_name | customer_name | - |
| dob | date_of_birth | to_date |
| email_addr | email | - |
| phone_num | phone | - |
| state_cd | state | - |
| zip | zip_code | - |
| cust_status | status | - |
| last_login_ts | last_login | to_timestamp |
| sess_cnt | session_count | cast_int |
| pg_views | page_views | cast_int |
| is_deleted | is_deleted | cast_boolean |
| event_ts | event_timestamp | to_timestamp |
| ingest_ts | ingestion_timestamp | to_timestamp |
| src_sys | source_system | - |
| version_num | _version | cast_long |
| valid_from | - | **EXCLUDE** |
| valid_to | - | **EXCLUDE** |
| is_current | - | **EXCLUDE** |

### rdl_customer_init_st (SQL Server)

**Real-world schema differences:** Uses PascalCase column names.

| Source Column | Target Column | Transform |
|---------------|---------------|----------|
| CustomerID | customer_id | cast_string |
| CustomerName | customer_name | - |
| DateOfBirth | date_of_birth | to_date |
| EmailAddress | email | - |
| PhoneNumber | phone | - |
| StateCode | state | - |
| ZipCode | zip_code | - |
| Status | status | - |
| LastLoginDate | last_login | to_timestamp |
| SessionCount | session_count | cast_int |
| PageViews | page_views | cast_int |
| IsDeleted | is_deleted | cast_boolean |
| ModifiedDate | event_timestamp | to_timestamp |
| IngestionTime | ingestion_timestamp | to_timestamp |
| SourceSystem | source_system | - |
| RowVersion | _version | cast_long |

### rdl_customer (Kafka CDC)

**Real-world schema differences:** Uses snake_case, missing source_system.

| Source Column | Target Column | Transform |
|---------------|---------------|----------|
| customer_id | customer_id | - |
| customer_name | customer_name | - |
| date_of_birth | date_of_birth | - |
| email | email | - |
| phone | phone | - |
| state | state | - |
| zip_code | zip_code | - |
| status | status | - |
| last_login | last_login | - |
| session_count | session_count | - |
| page_views | page_views | - |
| is_deleted | is_deleted | - |
| event_timestamp | event_timestamp | - |
| ingestion_timestamp | ingestion_timestamp | - |
| *(missing)* | source_system | **DEFAULT: "kafka_cdc"** |
| _version | _version | - |

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
| 8 | Modular Refactoring | ✅ Complete | Done |
| 9 | Metadata-Driven Config | ✅ Complete | Done |
| 10 | Project Structure Refactoring | ✅ Complete | Done |

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
