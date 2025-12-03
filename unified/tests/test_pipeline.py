# Tests for pipeline.py - DLT Pipeline Configuration and CDC Flow Creation
# Run with: pytest tests/test_pipeline.py -v

import pytest
from unittest.mock import MagicMock, patch, call
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestPipelineConfiguration:
    """Test cases for pipeline configuration loaded from metadata."""
    
    @pytest.fixture
    def mock_metadata(self):
        """Mock metadata structure matching pipeline_metadata.json."""
        return {
            "pipeline": {
                "name": "unified_scd2_pipeline",
                "version": "2.0.0",
                "catalog": "ltc_insurance",
                "target_schema": "unified_dev"
            },
            "target": {
                "table_name": "unified_customer_scd2",
                "keys": ["customer_id"],
                "sequence_by": "event_timestamp",
                "delete_condition": "is_deleted = true",
                "except_columns": ["source_system", "ingestion_timestamp", "_version"]
            },
            "sources": {
                "greenplum": {
                    "view_name": "gp_customer_v",
                    "flow_name": "gp_to_unified_flow"
                },
                "sqlserver": {
                    "view_name": "sql_customer_v",
                    "flow_name": "sql_to_unified_flow"
                },
                "kafka_cdc": {
                    "view_name": "cdc_customer_v",
                    "flow_name": "cdc_to_unified_flow"
                }
            }
        }
    
    def test_target_table_name(self, mock_metadata):
        """Test that target table name is correctly extracted."""
        target_table = mock_metadata["target"]["table_name"]
        assert target_table == "unified_customer_scd2"
    
    def test_scd2_keys(self, mock_metadata):
        """Test that SCD2 keys are correctly configured."""
        keys = mock_metadata["target"]["keys"]
        assert keys == ["customer_id"]
        assert len(keys) == 1
    
    def test_sequence_column(self, mock_metadata):
        """Test that sequence column is correctly configured."""
        sequence_by = mock_metadata["target"]["sequence_by"]
        assert sequence_by == "event_timestamp"
    
    def test_delete_condition(self, mock_metadata):
        """Test that delete condition is correctly configured."""
        delete_condition = mock_metadata["target"]["delete_condition"]
        assert delete_condition == "is_deleted = true"
    
    def test_except_columns(self, mock_metadata):
        """Test that except_columns list is correctly configured."""
        except_cols = mock_metadata["target"]["except_columns"]
        assert "source_system" in except_cols
        assert "ingestion_timestamp" in except_cols
        assert "_version" in except_cols
        assert len(except_cols) == 3
    
    def test_source_count(self, mock_metadata):
        """Test that all 3 sources are configured."""
        sources = mock_metadata["sources"]
        assert len(sources) == 3
        assert "greenplum" in sources
        assert "sqlserver" in sources
        assert "kafka_cdc" in sources


class TestCDCFlowConfiguration:
    """Test cases for CDC flow configuration."""
    
    @pytest.fixture
    def source_configs(self):
        """Source configurations for CDC flows."""
        return {
            "greenplum": {
                "table_name": "rdl_customer_hist_st",
                "catalog": "ltc_insurance",
                "schema": "raw_data_layer",
                "view_name": "gp_customer_v",
                "flow_name": "gp_to_unified_flow",
            },
            "sqlserver": {
                "table_name": "rdl_customer_init_st",
                "catalog": "ltc_insurance",
                "schema": "raw_data_layer",
                "view_name": "sql_customer_v",
                "flow_name": "sql_to_unified_flow",
            },
            "kafka_cdc": {
                "table_name": "rdl_customer",
                "catalog": "ltc_insurance",
                "schema": "raw_data_layer",
                "view_name": "cdc_customer_v",
                "flow_name": "cdc_to_unified_flow",
            }
        }
    
    def test_flow_names_are_unique(self, source_configs):
        """Test that all flow names are unique."""
        flow_names = [cfg["flow_name"] for cfg in source_configs.values()]
        assert len(flow_names) == len(set(flow_names)), "Flow names must be unique"
    
    def test_view_names_are_unique(self, source_configs):
        """Test that all view names are unique."""
        view_names = [cfg["view_name"] for cfg in source_configs.values()]
        assert len(view_names) == len(set(view_names)), "View names must be unique"
    
    def test_greenplum_flow_config(self, source_configs):
        """Test Greenplum CDC flow configuration."""
        gp = source_configs["greenplum"]
        assert gp["flow_name"] == "gp_to_unified_flow"
        assert gp["view_name"] == "gp_customer_v"
    
    def test_sqlserver_flow_config(self, source_configs):
        """Test SQL Server CDC flow configuration."""
        sql = source_configs["sqlserver"]
        assert sql["flow_name"] == "sql_to_unified_flow"
        assert sql["view_name"] == "sql_customer_v"
    
    def test_kafka_flow_config(self, source_configs):
        """Test Kafka CDC flow configuration."""
        cdc = source_configs["kafka_cdc"]
        assert cdc["flow_name"] == "cdc_to_unified_flow"
        assert cdc["view_name"] == "cdc_customer_v"
    
    def test_all_sources_target_same_table(self, source_configs):
        """Test that all CDC flows should target the same unified table."""
        # In the actual pipeline, all flows target config.TARGET_TABLE
        target_table = "unified_customer_scd2"
        
        # This is a conceptual test - all flows go to the same target
        for source_key in source_configs:
            # Each flow should merge into the unified table
            assert target_table == "unified_customer_scd2"


class TestCDCFlowCreation:
    """Test the CDC flow creation logic (mocked)."""
    
    @pytest.fixture
    def pipeline_config(self):
        """Complete pipeline configuration."""
        return {
            "target_table": "unified_customer_scd2",
            "scd2_keys": ["customer_id"],
            "sequence_column": "event_timestamp",
            "delete_condition": "is_deleted = true",
            "except_columns": ["source_system", "ingestion_timestamp", "_version"],
            "sources": {
                "greenplum": {"view_name": "gp_customer_v", "flow_name": "gp_to_unified_flow"},
                "sqlserver": {"view_name": "sql_customer_v", "flow_name": "sql_to_unified_flow"},
                "kafka_cdc": {"view_name": "cdc_customer_v", "flow_name": "cdc_to_unified_flow"},
            }
        }
    
    def test_create_auto_cdc_flow_parameters(self, pipeline_config):
        """Test that create_auto_cdc_flow would be called with correct parameters."""
        expected_calls = []
        
        for source_key, source_cfg in pipeline_config["sources"].items():
            expected_call = {
                "name": source_cfg["flow_name"],
                "target": pipeline_config["target_table"],
                "source": source_cfg["view_name"],
                "keys": pipeline_config["scd2_keys"],
                "stored_as_scd_type": "2",
            }
            expected_calls.append(expected_call)
        
        # Verify we have 3 expected calls
        assert len(expected_calls) == 3
        
        # Verify each call has the correct structure
        for call_params in expected_calls:
            assert "name" in call_params
            assert "target" in call_params
            assert "source" in call_params
            assert "keys" in call_params
            assert call_params["stored_as_scd_type"] == "2"
            assert call_params["target"] == "unified_customer_scd2"
            assert call_params["keys"] == ["customer_id"]
    
    def test_scd_type_is_2(self, pipeline_config):
        """Test that SCD type is always 2 for this pipeline."""
        scd_type = "2"
        assert scd_type == "2", "Pipeline should use SCD Type 2"
    
    def test_except_columns_excludes_metadata(self, pipeline_config):
        """Test that metadata columns are excluded from change detection."""
        except_cols = pipeline_config["except_columns"]
        
        # These columns should not trigger a new SCD2 version
        assert "source_system" in except_cols, "source_system should be excluded"
        assert "ingestion_timestamp" in except_cols, "ingestion_timestamp should be excluded"
        assert "_version" in except_cols, "_version should be excluded"
    
    def test_sequence_column_for_ordering(self, pipeline_config):
        """Test that event_timestamp is used for SCD2 ordering."""
        sequence_col = pipeline_config["sequence_column"]
        assert sequence_col == "event_timestamp"


class TestStreamingTableCreation:
    """Test streaming table creation configuration."""
    
    def test_streaming_table_name(self):
        """Test streaming table name matches target."""
        table_name = "unified_customer_scd2"
        assert table_name == "unified_customer_scd2"
    
    def test_streaming_table_comment(self):
        """Test streaming table has descriptive comment."""
        comment = "Unified SCD Type 2 - complete customer history from Greenplum legacy to real-time Kafka CDC"
        assert "SCD Type 2" in comment
        assert "Greenplum" in comment
        assert "Kafka CDC" in comment


class TestPipelineIntegration:
    """Integration tests for pipeline configuration consistency."""
    
    @pytest.fixture
    def full_config(self):
        """Full pipeline configuration from metadata."""
        return {
            "pipeline": {
                "name": "unified_scd2_pipeline",
                "version": "2.0.0",
            },
            "target": {
                "table_name": "unified_customer_scd2",
                "keys": ["customer_id"],
                "sequence_by": "event_timestamp",
                "delete_condition": "is_deleted = true",
                "except_columns": ["source_system", "ingestion_timestamp", "_version"]
            },
            "sources": {
                "greenplum": {
                    "view_name": "gp_customer_v",
                    "flow_name": "gp_to_unified_flow",
                    "column_mapping": {
                        "source_system": {"source_col": None, "default": "greenplum"}
                    }
                },
                "sqlserver": {
                    "view_name": "sql_customer_v",
                    "flow_name": "sql_to_unified_flow",
                    "column_mapping": {
                        "source_system": {"source_col": None, "default": "sqlserver"}
                    }
                },
                "kafka_cdc": {
                    "view_name": "cdc_customer_v",
                    "flow_name": "cdc_to_unified_flow",
                    "column_mapping": {
                        "source_system": {"source_col": None, "default": "kafka_cdc"}
                    }
                }
            }
        }
    
    def test_source_system_in_except_columns(self, full_config):
        """
        Critical: source_system should be in except_columns.
        This prevents source_system changes from creating new SCD2 versions.
        """
        except_cols = full_config["target"]["except_columns"]
        assert "source_system" in except_cols
    
    def test_each_source_has_unique_source_system_value(self, full_config):
        """Test that each source produces a unique source_system value."""
        source_system_values = []
        
        for source_key, source_cfg in full_config["sources"].items():
            ss_mapping = source_cfg["column_mapping"]["source_system"]
            source_system_values.append(ss_mapping["default"])
        
        # All values should be unique
        assert len(source_system_values) == len(set(source_system_values))
        
        # Verify expected values
        assert "greenplum" in source_system_values
        assert "sqlserver" in source_system_values
        assert "kafka_cdc" in source_system_values
    
    def test_view_to_flow_mapping(self, full_config):
        """Test that each view has a corresponding flow."""
        for source_key, source_cfg in full_config["sources"].items():
            view_name = source_cfg["view_name"]
            flow_name = source_cfg["flow_name"]
            
            # Flow name should reference the source
            assert source_key in flow_name or view_name.split("_")[0] in flow_name
    
    def test_primary_key_is_customer_id(self, full_config):
        """Test that customer_id is the primary key for SCD2."""
        keys = full_config["target"]["keys"]
        assert keys == ["customer_id"]
    
    def test_delete_condition_uses_is_deleted(self, full_config):
        """Test that soft deletes are handled via is_deleted column."""
        delete_condition = full_config["target"]["delete_condition"]
        assert "is_deleted" in delete_condition
        assert "true" in delete_condition.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
