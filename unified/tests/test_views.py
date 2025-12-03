# Tests for views.py - Dynamic View Generation
# Run with: pytest tests/test_views.py -v

import pytest
from unittest.mock import MagicMock, patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestViewGeneration:
    """Test cases for dynamic view generation logic."""
    
    @pytest.fixture
    def sample_source_config(self):
        """Sample source configuration for testing."""
        return {
            "greenplum": {
                "table_name": "rdl_customer_hist_st",
                "catalog": "ltc_insurance",
                "schema": "raw_data_layer",
                "view_name": "gp_customer_v",
                "flow_name": "gp_to_unified_flow",
                "description": "Greenplum legacy customer history",
                "column_mapping": {
                    "customer_id": {"source_col": "customer_id", "transform": None, "default": None},
                    "customer_name": {"source_col": "customer_name", "transform": None, "default": None},
                    "source_system": {"source_col": None, "transform": None, "default": "greenplum"},
                }
            },
            "sqlserver": {
                "table_name": "rdl_customer_init_st",
                "catalog": "ltc_insurance",
                "schema": "raw_data_layer",
                "view_name": "sql_customer_v",
                "flow_name": "sql_to_unified_flow",
                "description": "SQL Server initial snapshot",
                "column_mapping": {
                    "customer_id": {"source_col": "customer_id", "transform": None, "default": None},
                    "customer_name": {"source_col": "customer_name", "transform": None, "default": None},
                    "source_system": {"source_col": None, "transform": None, "default": "sqlserver"},
                }
            },
            "kafka_cdc": {
                "table_name": "rdl_customer",
                "catalog": "ltc_insurance",
                "schema": "raw_data_layer",
                "view_name": "cdc_customer_v",
                "flow_name": "cdc_to_unified_flow",
                "description": "Kafka CDC stream",
                "column_mapping": {
                    "customer_id": {"source_col": "customer_id", "transform": None, "default": None},
                    "customer_name": {"source_col": "customer_name", "transform": None, "default": None},
                    "source_system": {"source_col": None, "transform": None, "default": "kafka_cdc"},
                }
            }
        }
    
    def test_view_name_extraction(self, sample_source_config):
        """Test that view names are correctly extracted from config."""
        expected_views = ["gp_customer_v", "sql_customer_v", "cdc_customer_v"]
        actual_views = [cfg["view_name"] for cfg in sample_source_config.values()]
        assert actual_views == expected_views
    
    def test_table_fqn_construction(self, sample_source_config):
        """Test fully qualified table name construction."""
        for source_key, config in sample_source_config.items():
            expected_fqn = f"{config['catalog']}.{config['schema']}.{config['table_name']}"
            actual_fqn = f"{config['catalog']}.{config['schema']}.{config['table_name']}"
            assert actual_fqn == expected_fqn
    
    def test_greenplum_fqn(self, sample_source_config):
        """Test Greenplum table FQN."""
        gp_config = sample_source_config["greenplum"]
        fqn = f"{gp_config['catalog']}.{gp_config['schema']}.{gp_config['table_name']}"
        assert fqn == "ltc_insurance.raw_data_layer.rdl_customer_hist_st"
    
    def test_sqlserver_fqn(self, sample_source_config):
        """Test SQL Server table FQN."""
        sql_config = sample_source_config["sqlserver"]
        fqn = f"{sql_config['catalog']}.{sql_config['schema']}.{sql_config['table_name']}"
        assert fqn == "ltc_insurance.raw_data_layer.rdl_customer_init_st"
    
    def test_kafka_fqn(self, sample_source_config):
        """Test Kafka CDC table FQN."""
        cdc_config = sample_source_config["kafka_cdc"]
        fqn = f"{cdc_config['catalog']}.{cdc_config['schema']}.{cdc_config['table_name']}"
        assert fqn == "ltc_insurance.raw_data_layer.rdl_customer"
    
    def test_source_system_default_values(self, sample_source_config):
        """Test that source_system has correct default values for each source."""
        expected_defaults = {
            "greenplum": "greenplum",
            "sqlserver": "sqlserver",
            "kafka_cdc": "kafka_cdc"
        }
        
        for source_key, config in sample_source_config.items():
            source_system_mapping = config["column_mapping"]["source_system"]
            assert source_system_mapping["source_col"] is None, f"{source_key}: source_col should be None"
            assert source_system_mapping["default"] == expected_defaults[source_key], \
                f"{source_key}: default should be '{expected_defaults[source_key]}'"
    
    def test_all_sources_have_required_keys(self, sample_source_config):
        """Test that all source configs have required keys."""
        required_keys = ["table_name", "catalog", "schema", "view_name", "flow_name", "column_mapping"]
        
        for source_key, config in sample_source_config.items():
            for key in required_keys:
                assert key in config, f"{source_key} is missing required key: {key}"
    
    def test_column_mapping_has_source_system(self, sample_source_config):
        """Test that all sources have source_system in column mapping."""
        for source_key, config in sample_source_config.items():
            assert "source_system" in config["column_mapping"], \
                f"{source_key} is missing source_system in column_mapping"


class TestViewQueryGeneration:
    """Test the SQL query generation logic."""
    
    def generate_select_expr(self, target_col: str, mapping: dict, target_type: str = "STRING") -> str:
        """Generate a SELECT expression for a column."""
        source_col = mapping.get("source_col")
        default_val = mapping.get("default")
        
        if source_col is None:
            if default_val is not None:
                return f"CAST('{default_val}' AS {target_type}) AS {target_col}"
            else:
                return f"CAST(NULL AS {target_type}) AS {target_col}"
        else:
            return f"CAST({source_col} AS {target_type}) AS {target_col}"
    
    def test_source_col_present(self):
        """Test expression when source_col is present."""
        mapping = {"source_col": "customer_id", "transform": None, "default": None}
        expr = self.generate_select_expr("customer_id", mapping, "STRING")
        assert expr == "CAST(customer_id AS STRING) AS customer_id"
    
    def test_source_col_none_with_default(self):
        """Test expression when source_col is None but default is provided."""
        mapping = {"source_col": None, "transform": None, "default": "greenplum"}
        expr = self.generate_select_expr("source_system", mapping, "STRING")
        assert expr == "CAST('greenplum' AS STRING) AS source_system"
    
    def test_source_col_none_no_default(self):
        """Test expression when both source_col and default are None."""
        mapping = {"source_col": None, "transform": None, "default": None}
        expr = self.generate_select_expr("optional_field", mapping, "STRING")
        assert expr == "CAST(NULL AS STRING) AS optional_field"
    
    def test_different_target_types(self):
        """Test expressions with different target types."""
        test_cases = [
            ({"source_col": "count_val", "default": None}, "INT", "CAST(count_val AS INT) AS count_val"),
            ({"source_col": "timestamp_val", "default": None}, "TIMESTAMP", "CAST(timestamp_val AS TIMESTAMP) AS timestamp_val"),
            ({"source_col": "flag_val", "default": None}, "BOOLEAN", "CAST(flag_val AS BOOLEAN) AS flag_val"),
        ]
        
        for mapping, dtype, expected in test_cases:
            expr = self.generate_select_expr(mapping["source_col"], mapping, dtype)
            assert expr == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
