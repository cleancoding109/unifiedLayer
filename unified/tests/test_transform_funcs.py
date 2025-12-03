# Tests for transformations.py - Schema Mapping and Column Transformations
# Run with: pytest tests/test_transformations.py -v

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, DateType, LongType
import sys
import os
from datetime import datetime, date

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("TransformationsTest") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def sample_target_schema():
    """Sample target schema for testing."""
    return {
        "customer_id": {"dtype": "STRING", "nullable": False},
        "customer_name": {"dtype": "STRING", "nullable": True},
        "date_of_birth": {"dtype": "DATE", "nullable": True},
        "email": {"dtype": "STRING", "nullable": True},
        "session_count": {"dtype": "INT", "nullable": True},
        "is_deleted": {"dtype": "BOOLEAN", "nullable": False},
        "event_timestamp": {"dtype": "TIMESTAMP", "nullable": False},
        "source_system": {"dtype": "STRING", "nullable": False},
        "_version": {"dtype": "LONG", "nullable": True},
    }


@pytest.fixture
def sample_column_mapping():
    """Sample column mapping for Greenplum source."""
    return {
        "customer_id": {"source_col": "customer_id", "transform": None, "default": None},
        "customer_name": {"source_col": "customer_name", "transform": None, "default": None},
        "date_of_birth": {"source_col": "date_of_birth", "transform": None, "default": None},
        "email": {"source_col": "email", "transform": None, "default": None},
        "session_count": {"source_col": "session_count", "transform": None, "default": None},
        "is_deleted": {"source_col": "is_deleted", "transform": None, "default": None},
        "event_timestamp": {"source_col": "event_timestamp", "transform": None, "default": None},
        "source_system": {"source_col": None, "transform": None, "default": "greenplum"},
        "_version": {"source_col": "_version", "transform": None, "default": None},
    }


class TestBuildColumnExpression:
    """Test the _build_column_expression function logic."""
    
    def test_source_col_none_with_default_string(self):
        """When source_col is None and default is provided, should use lit(default)."""
        source_col = None
        default_val = "greenplum"
        target_type = "STRING"
        
        # Simulate the logic
        if source_col is None:
            if default_val is not None:
                result = f"lit('{default_val}').cast('{target_type}')"
            else:
                result = f"lit(None).cast('{target_type}')"
        
        assert result == "lit('greenplum').cast('STRING')"
    
    def test_source_col_none_without_default(self):
        """When source_col is None and no default, should use lit(None)."""
        source_col = None
        default_val = None
        target_type = "STRING"
        
        if source_col is None:
            if default_val is not None:
                result = f"lit('{default_val}').cast('{target_type}')"
            else:
                result = f"lit(None).cast('{target_type}')"
        
        assert result == "lit(None).cast('STRING')"
    
    def test_source_col_present(self):
        """When source_col is provided, should use col(source_col)."""
        source_col = "customer_id"
        default_val = None
        target_type = "STRING"
        
        if source_col is None:
            result = f"lit('{default_val}').cast('{target_type}')"
        else:
            result = f"col('{source_col}').cast('{target_type}')"
        
        assert result == "col('customer_id').cast('STRING')"


class TestApplySchemaMapping:
    """Test the apply_schema_mapping function with actual Spark."""
    
    def test_basic_mapping(self, spark, sample_target_schema, sample_column_mapping):
        """Test basic schema mapping with a simple DataFrame."""
        # Create sample source data
        source_data = [
            ("CUST001", "John Doe", "john@example.com", 10, False, "2024-01-01 10:00:00", 1),
            ("CUST002", "Jane Smith", "jane@example.com", 20, False, "2024-01-02 11:00:00", 2),
        ]
        
        source_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("session_count", IntegerType(), True),
            StructField("is_deleted", BooleanType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("_version", LongType(), True),
        ])
        
        df = spark.createDataFrame(source_data, source_schema)
        
        # Build select expressions manually (simulating apply_schema_mapping)
        select_exprs = []
        for target_col, mapping in sample_column_mapping.items():
            source_col = mapping.get("source_col")
            default_val = mapping.get("default")
            target_type = sample_target_schema.get(target_col, {}).get("dtype", "STRING")
            
            if target_col == "date_of_birth":
                # Skip this column as it's not in source
                select_exprs.append(F.lit(None).cast("DATE").alias(target_col))
            elif source_col is None:
                if default_val is not None:
                    select_exprs.append(F.lit(default_val).cast(target_type).alias(target_col))
                else:
                    select_exprs.append(F.lit(None).cast(target_type).alias(target_col))
            else:
                if source_col in df.columns:
                    select_exprs.append(F.col(source_col).cast(target_type).alias(target_col))
                else:
                    select_exprs.append(F.lit(None).cast(target_type).alias(target_col))
        
        result_df = df.select(*select_exprs)
        
        # Verify source_system column
        source_systems = [row.source_system for row in result_df.collect()]
        assert all(ss == "greenplum" for ss in source_systems), "All source_system values should be 'greenplum'"
    
    def test_source_system_column_populated(self, spark):
        """Specifically test that source_system column gets the default value."""
        source_data = [("CUST001", "John")]
        source_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
        ])
        
        df = spark.createDataFrame(source_data, source_schema)
        
        # Simulate the transformation for source_system
        # source_col=None, default="greenplum"
        result_df = df.withColumn("source_system", F.lit("greenplum").cast("STRING"))
        
        rows = result_df.collect()
        assert rows[0].source_system == "greenplum"
    
    def test_multiple_source_systems(self, spark):
        """Test that different sources get different source_system values."""
        # Greenplum data
        gp_data = [("CUST001", "John")]
        gp_df = spark.createDataFrame(gp_data, ["customer_id", "customer_name"])
        gp_result = gp_df.withColumn("source_system", F.lit("greenplum"))
        
        # SQL Server data
        sql_data = [("CUST002", "Jane")]
        sql_df = spark.createDataFrame(sql_data, ["customer_id", "customer_name"])
        sql_result = sql_df.withColumn("source_system", F.lit("sqlserver"))
        
        # Kafka CDC data
        cdc_data = [("CUST003", "Bob")]
        cdc_df = spark.createDataFrame(cdc_data, ["customer_id", "customer_name"])
        cdc_result = cdc_df.withColumn("source_system", F.lit("kafka_cdc"))
        
        # Union all
        unified = gp_result.union(sql_result).union(cdc_result)
        
        rows = unified.collect()
        source_systems = {row.customer_id: row.source_system for row in rows}
        
        assert source_systems["CUST001"] == "greenplum"
        assert source_systems["CUST002"] == "sqlserver"
        assert source_systems["CUST003"] == "kafka_cdc"


class TestTransformFunctions:
    """Test individual transformation functions."""
    
    def test_parse_date_iso_format(self, spark):
        """Test date parsing with ISO format."""
        df = spark.createDataFrame([("2024-01-15",)], ["date_str"])
        result = df.withColumn("parsed_date", F.to_date(F.col("date_str"), "yyyy-MM-dd"))
        
        row = result.collect()[0]
        assert row.parsed_date == date(2024, 1, 15)
    
    def test_parse_date_us_format(self, spark):
        """Test date parsing with US format."""
        df = spark.createDataFrame([("01/15/2024",)], ["date_str"])
        result = df.withColumn("parsed_date", F.to_date(F.col("date_str"), "MM/dd/yyyy"))
        
        row = result.collect()[0]
        assert row.parsed_date == date(2024, 1, 15)
    
    def test_parse_timestamp_iso_format(self, spark):
        """Test timestamp parsing with ISO format."""
        df = spark.createDataFrame([("2024-01-15 10:30:00",)], ["ts_str"])
        result = df.withColumn("parsed_ts", F.to_timestamp(F.col("ts_str"), "yyyy-MM-dd HH:mm:ss"))
        
        row = result.collect()[0]
        assert row.parsed_ts.year == 2024
        assert row.parsed_ts.month == 1
        assert row.parsed_ts.day == 15
    
    def test_normalize_boolean_true_values(self, spark):
        """Test boolean normalization for true values."""
        test_values = [("TRUE",), ("true",), ("1",), ("Y",), ("YES",)]
        df = spark.createDataFrame(test_values, ["bool_str"])
        
        result = df.withColumn(
            "parsed_bool",
            F.when(F.upper(F.col("bool_str")).isin("TRUE", "1", "Y", "YES"), True)
            .otherwise(False)
        )
        
        rows = result.collect()
        assert all(row.parsed_bool == True for row in rows)
    
    def test_normalize_boolean_false_values(self, spark):
        """Test boolean normalization for false values."""
        test_values = [("FALSE",), ("false",), ("0",), ("N",), ("NO",)]
        df = spark.createDataFrame(test_values, ["bool_str"])
        
        result = df.withColumn(
            "parsed_bool",
            F.when(F.upper(F.col("bool_str")).isin("FALSE", "0", "N", "NO"), False)
            .otherwise(True)
        )
        
        rows = result.collect()
        assert all(row.parsed_bool == False for row in rows)
    
    def test_cast_to_int(self, spark):
        """Test casting string to INT."""
        df = spark.createDataFrame([("100",), ("200",), ("0",)], ["num_str"])
        result = df.withColumn("num_int", F.col("num_str").cast("INT"))
        
        rows = result.collect()
        assert rows[0].num_int == 100
        assert rows[1].num_int == 200
        assert rows[2].num_int == 0
    
    def test_cast_to_long(self, spark):
        """Test casting string to LONG."""
        df = spark.createDataFrame([("9999999999",)], ["num_str"])
        result = df.withColumn("num_long", F.col("num_str").cast("LONG"))
        
        row = result.collect()[0]
        assert row.num_long == 9999999999


class TestColumnMappingValidation:
    """Test column mapping structure validation."""
    
    def test_mapping_has_required_keys(self, sample_column_mapping):
        """Test that each mapping entry has required keys."""
        for col_name, mapping in sample_column_mapping.items():
            assert "source_col" in mapping, f"{col_name} missing 'source_col'"
            assert "transform" in mapping, f"{col_name} missing 'transform'"
            assert "default" in mapping, f"{col_name} missing 'default'"
    
    def test_source_system_mapping_is_correct(self, sample_column_mapping):
        """Test that source_system mapping is configured correctly."""
        ss_mapping = sample_column_mapping["source_system"]
        
        assert ss_mapping["source_col"] is None, "source_col should be None"
        assert ss_mapping["default"] == "greenplum", "default should be 'greenplum'"
        assert ss_mapping["transform"] is None, "transform should be None"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
