# Databricks notebook source
# MAGIC %md
# MAGIC # Transformations Module
# MAGIC 
# MAGIC Contains the schema mapping transformation logic that normalizes
# MAGIC source DataFrames to the unified target schema.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./schema

# COMMAND ----------

def apply_schema_mapping(df: DataFrame, column_mapping: dict, source_name: str) -> DataFrame:
    """
    Apply column mapping and type conversions to normalize a source DataFrame.
    
    This function takes a source DataFrame with source-specific column names
    and data types, and transforms it to match the unified target schema.
    
    Args:
        df: Source DataFrame with source-specific schema
        column_mapping: Dictionary mapping target columns to source columns + transforms
                       Format: {"target_col": {"source_col": "src", "transform": "type", "default": val}}
        source_name: Name of source for logging/debugging
    
    Returns:
        DataFrame with unified schema:
        - Columns renamed to target names
        - Types converted to target types
        - Missing columns filled with defaults
        - Null handling applied
    
    Example:
        >>> df = spark.readStream.table("raw_data_layer.rdl_customer_hist_st")
        >>> normalized_df = apply_schema_mapping(df, GP_COLUMN_MAPPING, "greenplum")
    """
    select_exprs = []
    
    for target_col, mapping in column_mapping.items():
        source_col = mapping.get("source_col")
        transform = mapping.get("transform")
        default_val = mapping.get("default")
        target_type = TARGET_SCHEMA[target_col]["dtype"]
        
        # Build the column expression
        col_expr = _build_column_expression(source_col, transform, default_val, target_type)
        
        # Alias to target column name
        select_exprs.append(col_expr.alias(target_col))
    
    return df.select(*select_exprs)


def _build_column_expression(source_col: str, transform: str, default_val, target_type: str):
    """
    Build a column expression with the appropriate transformation.
    
    Args:
        source_col: Source column name (None if missing in source)
        transform: Transformation type to apply
        default_val: Default value if source_col is None
        target_type: Target data type for final cast
    
    Returns:
        Column expression ready to be selected
    """
    # Case 1: Source column is missing - use default value
    if source_col is None:
        if default_val is not None:
            return F.lit(default_val).cast(target_type)
        else:
            return F.lit(None).cast(target_type)
    
    # Case 2: Source column exists - apply transformation
    col_expr = F.col(source_col)
    
    if transform is None:
        # No transformation needed
        pass
    elif transform == "to_date":
        col_expr = _parse_date(col_expr)
    elif transform == "to_timestamp":
        col_expr = _parse_timestamp(col_expr)
    elif transform == "cast_string":
        col_expr = col_expr.cast("STRING")
    elif transform == "cast_int":
        col_expr = col_expr.cast("INT")
    elif transform == "cast_long":
        col_expr = col_expr.cast("LONG")
    elif transform == "cast_boolean":
        col_expr = _normalize_boolean(col_expr)
    else:
        raise ValueError(f"Unknown transform type: {transform}")
    
    # Apply final cast to target type
    return col_expr.cast(target_type)


def _parse_date(col_expr) -> F.Column:
    """
    Parse various date string formats to DATE type.
    
    Handles:
    - ISO format: yyyy-MM-dd
    - US format: MM/dd/yyyy
    - Legacy format: dd-MMM-yyyy
    - Native DATE type (passthrough)
    """
    return F.coalesce(
        F.to_date(col_expr, "yyyy-MM-dd"),
        F.to_date(col_expr, "MM/dd/yyyy"),
        F.to_date(col_expr, "dd-MMM-yyyy"),
        F.to_date(col_expr, "yyyyMMdd"),
        F.to_date(col_expr)  # Default parsing for native DATE
    )


def _parse_timestamp(col_expr) -> F.Column:
    """
    Parse various timestamp string formats to TIMESTAMP type.
    
    Handles:
    - ISO format: yyyy-MM-dd HH:mm:ss
    - ISO 8601: yyyy-MM-dd'T'HH:mm:ss
    - ISO 8601 with timezone: yyyy-MM-dd'T'HH:mm:ss.SSSZ
    - US format: MM/dd/yyyy HH:mm:ss
    - Native TIMESTAMP type (passthrough)
    """
    return F.coalesce(
        F.to_timestamp(col_expr, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        F.to_timestamp(col_expr, "MM/dd/yyyy HH:mm:ss"),
        F.to_timestamp(col_expr)  # Default parsing for native TIMESTAMP
    )


def _normalize_boolean(col_expr) -> F.Column:
    """
    Normalize various boolean representations to BOOLEAN type.
    
    Handles:
    - String: "true", "false", "True", "False", "TRUE", "FALSE"
    - Numeric: 1, 0
    - Char: "Y", "N", "y", "n"
    - Word: "yes", "no", "Yes", "No"
    - Native BOOLEAN (passthrough)
    """
    return (
        F.when(F.upper(col_expr.cast("STRING")).isin("TRUE", "1", "Y", "YES"), True)
        .when(F.upper(col_expr.cast("STRING")).isin("FALSE", "0", "N", "NO"), False)
        .otherwise(col_expr.cast("BOOLEAN"))
    )
