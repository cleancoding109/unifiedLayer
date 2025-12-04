"""
Transformations Module

Handles data type transformations and conversions.
This is the second step in the data transformation pipeline:
  Source DataFrame → apply_mapping() → apply_transforms() → Target DataFrame

Responsibilities:
- Apply type conversions (string → date, epoch → timestamp, etc.)
- Cast columns to target data types
- Handle complex parsing (multi-format dates, boolean normalization)

Uses a registry pattern for extensible transform functions.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

try:
    import metadata_loader
except ImportError:
    from . import metadata_loader

# COMMAND ----------

# =============================================================================
# TRANSFORM REGISTRY
# =============================================================================
# Registry pattern allows easy addition of new transforms without modifying
# core logic. Each transform function takes a Column and returns a Column.

def _parse_date(col_expr: Column) -> Column:
    """
    Parse various date string formats to DATE type.
    
    Handles:
    - ISO format: yyyy-MM-dd
    - US format: MM/dd/yyyy
    - Legacy format: dd-MMM-yyyy
    - Compact: yyyyMMdd
    - Native DATE type (passthrough)
    """
    return F.coalesce(
        F.to_date(col_expr, "yyyy-MM-dd"),
        F.to_date(col_expr, "MM/dd/yyyy"),
        F.to_date(col_expr, "dd-MMM-yyyy"),
        F.to_date(col_expr, "yyyyMMdd"),
        F.to_date(col_expr)  # Default parsing for native DATE
    )


def _parse_timestamp(col_expr: Column) -> Column:
    """
    Parse various timestamp string formats to TIMESTAMP type.
    
    Handles:
    - ISO format: yyyy-MM-dd HH:mm:ss
    - ISO 8601: yyyy-MM-dd'T'HH:mm:ss
    - ISO 8601 with millis: yyyy-MM-dd'T'HH:mm:ss.SSS
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


def _epoch_to_timestamp_ms(col_expr: Column) -> Column:
    """
    Convert epoch milliseconds to TIMESTAMP type.
    
    Common for Kafka timestamps, JavaScript Date.now(), etc.
    Divides by 1000 to convert ms → seconds before casting.
    """
    return (col_expr / 1000).cast("TIMESTAMP")


def _epoch_to_timestamp_s(col_expr: Column) -> Column:
    """
    Convert epoch seconds to TIMESTAMP type.
    
    Common for Unix timestamps, Python time.time(), etc.
    Direct cast without division.
    """
    return col_expr.cast("TIMESTAMP")


def _normalize_boolean(col_expr: Column) -> Column:
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


# Transform registry - maps transform names to functions
TRANSFORM_REGISTRY = {
    "to_date": _parse_date,
    "to_timestamp": _parse_timestamp,
    "epoch_to_timestamp": _epoch_to_timestamp_ms,      # Default: milliseconds
    "epoch_to_timestamp_ms": _epoch_to_timestamp_ms,   # Explicit: milliseconds
    "epoch_to_timestamp_s": _epoch_to_timestamp_s,     # Explicit: seconds
    "cast_string": lambda c: c.cast("STRING"),
    "cast_int": lambda c: c.cast("INT"),
    "cast_long": lambda c: c.cast("LONG"),
    "cast_boolean": _normalize_boolean,
}


# COMMAND ----------

# =============================================================================
# MAIN TRANSFORM FUNCTION
# =============================================================================

def apply_transforms(
    df: DataFrame, 
    column_mapping: dict, 
    target_index: int = 0
) -> DataFrame:
    """
    Apply type transformations to a mapped DataFrame.
    
    This function performs ONLY transformation operations:
    - Applies registered transforms (to_date, to_timestamp, epoch_to_timestamp, etc.)
    - Casts all columns to their target data types
    - Assumes columns are already renamed to target names (by apply_mapping)
    
    Args:
        df: DataFrame with target column names (output of apply_mapping)
        column_mapping: Dictionary with transform specifications
                       Format: {"target_col": {"source_col": "src", "transform": "type", "default": val}}
        target_index: Target index for multi-target pipelines (default 0)
    
    Returns:
        DataFrame with all columns transformed and cast to target types
    
    Example:
        >>> mapped_df = mapper.apply_mapping(df, column_mapping, "greenplum")
        >>> transformed_df = apply_transforms(mapped_df, column_mapping, 0)
    """
    select_exprs = []
    
    target_schema = metadata_loader.get_target_schema(target_index)
    
    for target_col, mapping in column_mapping.items():
        transform = mapping.get("transform")
        target_type = target_schema[target_col]["dtype"]
        
        # Start with the column (already renamed by apply_mapping)
        col_expr = F.col(target_col)
        
        # Apply transform if specified
        if transform is not None:
            col_expr = apply_transform(col_expr, transform)
        
        # Cast to target type
        col_expr = col_expr.cast(target_type)
        
        # Alias to preserve column name
        select_exprs.append(col_expr.alias(target_col))
    
    return df.select(*select_exprs)


def apply_transform(col_expr: Column, transform_name: str) -> Column:
    """
    Apply a single registered transform to a column expression.
    
    Args:
        col_expr: Input column expression
        transform_name: Name of the transform (must be in TRANSFORM_REGISTRY)
    
    Returns:
        Transformed column expression
    
    Raises:
        ValueError: If transform_name is not found in registry
    """
    if transform_name not in TRANSFORM_REGISTRY:
        raise ValueError(
            f"Unknown transform: '{transform_name}'. "
            f"Available transforms: {list(TRANSFORM_REGISTRY.keys())}"
        )
    
    return TRANSFORM_REGISTRY[transform_name](col_expr)


# COMMAND ----------

# =============================================================================
# LEGACY COMPATIBILITY
# =============================================================================
# Keep apply_schema_mapping for backward compatibility during transition

def apply_schema_mapping(
    df: DataFrame, 
    column_mapping: dict, 
    source_name: str, 
    target_index: int = 0
) -> DataFrame:
    """
    [DEPRECATED] Apply column mapping and type conversions in a single pass.
    
    This function is maintained for backward compatibility.
    New code should use: mapper.apply_mapping() → apply_transforms()
    
    Args:
        df: Source DataFrame with source-specific schema
        column_mapping: Dictionary mapping target columns to source columns + transforms
        source_name: Name of source for logging/debugging
        target_index: Target index for multi-target pipelines (default 0)
    
    Returns:
        DataFrame with unified schema
    """
    # Import mapper here to avoid circular imports
    try:
        import mapper
    except ImportError:
        from . import mapper
    
    # Sequential execution: mapping → transforms
    df_mapped = mapper.apply_mapping(df, column_mapping, source_name, target_index)
    df_transformed = apply_transforms(df_mapped, column_mapping, target_index)
    
    return df_transformed
