"""
Column Mapper Module

Handles column renaming and default value assignment.
This is the first step in the data transformation pipeline:
  Source DataFrame → apply_mapping() → apply_transforms() → Target DataFrame

Responsibilities:
- Rename source columns to target column names
- Apply default values for missing source columns
- Preserve original data types (no casting here)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

try:
    import metadata_loader
    from exceptions import (
        MappingError,
        ColumnNotFoundError,
        InvalidMappingConfigError,
    )
except ImportError:
    from . import metadata_loader
    from .exceptions import (
        MappingError,
        ColumnNotFoundError,
        InvalidMappingConfigError,
    )

# COMMAND ----------


def apply_mapping(
    df: DataFrame, 
    column_mapping: dict, 
    source_name: str, 
    target_index: int = 0
) -> DataFrame:
    """
    Apply column mapping to rename source columns to target names and set defaults.
    
    This function performs ONLY mapping operations:
    - Renames source columns to target column names
    - Applies default values for columns missing in source
    - Does NOT apply type transformations (that's done in apply_transforms)
    
    Args:
        df: Source DataFrame with source-specific schema
        column_mapping: Dictionary mapping target columns to source columns
                       Format: {"target_col": {"source_col": "src", "transform": "type", "default": val}}
        source_name: Name of source for logging/debugging
        target_index: Target index for multi-target pipelines (default 0)
    
    Returns:
        DataFrame with:
        - Columns renamed to target names
        - Missing columns filled with defaults
        - Original data types preserved
    
    Raises:
        InvalidMappingConfigError: If column_mapping structure is invalid
        ColumnNotFoundError: If a required source column doesn't exist
    
    Example:
        >>> df = spark.readStream.table("raw_data_layer.rdl_customer_hist_st")
        >>> mapped_df = apply_mapping(df, column_mapping, "greenplum")
    """
    # Validate column_mapping structure
    if not column_mapping:
        raise InvalidMappingConfigError(
            target_column="(all)",
            reason="column_mapping is empty or None"
        )
    
    if not isinstance(column_mapping, dict):
        raise InvalidMappingConfigError(
            target_column="(all)",
            reason=f"column_mapping must be a dictionary, got: {type(column_mapping).__name__}"
        )
    
    # Get available columns for validation
    df_columns = df.columns
    select_exprs = []
    
    for target_col, mapping in column_mapping.items():
        # Validate mapping structure
        if not isinstance(mapping, dict):
            raise InvalidMappingConfigError(
                target_column=target_col,
                reason=f"mapping must be a dictionary, got: {type(mapping).__name__}"
            )
        
        source_col = mapping.get("source_col")
        default_val = mapping.get("default")
        
        # Validate source column exists if specified
        if source_col is not None and source_col not in df_columns:
            # Check if there's a default value as fallback
            if default_val is None:
                raise ColumnNotFoundError(
                    column_name=source_col,
                    source_name=source_name,
                    available_columns=df_columns
                )
            else:
                # Log warning but continue with default value
                print(f"WARNING: Source column '{source_col}' not found for target '{target_col}' "
                      f"in source '{source_name}'. Using default value: {default_val}")
                source_col = None  # Force use of default
        
        # Build the column expression (mapping only, no transforms)
        col_expr = _build_mapping_expression(source_col, default_val)
        
        # Alias to target column name
        select_exprs.append(col_expr.alias(target_col))
    
    if not select_exprs:
        raise InvalidMappingConfigError(
            target_column="(all)",
            reason="No valid column mappings could be created"
        )
    
    return df.select(*select_exprs)


def _build_mapping_expression(source_col: str, default_val) -> Column:
    """
    Build a column expression for mapping (rename + defaults only).
    
    Args:
        source_col: Source column name (None if missing in source)
        default_val: Default value if source_col is None
    
    Returns:
        Column expression with source column or default literal
    """
    # Case 1: Source column is missing - use default value
    if source_col is None:
        if default_val is not None:
            return F.lit(default_val)
        else:
            return F.lit(None)
    
    # Case 2: Source column exists - just reference it (no transform)
    return F.col(source_col)


def get_columns_requiring_transform(column_mapping: dict) -> dict:
    """
    Extract columns that have transforms specified.
    
    Used by apply_transforms to know which columns need transformation.
    
    Args:
        column_mapping: Full column mapping dictionary
    
    Returns:
        Dictionary of {target_col: transform_name} for columns with transforms
    """
    return {
        target_col: mapping.get("transform")
        for target_col, mapping in column_mapping.items()
        if mapping.get("transform") is not None
    }
