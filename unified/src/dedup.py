"""
Deduplication and Out-of-Order Handling Module

This module provides watermark-based deduplication for streaming data,
particularly useful for Kafka sources where:
1. Same message may arrive with different offsets (producer retries)
2. Messages may arrive out-of-order across partitions
3. Late-arriving data needs to be handled gracefully

Usage:
    from dedup import apply_dedup
    
    df_deduped = apply_dedup(df, dedup_config)

Configuration Example:
    dedup_config = {
        "enabled": True,
        "watermark_column": "event_timestamp",
        "watermark_delay": "10 minutes",
        "dedup_keys": ["customer_id", "event_timestamp"],
        "offset_dedup": True,
        "offset_columns": ["kafka_partition", "kafka_offset"]
    }
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    from exceptions import (
        DedupError,
        InvalidDedupConfigError,
        WatermarkColumnNotFoundError,
        DedupColumnsNotFoundError,
    )
except ImportError:
    from .exceptions import (
        DedupError,
        InvalidDedupConfigError,
        WatermarkColumnNotFoundError,
        DedupColumnsNotFoundError,
    )


def apply_dedup(df: DataFrame, dedup_config: dict) -> DataFrame:
    """
    Apply watermark and deduplication based on configuration.
    
    This function handles two types of duplicates:
    1. Offset duplicates: Same Kafka message with different offsets (producer retries)
    2. Logical duplicates: Same business event arriving multiple times
    
    The order of operations is important:
    1. Watermark MUST be applied before dropDuplicates for streaming
    2. Offset dedup first (exact duplicates)
    3. Logical dedup second (business duplicates)
    
    Args:
        df: Input streaming DataFrame
        dedup_config: Configuration dictionary with the following structure:
            {
                "enabled": bool,           # Enable/disable dedup (default: False)
                "watermark_column": str,   # Column for watermark (default: "event_timestamp")
                "watermark_delay": str,    # Late arrival tolerance (default: "10 minutes")
                "dedup_keys": list,        # Logical dedup keys (default: [])
                "offset_dedup": bool,      # Enable Kafka offset dedup (default: False)
                "offset_columns": list     # Offset columns (default: ["kafka_partition", "kafka_offset"])
            }
    
    Returns:
        DataFrame with duplicates removed (or original if dedup disabled)
    
    Raises:
        InvalidDedupConfigError: If dedup_config structure is invalid
        WatermarkColumnNotFoundError: If watermark column doesn't exist in DataFrame
        DedupColumnsNotFoundError: If required dedup columns are missing
    
    Example:
        >>> dedup_config = {
        ...     "enabled": True,
        ...     "watermark_column": "event_timestamp",
        ...     "watermark_delay": "10 minutes",
        ...     "dedup_keys": ["customer_id", "event_timestamp"]
        ... }
        >>> df_clean = apply_dedup(df_raw, dedup_config)
    
    Note:
        - Watermark defines how long Spark waits for late data
        - Data arriving after watermark_delay is dropped (not processed)
        - For non-Kafka sources, set offset_dedup=False
        - dedup_keys should include business key + timestamp for best results
    """
    # Return unchanged if dedup is disabled or config is empty
    if not dedup_config or not dedup_config.get("enabled", False):
        return df
    
    # Validate config structure
    if not isinstance(dedup_config, dict):
        raise InvalidDedupConfigError(
            reason="dedup_config must be a dictionary",
            config=dedup_config
        )
    
    # Get DataFrame columns for validation
    df_columns = df.columns
    
    # Step 1: Apply watermark for out-of-order handling
    # Watermark tells Spark how long to wait for late data
    # This MUST be applied before dropDuplicates for streaming queries
    watermark_column = dedup_config.get("watermark_column", "event_timestamp")
    watermark_delay = dedup_config.get("watermark_delay", "10 minutes")
    
    # Validate watermark column exists
    if watermark_column not in df_columns:
        raise WatermarkColumnNotFoundError(
            column_name=watermark_column,
            available_columns=df_columns
        )
    
    # Validate watermark_delay format
    if not isinstance(watermark_delay, str) or not watermark_delay.strip():
        raise InvalidDedupConfigError(
            reason=f"watermark_delay must be a non-empty string (e.g., '10 minutes'), got: '{watermark_delay}'",
            config=dedup_config
        )
    
    df = df.withWatermark(watermark_column, watermark_delay)
    
    # Step 2: Offset-based deduplication (exact Kafka duplicates)
    # This handles producer retries where same message gets different offsets
    # Apply this FIRST to eliminate exact duplicates before logical dedup
    if dedup_config.get("offset_dedup", False):
        offset_columns = dedup_config.get(
            "offset_columns", 
            ["kafka_partition", "kafka_offset"]
        )
        
        # Validate offset columns exist
        existing_offset_cols = [c for c in offset_columns if c in df_columns]
        missing_offset_cols = [c for c in offset_columns if c not in df_columns]
        
        if missing_offset_cols and not existing_offset_cols:
            raise DedupColumnsNotFoundError(
                missing_columns=missing_offset_cols,
                available_columns=df_columns,
                dedup_type="Offset"
            )
        
        if existing_offset_cols:
            if missing_offset_cols:
                print(f"WARNING: Some offset columns not found: {missing_offset_cols}. Using: {existing_offset_cols}")
            df = df.dropDuplicates(existing_offset_cols)
    
    # Step 3: Logical deduplication (business key + timestamp)
    # This handles same event arriving from different sources/paths
    # or same event with different Kafka metadata
    dedup_keys = dedup_config.get("dedup_keys", [])
    if dedup_keys:
        # Validate dedup keys exist
        existing_keys = [k for k in dedup_keys if k in df_columns]
        missing_keys = [k for k in dedup_keys if k not in df_columns]
        
        if missing_keys and not existing_keys:
            raise DedupColumnsNotFoundError(
                missing_columns=missing_keys,
                available_columns=df_columns,
                dedup_type="Logical"
            )
        
        if existing_keys:
            if missing_keys:
                print(f"WARNING: Some dedup keys not found: {missing_keys}. Using: {existing_keys}")
            df = df.dropDuplicates(existing_keys)
    
    return df


def apply_watermark_only(df: DataFrame, watermark_config: dict) -> DataFrame:
    """
    Apply only watermark without deduplication.
    
    Use this when you need out-of-order handling but not deduplication.
    Useful for windowed aggregations downstream where you want to
    handle late data but preserve all records.
    
    Args:
        df: Input streaming DataFrame
        watermark_config: Configuration dictionary:
            {
                "enabled": bool,           # Enable/disable watermark
                "watermark_column": str,   # Column for watermark
                "watermark_delay": str     # Late arrival tolerance
            }
    
    Returns:
        DataFrame with watermark applied (or original if disabled)
    
    Raises:
        InvalidDedupConfigError: If watermark_config structure is invalid
        WatermarkColumnNotFoundError: If watermark column doesn't exist
    
    Example:
        >>> config = {
        ...     "enabled": True,
        ...     "watermark_column": "event_timestamp",
        ...     "watermark_delay": "30 minutes"
        ... }
        >>> df_with_watermark = apply_watermark_only(df, config)
    """
    if not watermark_config or not watermark_config.get("enabled", False):
        return df
    
    # Validate config structure
    if not isinstance(watermark_config, dict):
        raise InvalidDedupConfigError(
            reason="watermark_config must be a dictionary",
            config=watermark_config
        )
    
    watermark_column = watermark_config.get("watermark_column", "event_timestamp")
    watermark_delay = watermark_config.get("watermark_delay", "10 minutes")
    
    # Validate watermark column exists
    if watermark_column not in df.columns:
        raise WatermarkColumnNotFoundError(
            column_name=watermark_column,
            available_columns=df.columns
        )
    
    # Validate watermark_delay format
    if not isinstance(watermark_delay, str) or not watermark_delay.strip():
        raise InvalidDedupConfigError(
            reason=f"watermark_delay must be a non-empty string (e.g., '10 minutes'), got: '{watermark_delay}'",
            config=watermark_config
        )
    
    df = df.withWatermark(watermark_column, watermark_delay)
    
    return df


def get_dedup_stats(df: DataFrame, dedup_keys: list) -> DataFrame:
    """
    Get statistics about potential duplicates in a DataFrame.
    
    Useful for debugging and monitoring duplicate rates.
    Run this on a batch DataFrame (not streaming) to analyze
    duplicate patterns in your data.
    
    Args:
        df: Input DataFrame (batch mode for analysis)
        dedup_keys: Keys to check for duplicates
    
    Returns:
        DataFrame with duplicate counts per key combination,
        sorted by highest duplicate count first
    
    Example:
        >>> stats = get_dedup_stats(df, ["customer_id", "event_timestamp"])
        >>> stats.show()
        +------------+-------------------+---------------+
        |customer_id |event_timestamp    |duplicate_count|
        +------------+-------------------+---------------+
        |C001        |2024-01-01 10:00:00|5              |
        |C002        |2024-01-01 11:00:00|3              |
        +------------+-------------------+---------------+
    """
    existing_keys = [k for k in dedup_keys if k in df.columns]
    if not existing_keys:
        # Return empty DataFrame with expected schema
        return df.limit(0)
    
    return (
        df.groupBy(existing_keys)
        .agg(F.count("*").alias("duplicate_count"))
        .filter(F.col("duplicate_count") > 1)
        .orderBy(F.desc("duplicate_count"))
    )


def validate_dedup_config(dedup_config: dict) -> list:
    """
    Validate dedup configuration and return any warnings.
    
    Args:
        dedup_config: Dedup configuration dictionary
    
    Returns:
        List of warning messages (empty if config is valid)
    
    Example:
        >>> warnings = validate_dedup_config(config)
        >>> for w in warnings:
        ...     print(f"WARNING: {w}")
    """
    warnings = []
    
    if not dedup_config:
        return warnings
    
    if not dedup_config.get("enabled", False):
        return warnings
    
    # Check watermark configuration
    if not dedup_config.get("watermark_column"):
        warnings.append("watermark_column not specified, defaulting to 'event_timestamp'")
    
    if not dedup_config.get("watermark_delay"):
        warnings.append("watermark_delay not specified, defaulting to '10 minutes'")
    
    # Check dedup keys
    if not dedup_config.get("dedup_keys") and not dedup_config.get("offset_dedup"):
        warnings.append("Neither dedup_keys nor offset_dedup specified - dedup will have no effect")
    
    # Check offset dedup configuration
    if dedup_config.get("offset_dedup"):
        offset_cols = dedup_config.get("offset_columns", [])
        if not offset_cols:
            warnings.append("offset_dedup enabled but offset_columns not specified, using defaults")
    
    return warnings
