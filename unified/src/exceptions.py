"""
Custom Exceptions Module

Provides specific exception types for the unified pipeline framework.
Using specific exceptions allows:
- Clear error messages that identify the root cause
- Proper error handling at different levels
- Easier debugging and troubleshooting
"""


class UnifiedPipelineError(Exception):
    """Base exception for all unified pipeline errors."""
    pass


# =============================================================================
# Metadata Errors
# =============================================================================

class MetadataError(UnifiedPipelineError):
    """Base exception for metadata-related errors."""
    pass


class MetadataFileNotFoundError(MetadataError):
    """Raised when metadata JSON file cannot be found."""
    
    def __init__(self, file_path: str, searched_paths: list = None):
        self.file_path = file_path
        self.searched_paths = searched_paths or []
        paths_msg = "\n  - ".join(self.searched_paths) if self.searched_paths else "N/A"
        message = (
            f"Metadata file not found: '{file_path}'\n"
            f"Searched paths:\n  - {paths_msg}\n"
            f"Ensure the pipeline.metadata_path Spark config is set correctly."
        )
        super().__init__(message)


class MetadataParseError(MetadataError):
    """Raised when metadata JSON file cannot be parsed."""
    
    def __init__(self, file_path: str, parse_error: str):
        self.file_path = file_path
        self.parse_error = parse_error
        message = (
            f"Failed to parse metadata file: '{file_path}'\n"
            f"Parse error: {parse_error}\n"
            f"Ensure the file contains valid JSON."
        )
        super().__init__(message)


class MetadataValidationError(MetadataError):
    """Raised when metadata fails validation."""
    
    def __init__(self, errors: list):
        self.errors = errors
        message = (
            f"Metadata validation failed with {len(errors)} error(s):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
        super().__init__(message)


class SourceNotFoundError(MetadataError):
    """Raised when a requested source is not defined in metadata."""
    
    def __init__(self, source_name: str, available_sources: list):
        self.source_name = source_name
        self.available_sources = available_sources
        message = (
            f"Source '{source_name}' not found in metadata.\n"
            f"Available sources: {available_sources}"
        )
        super().__init__(message)


class TargetNotFoundError(MetadataError):
    """Raised when a requested target index is out of range."""
    
    def __init__(self, target_index: int, num_targets: int):
        self.target_index = target_index
        self.num_targets = num_targets
        message = (
            f"Target index {target_index} out of range.\n"
            f"Available targets: 0-{num_targets - 1} ({num_targets} total)"
        )
        super().__init__(message)


class SourceMappingNotFoundError(MetadataError):
    """Raised when a source mapping is not defined for a target."""
    
    def __init__(self, source_name: str, target_index: int, available_mappings: list):
        self.source_name = source_name
        self.target_index = target_index
        self.available_mappings = available_mappings
        message = (
            f"No mapping for source '{source_name}' in target[{target_index}].\n"
            f"Available mappings: {available_mappings}"
        )
        super().__init__(message)


# =============================================================================
# Mapping Errors
# =============================================================================

class MappingError(UnifiedPipelineError):
    """Base exception for column mapping errors."""
    pass


class ColumnNotFoundError(MappingError):
    """Raised when a required source column is not found in DataFrame."""
    
    def __init__(self, column_name: str, source_name: str, available_columns: list):
        self.column_name = column_name
        self.source_name = source_name
        self.available_columns = available_columns
        message = (
            f"Source column '{column_name}' not found in source '{source_name}'.\n"
            f"Available columns: {available_columns}"
        )
        super().__init__(message)


class InvalidMappingConfigError(MappingError):
    """Raised when column mapping configuration is invalid."""
    
    def __init__(self, target_column: str, reason: str):
        self.target_column = target_column
        self.reason = reason
        message = (
            f"Invalid mapping configuration for target column '{target_column}'.\n"
            f"Reason: {reason}"
        )
        super().__init__(message)


# =============================================================================
# Transformation Errors
# =============================================================================

class TransformError(UnifiedPipelineError):
    """Base exception for transformation errors."""
    pass


class UnknownTransformError(TransformError):
    """Raised when an unknown transform type is specified."""
    
    def __init__(self, transform_name: str, available_transforms: list):
        self.transform_name = transform_name
        self.available_transforms = available_transforms
        message = (
            f"Unknown transform: '{transform_name}'.\n"
            f"Available transforms: {available_transforms}"
        )
        super().__init__(message)


class TransformExecutionError(TransformError):
    """Raised when a transform fails during execution."""
    
    def __init__(self, transform_name: str, column_name: str, error: str):
        self.transform_name = transform_name
        self.column_name = column_name
        self.error = error
        message = (
            f"Transform '{transform_name}' failed on column '{column_name}'.\n"
            f"Error: {error}"
        )
        super().__init__(message)


class SchemaNotFoundError(TransformError):
    """Raised when target schema is not defined in metadata."""
    
    def __init__(self, target_index: int):
        self.target_index = target_index
        message = (
            f"Target schema not found for target[{target_index}].\n"
            f"Ensure 'schema' is defined in the target configuration."
        )
        super().__init__(message)


class ColumnSchemaNotFoundError(TransformError):
    """Raised when a column is not defined in target schema."""
    
    def __init__(self, column_name: str, target_index: int, available_columns: list):
        self.column_name = column_name
        self.target_index = target_index
        self.available_columns = available_columns
        message = (
            f"Column '{column_name}' not found in target[{target_index}] schema.\n"
            f"Available schema columns: {available_columns}"
        )
        super().__init__(message)


# =============================================================================
# Deduplication Errors
# =============================================================================

class DedupError(UnifiedPipelineError):
    """Base exception for deduplication errors."""
    pass


class InvalidDedupConfigError(DedupError):
    """Raised when dedup configuration is invalid."""
    
    def __init__(self, reason: str, config: dict = None):
        self.reason = reason
        self.config = config
        message = f"Invalid dedup configuration.\nReason: {reason}"
        if config:
            message += f"\nConfig: {config}"
        super().__init__(message)


class WatermarkColumnNotFoundError(DedupError):
    """Raised when watermark column is not found in DataFrame."""
    
    def __init__(self, column_name: str, available_columns: list):
        self.column_name = column_name
        self.available_columns = available_columns
        message = (
            f"Watermark column '{column_name}' not found in DataFrame.\n"
            f"Available columns: {available_columns}\n"
            f"Ensure the column exists after mapping/transformation."
        )
        super().__init__(message)


class DedupColumnsNotFoundError(DedupError):
    """Raised when dedup key columns are not found in DataFrame."""
    
    def __init__(self, missing_columns: list, available_columns: list, dedup_type: str):
        self.missing_columns = missing_columns
        self.available_columns = available_columns
        self.dedup_type = dedup_type
        message = (
            f"{dedup_type} dedup columns not found: {missing_columns}\n"
            f"Available columns: {available_columns}"
        )
        super().__init__(message)


# =============================================================================
# View/Pipeline Errors
# =============================================================================

class ViewCreationError(UnifiedPipelineError):
    """Raised when a Lakeflow view cannot be created."""
    
    def __init__(self, view_name: str, source_name: str, error: str):
        self.view_name = view_name
        self.source_name = source_name
        self.error = error
        message = (
            f"Failed to create view '{view_name}' for source '{source_name}'.\n"
            f"Error: {error}"
        )
        super().__init__(message)


class SourceTableNotFoundError(UnifiedPipelineError):
    """Raised when a source table cannot be found."""
    
    def __init__(self, table_fqn: str, source_name: str):
        self.table_fqn = table_fqn
        self.source_name = source_name
        message = (
            f"Source table '{table_fqn}' not found for source '{source_name}'.\n"
            f"Ensure the table exists and the pipeline has access to it."
        )
        super().__init__(message)


class CDCFlowCreationError(UnifiedPipelineError):
    """Raised when a CDC flow cannot be created."""
    
    def __init__(self, flow_name: str, target_name: str, error: str):
        self.flow_name = flow_name
        self.target_name = target_name
        self.error = error
        message = (
            f"Failed to create CDC flow '{flow_name}' for target '{target_name}'.\n"
            f"Error: {error}"
        )
        super().__init__(message)


class StreamingTableCreationError(UnifiedPipelineError):
    """Raised when a streaming table cannot be created."""
    
    def __init__(self, table_name: str, error: str):
        self.table_name = table_name
        self.error = error
        message = (
            f"Failed to create streaming table '{table_name}'.\n"
            f"Error: {error}"
        )
        super().__init__(message)


# =============================================================================
# Configuration Errors
# =============================================================================

class SparkConfigError(UnifiedPipelineError):
    """Raised when required Spark configuration is missing."""
    
    def __init__(self, config_key: str, description: str = None):
        self.config_key = config_key
        self.description = description
        message = f"Missing required Spark configuration: '{config_key}'"
        if description:
            message += f"\nDescription: {description}"
        message += "\nEnsure the configuration is set in databricks.yml or pipeline YAML."
        super().__init__(message)
