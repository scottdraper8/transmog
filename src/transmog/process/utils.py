"""Utility functions for the Transmog processor.

This module contains common utility functions used across the process module.
"""

import json
from typing import Any, Optional, TypeVar, Union

from ..core.metadata import get_current_timestamp
from ..error import FileError, ParsingError, ProcessingError

# Type variable for the processor
P = TypeVar("P", bound=Any)


def get_common_config_params(
    processor: Any, extract_time: Optional[Any] = None
) -> dict[str, Any]:
    """Get common configuration parameters used across processing methods.

    Args:
        processor: Processor instance
        extract_time: Optional extraction timestamp override

    Returns:
        Dictionary of common configuration parameters
    """
    # Use current timestamp if not provided
    if extract_time is None:
        extract_time = get_current_timestamp()

    return {
        # Naming config
        "separator": processor.config.naming.separator,
        "deeply_nested_threshold": processor.config.naming.deeply_nested_threshold,
        # Processing config
        "cast_to_string": processor.config.processing.cast_to_string,
        "include_empty": processor.config.processing.include_empty,
        "skip_null": processor.config.processing.skip_null,
        "visit_arrays": processor.config.processing.visit_arrays,
        # Metadata config
        "id_field": processor.config.metadata.id_field,
        "parent_field": processor.config.metadata.parent_field,
        "time_field": processor.config.metadata.time_field,
        "default_id_field": processor.config.metadata.default_id_field,
        "id_generation_strategy": processor.config.metadata.id_generation_strategy,
        # Timestamps
        "extract_time": extract_time,
    }


def get_batch_size(
    config_or_processor: Any, chunk_size: Optional[Union[int, dict[str, Any]]] = None
) -> int:
    """Get the batch size, using provided value or config default.

    Args:
        config_or_processor: Processor or TransmogConfig instance
        chunk_size: Optional chunk size override or dict containing chunk_size key

    Returns:
        Batch size to use
    """
    # Handle chunk_size kwargs being a dict
    if isinstance(chunk_size, dict):
        chunk_size = chunk_size.get("chunk_size") or None

    # Extract batch size from either config or processor
    if hasattr(config_or_processor, "processing"):
        # It's a TransmogConfig object
        return chunk_size or config_or_processor.processing.batch_size
    elif hasattr(config_or_processor, "config"):
        # It's a Processor object
        return chunk_size or config_or_processor.config.processing.batch_size
    else:
        # Default to a reasonable batch size
        return chunk_size or 1000


def handle_file_error(
    file_path: str, error: Exception, error_type: str = "file"
) -> None:
    """Handle errors in file processing with consistent error messages.

    Args:
        file_path: Path to the file
        error: Exception that occurred
        error_type: Type of error for message customization

    Raises:
        FileError: If file cannot be read
        ParsingError: If file format is invalid
        ProcessingError: For other processing errors
    """
    if isinstance(error, (ProcessingError, FileError, ParsingError)):
        # Pass through known error types
        raise
    elif isinstance(error, json.JSONDecodeError):
        # JSON parsing errors
        raise ParsingError(f"Invalid JSON in {error_type} {file_path}: {str(error)}")
    else:
        # General file errors
        raise FileError(f"Error reading {error_type} {file_path}: {str(error)}")
