"""Utility functions for the Transmog processor.

This module contains common utility functions used across the process module.
"""

import json
from typing import Any, NoReturn, Optional, TypeVar

from ..config.utils import ConfigParameterBuilder
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
    # Use the unified parameter builder
    builder = ConfigParameterBuilder(processor.config)
    return builder.build_common_params(extract_time=extract_time)


def get_batch_size(processor: Any, override: Optional[int] = None) -> int:
    """Get batch size for processing.

    Args:
        processor: Processor instance
        override: Optional batch size override

    Returns:
        Batch size to use
    """
    builder = ConfigParameterBuilder(processor.config)
    return builder.get_batch_size(override)


def handle_file_error(
    file_path: str, error: Exception, error_type: str = "file"
) -> NoReturn:
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
