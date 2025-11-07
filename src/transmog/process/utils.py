"""Utility functions for the Transmog processor.

This module contains common utility functions used across the process module.
"""

import json
from typing import Any, NoReturn, TypeVar

from transmog.error import FileError, ParsingError, ProcessingError

# Type variable for the processor
P = TypeVar("P", bound=Any)


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
        raise
    elif isinstance(error, json.JSONDecodeError):
        raise ParsingError(f"Invalid JSON in {error_type} {file_path}: {str(error)}")
    else:
        raise FileError(f"Error reading {error_type} {file_path}: {str(error)}")
