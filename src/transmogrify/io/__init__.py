"""
IO module for Transmogrify.

This module provides utilities for reading and writing data in different formats.
"""

import logging

logger = logging.getLogger(__name__)

# Define available formats
SUPPORTED_FORMATS = ["json", "csv", "parquet"]

# Track which formats are actually available with current dependencies
_available_formats = {"json": True}  # JSON is always available
_available_readers = {"json": True}  # JSON reader is always available

# Try to import format-specific modules
try:
    import csv

    _available_formats["csv"] = True
    _available_readers["csv"] = True
except ImportError:
    _available_formats["csv"] = False
    _available_readers["csv"] = False

try:
    import pyarrow

    _available_formats["parquet"] = True
    _available_readers["csv"] = True  # PyArrow improves CSV reading
    PYARROW_AVAILABLE = True
except ImportError:
    _available_formats["parquet"] = False
    PYARROW_AVAILABLE = False
    # CSV reader will fall back to built-in if PyArrow not available

# Import the writer registry
from src.transmogrify.io.writer_registry import WriterRegistry

# Import reader functions for convenience
try:
    from src.transmogrify.io.json_reader import (
        read_json_file,
        read_jsonl_file,
        read_json_stream,
    )
except ImportError:
    logger.debug("Could not import JSON reader functions")

try:
    from src.transmogrify.io.csv_reader import (
        read_csv_file,
        read_csv_stream,
        CSVReader,
    )
except ImportError:
    logger.debug("Could not import CSV reader functions")


# Pre-register writer classes without importing them directly
# This prevents circular imports while ensuring writers are registered
def _register_default_writers():
    """Register default writers with the registry."""
    # Always register JSON writer (no external dependencies)
    WriterRegistry.register_format(
        "json", "src.transmogrify.io.json_writer", "JsonWriter"
    )

    # Register CSV writer if available
    if _available_formats.get("csv"):
        WriterRegistry.register_format(
            "csv", "src.transmogrify.io.csv_writer", "CsvWriter"
        )

    # Register Parquet writer if available
    if _available_formats.get("parquet"):
        WriterRegistry.register_format(
            "parquet", "src.transmogrify.io.parquet_writer", "ParquetWriter"
        )


# Run the registration
_register_default_writers()


# Functions to check writer availability
def is_writer_available(format_name: str) -> bool:
    """
    Check if a specific writer is available.

    Args:
        format_name: Format to check

    Returns:
        Whether the writer is available
    """
    return format_name in _available_formats and _available_formats[format_name]


def list_available_writers() -> list:
    """
    List all available writers.

    Returns:
        List of available writer format names
    """
    return [fmt for fmt, available in _available_formats.items() if available]


# Functions to check reader availability
def is_reader_available(format_name: str) -> bool:
    """
    Check if a specific reader is available.

    Args:
        format_name: Format to check

    Returns:
        Whether the reader is available
    """
    return format_name in _available_readers and _available_readers[format_name]


def list_available_readers() -> list:
    """
    List all available readers.

    Returns:
        List of available reader format names
    """
    return [fmt for fmt, available in _available_readers.items() if available]
