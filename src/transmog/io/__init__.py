"""
IO package for Transmog.

This package handles input and output operations for different formats.
"""

import logging
from importlib import import_module
from typing import Optional, Dict, List, Any, Union, BinaryIO

# Configure logging
logger = logging.getLogger(__name__)

# Import core interfaces
from transmog.io.writer_interface import DataWriter, StreamingWriter
from transmog.io.writer_factory import (
    create_writer,
    register_writer,
    create_streaming_writer,
    register_streaming_writer,
    get_supported_formats,
    get_supported_streaming_formats,
    is_format_available,
    is_streaming_format_available,
)
from transmog.io.formats import (
    FormatRegistry,
    detect_format,
    DependencyManager,
)


def initialize_io_features():
    """
    Initialize IO features by importing writer modules.

    This function attempts to import known writer modules to register
    their functionality.
    """
    # List of known formats to try importing
    known_formats = ["json", "csv", "parquet"]

    for fmt in known_formats:
        try:
            import_module(f"transmog.io.writers.{fmt}")
            logger.debug(f"Loaded {fmt} writer")
        except ImportError as e:
            # Log at debug level since this is expected for optional formats
            logger.debug(f"Could not load {fmt} writer: {e}")


# Define what to export
__all__ = [
    "DataWriter",
    "StreamingWriter",
    "create_writer",
    "register_writer",
    "create_streaming_writer",
    "register_streaming_writer",
    "get_supported_formats",
    "get_supported_streaming_formats",
    "is_format_available",
    "is_streaming_format_available",
    "FormatRegistry",
    "detect_format",
    "DependencyManager",
    "initialize_io_features",
]
