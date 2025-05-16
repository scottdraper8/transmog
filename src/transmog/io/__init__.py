"""IO module for Transmog.

This module provides I/O interfaces and utilities for reading and writing data.
"""

import logging

# Import dependency manager
from transmog.dependencies import DependencyManager

# Import format utilities
from transmog.io.formats import (
    FormatRegistry,
    detect_format,
)

# Import factory functions
from transmog.io.writer_factory import (
    create_streaming_writer,
    create_writer,
    get_supported_formats,
    get_supported_streaming_formats,
    is_format_available,
    is_streaming_format_available,
    register_streaming_writer,
    register_writer,
)

# Import writer interface implementation
from transmog.io.writer_interface import DataWriter, StreamingWriter

# Configure logging
logger = logging.getLogger(__name__)


def initialize_io_features() -> None:
    """Initialize IO features based on available dependencies.

    This function checks for optional dependencies and registers
    available formats and handlers.
    """
    # Nothing special to do for now, as the imports above
    # handle registration of basic formats
    pass


# Define public API
__all__ = [
    # Formats
    "FormatRegistry",
    "DependencyManager",
    "detect_format",
    # Factory functions
    "create_writer",
    "create_streaming_writer",
    "register_writer",
    "register_streaming_writer",
    "get_supported_formats",
    "get_supported_streaming_formats",
    "is_format_available",
    "is_streaming_format_available",
    # Writer interfaces
    "DataWriter",
    "StreamingWriter",
    # Feature initialization
    "initialize_io_features",
]
