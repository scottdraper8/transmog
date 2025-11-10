"""IO module for Transmog.

This module provides I/O interfaces and utilities for reading and writing data.
"""

# Import format utilities
from transmog.io.formats import detect_format

# Import factory functions
from transmog.io.writer_factory import (
    create_streaming_writer,
    create_writer,
)

# Import writer interface implementation
from transmog.io.writer_interface import DataWriter, StreamingWriter

# Define public API
__all__ = [
    "detect_format",
    "create_writer",
    "create_streaming_writer",
    "DataWriter",
    "StreamingWriter",
]
