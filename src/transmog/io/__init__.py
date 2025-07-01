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
    # Check if writers are already registered to avoid duplicate registration
    if is_format_available("json"):
        return

    # Manually register writers to avoid circular import issues
    try:
        # Import writer modules first
        from . import writers  # noqa: F401

        # Now register them manually
        from .writers.json import JsonStreamingWriter, JsonWriter

        register_writer(JsonWriter.format_name(), JsonWriter)
        register_streaming_writer(
            JsonStreamingWriter.format_name(), JsonStreamingWriter
        )
        logger.debug("Registered JSON writers")

        from .writers.csv import CsvStreamingWriter, CsvWriter

        register_writer(CsvWriter.format_name(), CsvWriter)
        register_streaming_writer(CsvStreamingWriter.format_name(), CsvStreamingWriter)
        logger.debug("Registered CSV writers")

        try:
            from .writers.parquet import ParquetStreamingWriter, ParquetWriter

            register_writer(ParquetWriter.format_name(), ParquetWriter)
            register_streaming_writer(
                ParquetStreamingWriter.format_name(), ParquetStreamingWriter
            )
            logger.debug("Registered Parquet writers")
        except ImportError:
            logger.debug("Parquet writers not available")

    except ImportError as e:
        logger.debug(f"Failed to register writers: {e}")
        # Fallback: Force manual registration if imports fail
        pass


# Define public API
__all__ = [
    # Format detection
    "detect_format",
    # Factory functions for common usage
    "create_writer",
    "create_streaming_writer",
    "get_supported_formats",
    "get_supported_streaming_formats",
    "is_format_available",
    "is_streaming_format_available",
    # Writer interfaces for custom implementations
    "DataWriter",
    "StreamingWriter",
]


# Advanced API - for power users who need internal access
# Import these explicitly: from transmog.io.advanced import ...
class _AdvancedAPI:
    """Advanced IO functionality for power users."""

    @staticmethod
    def get_format_registry() -> type:
        """Get the format registry for advanced format management."""
        return FormatRegistry

    @staticmethod
    def get_dependency_manager() -> type:
        """Get the dependency manager for advanced dependency checking."""
        return DependencyManager

    @staticmethod
    def register_writer(format_name: str, writer_class: type) -> None:
        """Register a custom writer class."""
        return register_writer(format_name, writer_class)

    @staticmethod
    def register_streaming_writer(format_name: str, writer_class: type) -> None:
        """Register a custom streaming writer class."""
        return register_streaming_writer(format_name, writer_class)

    @staticmethod
    def initialize_features() -> None:
        """Manually initialize IO features."""
        return initialize_io_features()


# Make advanced API available but not exported
advanced = _AdvancedAPI()

# Initialize IO features on module import to ensure writers are registered
initialize_io_features()
