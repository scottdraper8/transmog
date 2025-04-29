"""
IO module for Transmog package.

This module provides input/output functionality for various file formats.
"""

from .formats import FormatRegistry, DependencyManager, detect_format

# Import writer interface and factory
from .writer_interface import DataWriter
from .writer_factory import WriterFactory

# Import reader and writer submodules
from . import readers
from . import writers


def initialize_io_features():
    """
    Initialize all IO features.

    This function ensures all IO modules are properly loaded
    and their formats are registered.
    """
    # Import causes side-effects (format registration)
    from . import readers
    from . import writers


# Initialize formats on import
initialize_io_features()


def get_available_reader_formats():
    """
    Get a list of all available reader formats.

    Returns:
        List of format names
    """
    return FormatRegistry.get_available_reader_formats()


def get_available_writer_formats():
    """
    Get a list of all available writer formats.

    Returns:
        List of format names
    """
    return FormatRegistry.get_available_writer_formats()


def has_reader_format(format_name):
    """
    Check if a reader format is available.

    Args:
        format_name: Format name to check

    Returns:
        Whether the format is available
    """
    return FormatRegistry.has_reader_format(format_name)


def has_writer_format(format_name):
    """
    Check if a writer format is available.

    Args:
        format_name: Format name to check

    Returns:
        Whether the format is available
    """
    return FormatRegistry.has_writer_format(format_name)


def create_writer(format_name, **options):
    """
    Create a writer for the specified format.

    Args:
        format_name: Format name
        **options: Format-specific options

    Returns:
        Writer instance or None if not available
    """
    return WriterFactory.create_writer(format_name, **options)


__all__ = [
    "initialize_io_features",
    "get_available_reader_formats",
    "get_available_writer_formats",
    "has_reader_format",
    "has_writer_format",
    "create_writer",
    "detect_format",
    "DataWriter",
    "WriterFactory",
    "readers",
    "writers",
]
