"""
Writer factory module for managing output format writers.

This module provides a factory for creating writers for different
output formats, replacing the old registry-based approach.
"""

from typing import Dict, Type, Optional, Any, List, Callable

from .writer_interface import DataWriter
from .formats import FormatRegistry


class WriterFactory:
    """
    Factory for creating writer instances.

    This class manages the creation of writer objects for different
    output formats, handling the dependencies and configuration.
    """

    _writers: Dict[str, Type[DataWriter]] = {}

    @classmethod
    def register(cls, format_name: str, writer_class: Type[DataWriter]) -> None:
        """
        Register a writer implementation for a format.

        Args:
            format_name: Name of the format (e.g., 'json', 'csv')
            writer_class: Class implementing the DataWriter
        """
        cls._writers[format_name] = writer_class
        # Also register with the FormatRegistry for discovery
        FormatRegistry.register_writer_format(format_name)

    @classmethod
    def create_writer(cls, format_name: str, **options) -> Optional[DataWriter]:
        """
        Create a writer instance for the specified format.

        Args:
            format_name: Name of the format to create a writer for
            **options: Format-specific options to pass to the writer

        Returns:
            Writer instance or None if the format is not available
        """
        if format_name not in cls._writers:
            return None

        writer_class = cls._writers[format_name]
        return writer_class(**options)

    @classmethod
    def get_writer_class(cls, format_name: str) -> Optional[Type[DataWriter]]:
        """
        Get the writer class for a format without instantiating it.

        Args:
            format_name: Format name to get the writer class for

        Returns:
            Writer class or None if not available
        """
        return cls._writers.get(format_name)

    @classmethod
    def list_available_formats(cls) -> List[str]:
        """
        List all registered format names.

        Returns:
            List of format names with registered writers
        """
        return list(cls._writers.keys())

    @classmethod
    def is_format_available(cls, format_name: str) -> bool:
        """
        Check if a specific format has a registered writer.

        Args:
            format_name: Format name to check

        Returns:
            Whether the format is available
        """
        return format_name in cls._writers
