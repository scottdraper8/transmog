"""
Writer factory for creating writers based on format.

This module provides factory functions for creating writers
for different output formats.
"""

import os
import importlib
from typing import Any, Dict, Optional, Type, Union, BinaryIO, List, Callable, Set

from transmog.types.io_types import WriterProtocol, StreamingWriterProtocol
from transmog.error import (
    ConfigurationError,
    MissingDependencyError,
    logger,
)
from .formats import FormatRegistry

# Registry of writer classes
_WRITER_REGISTRY: Dict[str, Type[WriterProtocol]] = {}
_STREAMING_WRITER_REGISTRY: Dict[str, Type[StreamingWriterProtocol]] = {}


def register_writer(format_name: str, writer_class: Type[WriterProtocol]) -> None:
    """
    Register a writer class for a format.

    Args:
        format_name: Format name
        writer_class: Writer class to register
    """
    _WRITER_REGISTRY[format_name.lower()] = writer_class
    # Also register with the FormatRegistry for discovery
    FormatRegistry.register_writer_format(format_name)
    logger.debug(f"Registered writer for {format_name}")


def register_streaming_writer(
    format_name: str, writer_class: Type[StreamingWriterProtocol]
) -> None:
    """
    Register a streaming writer class for a format.

    Args:
        format_name: Format name
        writer_class: StreamingWriter class to register
    """
    _STREAMING_WRITER_REGISTRY[format_name.lower()] = writer_class
    logger.debug(f"Registered streaming writer for {format_name}")


def create_writer(format_name: str, **kwargs) -> WriterProtocol:
    """
    Create a writer for the given format.

    Args:
        format_name: Format name
        **kwargs: Format-specific options

    Returns:
        WriterProtocol: Writer instance

    Raises:
        ConfigurationError: If the format is not supported
        MissingDependencyError: If a required dependency is missing
    """
    format_name = format_name.lower()

    if format_name not in _WRITER_REGISTRY:
        try:
            # Try to dynamically load the writer
            module_path = f"transmog.io.writers.{format_name}"
            importlib.import_module(module_path)
        except ImportError as e:
            # Handle specific error for missing dependency
            if "dependency" in str(e).lower():
                raise MissingDependencyError(
                    f"Missing dependency for {format_name} format: {str(e)}"
                )
            # Otherwise, the format is not supported
            raise ConfigurationError(f"Unsupported output format: {format_name}")

    # Try again after loading
    if format_name not in _WRITER_REGISTRY:
        raise ConfigurationError(f"No writer registered for format: {format_name}")

    try:
        # Create and return the writer instance
        writer_class = _WRITER_REGISTRY[format_name]
        return writer_class(**kwargs)
    except Exception as e:
        # Wrap any initialization errors
        raise ConfigurationError(f"Failed to create {format_name} writer: {str(e)}")


def create_streaming_writer(
    format_name: str,
    destination: Optional[Union[str, BinaryIO]] = None,
    entity_name: str = "entity",
    **kwargs,
) -> StreamingWriterProtocol:
    """
    Create a streaming writer for the given format.

    Args:
        format_name: Format name
        destination: File path or file-like object to write to
        entity_name: Name of the entity being processed
        **kwargs: Format-specific options

    Returns:
        StreamingWriterProtocol: Streaming writer instance

    Raises:
        ConfigurationError: If the format is not supported
        MissingDependencyError: If a required dependency is missing
    """
    format_name = format_name.lower()

    if format_name not in _STREAMING_WRITER_REGISTRY:
        try:
            # Try to dynamically load the writer
            module_path = f"transmog.io.writers.{format_name}"
            importlib.import_module(module_path)
        except ImportError as e:
            # Handle specific error for missing dependency
            if "dependency" in str(e).lower():
                raise MissingDependencyError(
                    f"Missing dependency for {format_name} format: {str(e)}"
                )
            # Otherwise, the format is not supported
            raise ConfigurationError(f"Unsupported output format: {format_name}")

    # Try again after loading
    if format_name not in _STREAMING_WRITER_REGISTRY:
        raise ConfigurationError(
            f"No streaming writer registered for format: {format_name}"
        )

    try:
        # Create and return the streaming writer instance
        writer_class = _STREAMING_WRITER_REGISTRY[format_name]
        return writer_class(destination=destination, entity_name=entity_name, **kwargs)
    except MissingDependencyError:
        # Let MissingDependencyError propagate directly
        raise
    except Exception as e:
        # Wrap any other initialization errors
        raise ConfigurationError(
            f"Failed to create {format_name} streaming writer: {str(e)}"
        )


def get_supported_formats() -> Dict[str, str]:
    """
    Get the supported output formats.

    Returns:
        Dict[str, str]: Dictionary of format names and descriptions
    """
    return {fmt: str(cls.__doc__ or "") for fmt, cls in _WRITER_REGISTRY.items()}


def get_supported_streaming_formats() -> List[str]:
    """
    Get the supported streaming output formats.

    Returns:
        List[str]: List of format names with streaming writer support
    """
    return list(_STREAMING_WRITER_REGISTRY.keys())


def is_format_available(format_name: str) -> bool:
    """
    Check if a specific format has a registered writer.

    Args:
        format_name: Format name to check

    Returns:
        bool: Whether the format is available
    """
    return format_name.lower() in _WRITER_REGISTRY


def is_streaming_format_available(format_name: str) -> bool:
    """
    Check if a specific format has a registered streaming writer.

    Args:
        format_name: Format name to check

    Returns:
        bool: Whether the streaming format is available
    """
    return format_name.lower() in _STREAMING_WRITER_REGISTRY


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
