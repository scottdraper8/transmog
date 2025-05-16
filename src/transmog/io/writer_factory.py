"""Writer factory for creating writers based on format.

This module provides factory functions for creating writers
for different output formats.
"""

import importlib
from typing import Any, BinaryIO, Optional, Union

from transmog.error import (
    ConfigurationError,
    MissingDependencyError,
    logger,
)
from transmog.types.io_types import StreamingWriterProtocol, WriterProtocol

from .formats import FormatRegistry

# Writer class registries
_WRITER_REGISTRY: dict[str, type[WriterProtocol]] = {}
_STREAMING_WRITER_REGISTRY: dict[str, type[StreamingWriterProtocol]] = {}


def register_writer(format_name: str, writer_class: type[WriterProtocol]) -> None:
    """Register a writer class for a format.

    Args:
        format_name: Format name
        writer_class: Writer class to register
    """
    _WRITER_REGISTRY[format_name.lower()] = writer_class
    # Register with FormatRegistry for format discovery
    FormatRegistry.register_writer_format(format_name)
    logger.debug(f"Registered writer for {format_name}")


def register_streaming_writer(
    format_name: str, writer_class: type[StreamingWriterProtocol]
) -> None:
    """Register a streaming writer class for a format.

    Args:
        format_name: Format name
        writer_class: StreamingWriter class to register
    """
    _STREAMING_WRITER_REGISTRY[format_name.lower()] = writer_class
    logger.debug(f"Registered streaming writer for {format_name}")


def create_writer(format_name: str, **kwargs: Any) -> WriterProtocol:
    """Create a writer for the given format.

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
            # Attempt dynamic loading of writer module
            module_path = f"transmog.io.writers.{format_name}"
            importlib.import_module(module_path)
        except ImportError as e:
            # Differentiate between missing dependency and unsupported format
            if "dependency" in str(e).lower():
                raise MissingDependencyError(
                    f"Missing dependency for {format_name} format: {str(e)}",
                    package=format_name,
                ) from e
            raise ConfigurationError(f"Unsupported output format: {format_name}") from e

    # Check registry again after attempted module load
    if format_name not in _WRITER_REGISTRY:
        raise ConfigurationError(f"No writer registered for format: {format_name}")

    try:
        writer_class = _WRITER_REGISTRY[format_name]
        return writer_class(**kwargs)
    except Exception as e:
        raise ConfigurationError(
            f"Failed to create {format_name} writer: {str(e)}"
        ) from e


def create_streaming_writer(
    format_name: str,
    destination: Optional[Union[str, BinaryIO]] = None,
    entity_name: str = "entity",
    **kwargs: Any,
) -> StreamingWriterProtocol:
    """Create a streaming writer for the given format.

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
            # Attempt dynamic loading of writer module
            module_path = f"transmog.io.writers.{format_name}"
            importlib.import_module(module_path)
        except ImportError as e:
            # Differentiate between missing dependency and unsupported format
            if "dependency" in str(e).lower():
                raise MissingDependencyError(
                    f"Missing dependency for {format_name} format: {str(e)}",
                    package=format_name,
                ) from e
            raise ConfigurationError(f"Unsupported output format: {format_name}") from e

    # Check registry again after attempted module load
    if format_name not in _STREAMING_WRITER_REGISTRY:
        raise MissingDependencyError(
            f"No streaming writer registered for format: {format_name}",
            package=format_name,
        )

    try:
        writer_class = _STREAMING_WRITER_REGISTRY[format_name]
        return writer_class(destination=destination, entity_name=entity_name, **kwargs)
    except MissingDependencyError:
        # Preserve MissingDependencyError exceptions
        raise
    except Exception as e:
        raise ConfigurationError(
            f"Failed to create {format_name} streaming writer: {str(e)}"
        ) from e


def get_supported_formats() -> dict[str, str]:
    """Get the supported output formats.

    Returns:
        dict[str, str]: Dictionary of format names and descriptions
    """
    return {fmt: str(cls.__doc__ or "") for fmt, cls in _WRITER_REGISTRY.items()}


def get_supported_streaming_formats() -> list[str]:
    """Get the supported streaming output formats.

    Returns:
        list[str]: List of format names with streaming writer support
    """
    return list(_STREAMING_WRITER_REGISTRY.keys())


def is_format_available(format_name: str) -> bool:
    """Check if a specific format has a registered writer.

    Args:
        format_name: Format name to check

    Returns:
        bool: Whether the format is available
    """
    return format_name.lower() in _WRITER_REGISTRY


def is_streaming_format_available(format_name: str) -> bool:
    """Check if a specific format has a registered streaming writer.

    Args:
        format_name: Format name to check

    Returns:
        bool: Whether the streaming format is available
    """
    return format_name.lower() in _STREAMING_WRITER_REGISTRY
