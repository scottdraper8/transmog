"""Writer factory for creating writers based on format."""

from typing import Any, BinaryIO, Optional, Union

from transmog.error import (
    ConfigurationError,
    MissingDependencyError,
)
from transmog.io.writer_interface import DataWriter, StreamingWriter

# Import built-in writers
from .writers.csv import CsvStreamingWriter, CsvWriter

try:
    from .writers.parquet import ParquetStreamingWriter, ParquetWriter

    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False


# Format support
FORMATS: dict[str, type[DataWriter]] = {"csv": CsvWriter}
STREAMING_FORMATS: dict[str, type[StreamingWriter]] = {"csv": CsvStreamingWriter}

if PARQUET_AVAILABLE:
    FORMATS["parquet"] = ParquetWriter
    STREAMING_FORMATS["parquet"] = ParquetStreamingWriter


def create_writer(format_name: str, **kwargs: Any) -> DataWriter:
    """Create a writer for the given format.

    Args:
        format_name: Format name (csv, parquet)
        **kwargs: Format-specific options

    Returns:
        Writer instance

    Raises:
        ConfigurationError: If the format is not supported
        MissingDependencyError: If a required dependency is missing
    """
    format_name = format_name.lower()
    writer_class = FORMATS.get(format_name)

    if not writer_class:
        if format_name == "parquet" and not PARQUET_AVAILABLE:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support. "
                "Install with: pip install pyarrow",
                package="pyarrow",
            )
        raise ConfigurationError(
            f"Unsupported format: {format_name}. Supported: {', '.join(FORMATS.keys())}"
        )

    try:
        return writer_class(**kwargs)
    except Exception as e:
        raise ConfigurationError(f"Failed to create {format_name} writer: {e}") from e


def create_streaming_writer(
    format_name: str,
    destination: Optional[Union[str, BinaryIO]] = None,
    entity_name: str = "entity",
    **kwargs: Any,
) -> StreamingWriter:
    """Create a streaming writer for the given format.

    Args:
        format_name: Format name (csv, parquet)
        destination: File path or file-like object to write to
        entity_name: Name of the entity being processed
        **kwargs: Format-specific options

    Returns:
        Streaming writer instance

    Raises:
        ConfigurationError: If the format is not supported
        MissingDependencyError: If a required dependency is missing
    """
    format_name = format_name.lower()
    writer_class = STREAMING_FORMATS.get(format_name)

    if not writer_class:
        if format_name == "parquet" and not PARQUET_AVAILABLE:
            raise MissingDependencyError(
                "PyArrow is required for Parquet streaming. "
                "Install with: pip install pyarrow",
                package="pyarrow",
            )
        raise ConfigurationError(
            f"Unsupported format: {format_name}. "
            f"Supported: {', '.join(STREAMING_FORMATS.keys())}"
        )

    try:
        return writer_class(destination=destination, entity_name=entity_name, **kwargs)
    except Exception as e:
        if isinstance(e, MissingDependencyError):
            raise
        raise ConfigurationError(
            f"Failed to create {format_name} streaming writer: {e}"
        ) from e
