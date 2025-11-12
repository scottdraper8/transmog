"""Data writers for various output formats.

This module provides writers for converting flattened data into various
output formats (CSV, Parquet). Each format has both a standard writer
and a streaming writer for memory-efficient processing.
"""

from typing import Any, BinaryIO

from transmog.exceptions import ConfigurationError, MissingDependencyError
from transmog.writers.base import DataWriter, StreamingWriter
from transmog.writers.csv import CsvStreamingWriter, CsvWriter
from transmog.writers.orc import (
    ORC_AVAILABLE,
    OrcStreamingWriter,
    OrcWriter,
)
from transmog.writers.parquet import (
    PARQUET_AVAILABLE,
    ParquetStreamingWriter,
    ParquetWriter,
)

# Registry of available writer formats
FORMATS: dict[str, type[DataWriter]] = {"csv": CsvWriter}
STREAMING_FORMATS: dict[str, type[StreamingWriter]] = {"csv": CsvStreamingWriter}

if PARQUET_AVAILABLE:
    FORMATS["parquet"] = ParquetWriter
    STREAMING_FORMATS["parquet"] = ParquetStreamingWriter

if ORC_AVAILABLE:
    FORMATS["orc"] = OrcWriter
    STREAMING_FORMATS["orc"] = OrcStreamingWriter


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
                "Install with: pip install pyarrow"
            )
        if format_name == "orc" and not ORC_AVAILABLE:
            raise MissingDependencyError(
                "PyArrow is required for ORC support. Install with: pip install pyarrow"
            )
        raise ConfigurationError(
            f"Unsupported format: {format_name}. Supported: {', '.join(FORMATS.keys())}"
        )

    return writer_class(**kwargs)


def create_streaming_writer(
    format_name: str,
    destination: str | BinaryIO | None = None,
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
                "Install with: pip install pyarrow"
            )
        if format_name == "orc" and not ORC_AVAILABLE:
            raise MissingDependencyError(
                "PyArrow is required for ORC streaming. "
                "Install with: pip install pyarrow"
            )
        raise ConfigurationError(
            f"Unsupported format: {format_name}. "
            f"Supported: {', '.join(STREAMING_FORMATS.keys())}"
        )

    return writer_class(destination=destination, entity_name=entity_name, **kwargs)


__all__ = [
    "DataWriter",
    "StreamingWriter",
    "CsvWriter",
    "CsvStreamingWriter",
    "OrcWriter",
    "OrcStreamingWriter",
    "ParquetWriter",
    "ParquetStreamingWriter",
    "create_writer",
    "create_streaming_writer",
]
