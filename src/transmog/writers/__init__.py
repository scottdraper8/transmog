"""Data writers for various output formats.

This module provides writers for converting flattened data into various
output formats (CSV, Parquet, ORC, Avro). Each format has both a standard writer
and a streaming writer for memory-efficient processing.
"""

from typing import Any

from transmog.exceptions import ConfigurationError
from transmog.writers.avro import AvroStreamingWriter, AvroWriter
from transmog.writers.base import DataWriter, StreamingWriter
from transmog.writers.csv import CsvStreamingWriter, CsvWriter
from transmog.writers.orc import OrcStreamingWriter, OrcWriter
from transmog.writers.parquet import ParquetStreamingWriter, ParquetWriter

FORMATS: dict[str, type[DataWriter]] = {
    "csv": CsvWriter,
    "parquet": ParquetWriter,
    "orc": OrcWriter,
    "avro": AvroWriter,
}
STREAMING_FORMATS: dict[str, type[StreamingWriter]] = {
    "csv": CsvStreamingWriter,
    "parquet": ParquetStreamingWriter,
    "orc": OrcStreamingWriter,
    "avro": AvroStreamingWriter,
}


def create_writer(format_name: str, **kwargs: Any) -> DataWriter:
    """Create a writer for the given format.

    Args:
        format_name: Format name (csv, parquet, orc, avro)
        **kwargs: Format-specific options

    Returns:
        Writer instance

    Raises:
        ConfigurationError: If the format is not supported
    """
    format_name = format_name.lower()
    writer_class = FORMATS.get(format_name)

    if not writer_class:
        raise ConfigurationError(
            f"Unsupported format: {format_name}. Supported: {', '.join(FORMATS.keys())}"
        )

    return writer_class(**kwargs)


def create_streaming_writer(
    format_name: str,
    destination: str | None = None,
    entity_name: str = "entity",
    batch_size: int = 5000,
    coerce_schema: bool = False,
    consolidate: bool = True,
    **kwargs: Any,
) -> StreamingWriter:
    """Create a streaming writer for the given format.

    Args:
        format_name: Format name (csv, parquet, orc, avro)
        destination: Directory path to write output files to
        entity_name: Name of the entity being processed
        batch_size: Number of records per batch before flushing to part file
        coerce_schema: Unify fields and compatible types across parts at close
        consolidate: Merge part files into a single file per table at close
        **kwargs: Format-specific options

    Returns:
        Streaming writer instance

    Raises:
        ConfigurationError: If the format is not supported
    """
    format_name = format_name.lower()
    writer_class = STREAMING_FORMATS.get(format_name)

    if not writer_class:
        raise ConfigurationError(
            f"Unsupported format: {format_name}. "
            f"Supported: {', '.join(STREAMING_FORMATS.keys())}"
        )

    return writer_class(
        destination=destination,
        entity_name=entity_name,
        batch_size=batch_size,
        coerce_schema=coerce_schema,
        consolidate=consolidate,
        **kwargs,
    )


__all__ = [
    "DataWriter",
    "StreamingWriter",
    "AvroWriter",
    "AvroStreamingWriter",
    "CsvWriter",
    "CsvStreamingWriter",
    "OrcWriter",
    "OrcStreamingWriter",
    "ParquetWriter",
    "ParquetStreamingWriter",
    "create_writer",
    "create_streaming_writer",
]
