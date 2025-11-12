"""Parquet format writers."""

from typing import Any, BinaryIO, TextIO

from transmog.writers.arrow_base import PyArrowStreamingWriter, PyArrowWriter

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PARQUET_AVAILABLE = True
except ImportError:
    pa = None
    pq = None
    PARQUET_AVAILABLE = False


class ParquetWriter(PyArrowWriter):
    """Parquet format writer using PyArrow."""

    def __init__(self, compression: str = "snappy", **options: Any) -> None:
        """Initialize the Parquet writer.

        Args:
            compression: Compression format (snappy, gzip, brotli, etc.)
            **options: Additional Parquet writer options
        """
        super().__init__(compression, **options)

    def _get_format_name(self) -> str:
        """Get the format name for error messages."""
        return "Parquet"

    def _write_table(
        self,
        table: Any,
        destination: str | BinaryIO | TextIO,
        compression: str,
        **options: Any,
    ) -> None:
        """Write a PyArrow table to Parquet destination."""
        pq.write_table(table, destination, compression=compression, **options)


class ParquetStreamingWriter(PyArrowStreamingWriter):
    """Streaming writer for Parquet format using PyArrow."""

    def __init__(
        self,
        destination: str | BinaryIO | None = None,
        entity_name: str = "entity",
        compression: str = "snappy",
        row_group_size: int = 10000,
        **options: Any,
    ) -> None:
        """Initialize the Parquet streaming writer.

        Args:
            destination: Path or file-like object to write to
            entity_name: Name of the entity for output files
            compression: Compression algorithm ("snappy", "gzip", etc.)
            row_group_size: Number of records per row group
            **options: Additional options for PyArrow
        """
        super().__init__(
            destination, entity_name, compression, row_group_size, **options
        )

    def _get_format_name(self) -> str:
        """Get the format name."""
        return "Parquet"

    def _get_file_extension(self) -> str:
        """Get the file extension."""
        return ".parquet"

    def _create_writer(self, file_path: str, schema: Any) -> Any:
        """Create the Parquet writer instance."""
        return pq.ParquetWriter(
            file_path, schema, compression=self.compression, **self.options
        )

    def _write_to_writer(self, writer: Any, table: Any) -> None:
        """Write table using the Parquet writer."""
        writer.write_table(table)


__all__ = ["ParquetWriter", "ParquetStreamingWriter", "PARQUET_AVAILABLE"]
