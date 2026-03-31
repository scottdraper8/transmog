"""Parquet format writers."""

from typing import Any, BinaryIO, TextIO

from transmog.writers.arrow_base import PyArrowStreamingWriter, PyArrowWriter

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PARQUET_AVAILABLE = True
    _PARQUET_READ_ERRORS: tuple[type[Exception], ...] = (
        OSError,
        pa.lib.ArrowException,
    )
except ImportError:
    pa = None
    pq = None
    PARQUET_AVAILABLE = False
    _PARQUET_READ_ERRORS: tuple[type[Exception], ...] = (OSError,)  # type: ignore[no-redef]


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
        destination: str | None = None,
        entity_name: str = "entity",
        compression: str = "snappy",
        **options: Any,
    ) -> None:
        """Initialize the Parquet streaming writer.

        Args:
            destination: Directory path to write part files to
            entity_name: Name of the entity for output files
            compression: Compression algorithm ("snappy", "gzip", etc.)
            **options: Additional options for PyArrow
        """
        super().__init__(
            destination=destination,
            entity_name=entity_name,
            compression=compression,
            **options,
        )

    def _get_format_name(self) -> str:
        """Get the format name."""
        return "Parquet"

    def _get_file_extension(self) -> str:
        """Get the file extension."""
        return ".parquet"

    def _create_format_writer(self, file_path: str, schema: Any) -> Any:
        """Create the Parquet writer instance."""
        return pq.ParquetWriter(
            file_path, schema, compression=self.compression, **self.options
        )

    def _write_to_format_writer(self, writer: Any, table: Any) -> None:
        """Write table using the Parquet writer."""
        writer.write_table(table)

    def _rewrite_part_with_schema(self, file_path: str, target_schema: Any) -> None:
        """Rewrite a Parquet part file with a new target schema."""
        table = pq.read_table(file_path)

        # Add missing columns as null arrays
        for field in target_schema:
            if field.name not in table.column_names:
                null_array = pa.nulls(len(table), type=field.type)
                table = table.append_column(field, null_array)

        # Reorder columns to match target schema, then cast types
        table = table.select([f.name for f in target_schema])
        table = table.cast(target_schema)

        pq.write_table(table, file_path, compression=self.compression)


__all__ = ["ParquetWriter", "ParquetStreamingWriter", "PARQUET_AVAILABLE"]
