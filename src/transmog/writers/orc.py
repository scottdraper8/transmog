"""ORC format writers."""

from typing import Any, BinaryIO, TextIO

from transmog.writers.arrow_base import PyArrowStreamingWriter, PyArrowWriter

try:
    import pyarrow.orc as orc

    ORC_AVAILABLE = True
except ImportError:
    orc = None
    ORC_AVAILABLE = False


class OrcWriter(PyArrowWriter):
    """ORC format writer using PyArrow."""

    def __init__(self, compression: str = "zstd", **options: Any) -> None:
        """Initialize the ORC writer.

        Args:
            compression: Compression format (zstd, snappy, lz4, zlib, etc.)
            **options: Additional ORC writer options
        """
        super().__init__(compression, **options)

    def _get_format_name(self) -> str:
        """Get the format name for error messages."""
        return "ORC"

    def _write_table(
        self,
        table: Any,
        destination: str | BinaryIO | TextIO,
        compression: str,
        **options: Any,
    ) -> None:
        """Write a PyArrow table to ORC destination."""
        orc.write_table(table, destination, compression=compression, **options)


class OrcStreamingWriter(PyArrowStreamingWriter):
    """Streaming writer for ORC format using PyArrow."""

    def __init__(
        self,
        destination: str | BinaryIO | None = None,
        entity_name: str = "entity",
        compression: str = "zstd",
        batch_size: int = 10000,
        **options: Any,
    ) -> None:
        """Initialize the ORC streaming writer.

        Args:
            destination: Path or file-like object to write to
            entity_name: Name of the entity for output files
            compression: Compression algorithm ("zstd", "snappy", "lz4", etc.)
            batch_size: Number of records per batch
            **options: Additional options for PyArrow
        """
        super().__init__(destination, entity_name, compression, batch_size, **options)

    def _get_format_name(self) -> str:
        """Get the format name."""
        return "ORC"

    def _get_file_extension(self) -> str:
        """Get the file extension."""
        return ".orc"

    def _create_writer(self, file_path: str, schema: Any) -> Any:
        """Create the ORC writer instance."""
        return orc.ORCWriter(file_path, compression=self.compression, **self.options)

    def _write_to_writer(self, writer: Any, table: Any) -> None:
        """Write table using the ORC writer."""
        writer.write(table)


__all__ = ["OrcWriter", "OrcStreamingWriter", "ORC_AVAILABLE"]
