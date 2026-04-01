"""ORC format writers."""

from typing import Any, BinaryIO, TextIO

from transmog.writers.arrow_base import PyArrowStreamingWriter, PyArrowWriter

try:
    import pyarrow as pa
    import pyarrow.orc as orc

    ORC_AVAILABLE = True
except ImportError:
    pa = None
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
        destination: str | None = None,
        entity_name: str = "entity",
        compression: str = "zstd",
        **options: Any,
    ) -> None:
        """Initialize the ORC streaming writer.

        Args:
            destination: Directory path to write part files to
            entity_name: Name of the entity for output files
            compression: Compression algorithm ("zstd", "snappy", "lz4", etc.)
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
        return "ORC"

    def _get_file_extension(self) -> str:
        """Get the file extension."""
        return ".orc"

    def _create_format_writer(self, file_path: str, schema: Any) -> Any:
        """Create the ORC writer instance."""
        return orc.ORCWriter(file_path, compression=self.compression, **self.options)

    def _write_to_format_writer(self, writer: Any, table: Any) -> None:
        """Write table using the ORC writer."""
        writer.write(table)

    def _read_part_table(self, file_path: str) -> Any:
        """Read an ORC part file as a PyArrow Table."""
        return orc.read_table(file_path)

    def _rewrite_part(self, file_path: str, target_schema: Any) -> None:
        """Rewrite an ORC part file with a new target schema."""
        table = orc.read_table(file_path)
        table = self._promote_table_to_schema(table, target_schema)
        orc.write_table(table, file_path, compression=self.compression)


__all__ = ["OrcWriter", "OrcStreamingWriter", "ORC_AVAILABLE"]
