"""Parquet format writers."""

import os
import pathlib
from typing import Any, BinaryIO, TextIO

from transmog.exceptions import MissingDependencyError, OutputError
from transmog.writers.base import DataWriter, StreamingWriter

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PARQUET_AVAILABLE = True
except ImportError:
    pa = None
    pq = None
    PARQUET_AVAILABLE = False


class ParquetWriter(DataWriter):
    """Parquet format writer using PyArrow."""

    def __init__(self, compression: str = "snappy", **options: Any) -> None:
        """Initialize the Parquet writer.

        Args:
            compression: Compression format (snappy, gzip, brotli, etc.)
            **options: Additional Parquet writer options
        """
        self.compression = compression
        self.options = options

    def write(
        self,
        data: list[dict[str, Any]],
        destination: str | BinaryIO | TextIO,
        **options: Any,
    ) -> str | BinaryIO | TextIO:
        """Write data to a Parquet file.

        Args:
            data: Data to write
            destination: Path or file-like object to write to
            **options: Format-specific options (compression, etc.)

        Returns:
            Path to the written file or file-like object

        Raises:
            MissingDependencyError: If PyArrow is not available
            OutputError: If writing fails
        """
        if pa is None or pq is None:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support. "
                "Install with: pip install pyarrow",
                package="pyarrow",
            )

        try:
            compression_val = options.get("compression", self.compression)

            if not data:
                empty_table = pa.table({})

                if isinstance(destination, (str, pathlib.Path)):
                    path = pathlib.Path(destination)
                    path.parent.mkdir(parents=True, exist_ok=True)
                    pq.write_table(empty_table, str(path), compression=compression_val)
                    return str(path) if isinstance(destination, str) else path
                else:
                    mode = getattr(destination, "mode", "")
                    if mode and "b" not in mode:
                        raise OutputError(
                            "Parquet format requires binary streams, "
                            "text streams not supported"
                        )
                    pq.write_table(
                        empty_table, destination, compression=compression_val
                    )
                    return destination

            columns: dict[str, list[Any]] = {}
            for key in data[0].keys():
                columns[key] = [record.get(key) for record in data]

            table = pa.table(columns)

            if isinstance(destination, (str, pathlib.Path)):
                path = pathlib.Path(destination)
                path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(table, str(path), compression=compression_val, **options)
                return str(path) if isinstance(destination, str) else path
            else:
                mode = getattr(destination, "mode", "")
                if mode and "b" not in mode:
                    raise OutputError(
                        "Parquet format requires binary streams, "
                        "text streams not supported"
                    )
                pq.write_table(
                    table, destination, compression=compression_val, **options
                )
                return destination
        except Exception as exc:
            if isinstance(exc, (OutputError, MissingDependencyError)):
                raise
            raise OutputError(f"Failed to write Parquet file: {exc}") from exc


class ParquetStreamingWriter(StreamingWriter):
    """Streaming writer for Parquet format using PyArrow."""

    def __init__(
        self,
        destination: str | BinaryIO | TextIO | None = None,
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
        super().__init__(destination, entity_name, **options)

        if pa is None:
            raise MissingDependencyError(
                "PyArrow is required for Parquet streaming support. "
                "Install with: pip install pyarrow",
                package="pyarrow",
            )

        self.compression = compression
        self.row_group_size = row_group_size
        self.writers: dict[str, Any] = {}
        self.schemas: dict[str, Any] = {}
        self.buffers: dict[str, list[dict[str, Any]]] = {}
        self.base_dir: str | None = None
        self.file_paths: dict[str, str] = {}
        self.finalized: bool = False

        if isinstance(destination, str):
            self.base_dir = destination
            os.makedirs(self.base_dir, exist_ok=True)

    def _get_table_path(self, table_name: str) -> str | None:
        """Get the file path for a table.

        Args:
            table_name: Name of the table

        Returns:
            File path or None if using file-like object
        """
        if self.base_dir is None:
            return None

        if table_name in self.file_paths:
            return self.file_paths[table_name]

        if table_name == "main":
            file_path = os.path.join(self.base_dir, f"{self.entity_name}.parquet")
        else:
            safe_name = table_name.replace(".", "_").replace("/", "_")
            file_path = os.path.join(self.base_dir, f"{safe_name}.parquet")

        self.file_paths[table_name] = file_path
        return file_path

    def _create_schema(self, records: list[dict[str, Any]], table_name: str) -> Any:
        """Create PyArrow schema from records.

        Args:
            records: Sample records to infer schema from
            table_name: Name of the table

        Returns:
            PyArrow schema
        """
        if not records:
            return pa.schema([])

        sample_record = records[0]
        fields = []

        for key, value in sample_record.items():
            if value is None:
                pa_type = pa.string()
            elif isinstance(value, bool):
                pa_type = pa.bool_()
            elif isinstance(value, int):
                pa_type = pa.int64()
            elif isinstance(value, float):
                pa_type = pa.float64()
            elif isinstance(value, str):
                pa_type = pa.string()
            else:
                pa_type = pa.string()

            fields.append(pa.field(key, pa_type))

        return pa.schema(fields)

    def _records_to_table(self, records: list[dict[str, Any]], table_name: str) -> Any:
        """Convert records to PyArrow table.

        Args:
            records: Records to convert
            table_name: Name of the table

        Returns:
            PyArrow table
        """
        if not records:
            return pa.table({})

        if table_name not in self.schemas:
            self.schemas[table_name] = self._create_schema(records, table_name)

        schema = self.schemas[table_name]

        columns: dict[str, list[Any]] = {}
        for field in schema:
            field_name = field.name
            columns[field_name] = []

        for record in records:
            for field in schema:
                field_name = field.name
                value = record.get(field_name)

                if value is None:
                    columns[field_name].append(None)
                elif field.type == pa.bool_():
                    columns[field_name].append(
                        bool(value) if value is not None else None
                    )
                elif field.type == pa.int64():
                    try:
                        columns[field_name].append(
                            int(value) if value is not None else None
                        )
                    except (ValueError, TypeError):
                        columns[field_name].append(None)
                elif field.type == pa.float64():
                    try:
                        columns[field_name].append(
                            float(value) if value is not None else None
                        )
                    except (ValueError, TypeError):
                        columns[field_name].append(None)
                else:
                    columns[field_name].append(
                        str(value) if value is not None else None
                    )

        arrays = []
        for field in schema:
            field_name = field.name
            arrays.append(pa.array(columns[field_name], type=field.type))

        return pa.table(arrays, schema=schema)

    def _initialize_writer(self, table_name: str, schema: Any) -> None:
        """Initialize a Parquet writer for a table.

        Args:
            table_name: Name of the table
            schema: PyArrow schema
        """
        file_path = self._get_table_path(table_name)

        if file_path is None:
            return

        writer = pq.ParquetWriter(
            file_path, schema, compression=self.compression, **self.options
        )
        self.writers[table_name] = writer
        self.schemas[table_name] = schema

    def _write_buffer(self, table_name: str) -> None:
        """Write buffered records to file.

        Args:
            table_name: Name of the table
        """
        if table_name not in self.buffers or not self.buffers[table_name]:
            return

        records = self.buffers[table_name]
        table = self._records_to_table(records, table_name)

        if table_name not in self.writers:
            self._initialize_writer(table_name, table.schema)

        writer = self.writers.get(table_name)
        if writer:
            writer.write_table(table)

        self.buffers[table_name] = []

    def write_main_records(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of main records.

        Args:
            records: List of main table records to write
        """
        if not records:
            return

        table_name = "main"

        if table_name not in self.buffers:
            self.buffers[table_name] = []

        self.buffers[table_name].extend(records)

        if len(self.buffers[table_name]) >= self.row_group_size:
            self._write_buffer(table_name)

    def write_child_records(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: List of child records to write
        """
        if not records:
            return

        if table_name not in self.buffers:
            self.buffers[table_name] = []

        self.buffers[table_name].extend(records)

        if len(self.buffers[table_name]) >= self.row_group_size:
            self._write_buffer(table_name)

    def finalize(self) -> None:
        """Finalize the output and close all writers."""
        if self.finalized:
            return

        for table_name in list(self.buffers.keys()):
            if self.buffers[table_name]:
                self._write_buffer(table_name)

        for writer in self.writers.values():
            if hasattr(writer, "close"):
                writer.close()

        self.finalized = True

    def close(self) -> None:
        """Clean up resources and close all writers."""
        if not self.finalized:
            self.finalize()

        self.writers.clear()
        self.schemas.clear()
        self.buffers.clear()


__all__ = ["ParquetWriter", "ParquetStreamingWriter", "PARQUET_AVAILABLE"]
