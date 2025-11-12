"""Base classes for PyArrow-based writers (Parquet, ORC)."""

import os
import pathlib
from abc import abstractmethod
from typing import Any, BinaryIO, TextIO

from transmog.exceptions import MissingDependencyError, OutputError
from transmog.writers.base import DataWriter, StreamingWriter, _collect_field_names

try:
    import pyarrow as pa

    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    PYARROW_AVAILABLE = False


class PyArrowWriter(DataWriter):
    """Base writer for PyArrow-based formats (Parquet, ORC)."""

    def __init__(self, compression: str, **options: Any) -> None:
        """Initialize the PyArrow writer.

        Args:
            compression: Compression format
            **options: Additional writer options
        """
        self.compression = compression
        self.options = options

    @abstractmethod
    def _get_format_name(self) -> str:
        """Get the format name for error messages."""
        pass

    @abstractmethod
    def _write_table(
        self,
        table: Any,
        destination: str | BinaryIO | TextIO,
        compression: str,
        **options: Any,
    ) -> None:
        """Write a PyArrow table to destination."""
        pass

    def write(
        self,
        data: list[dict[str, Any]],
        destination: str | BinaryIO | TextIO,
        **options: Any,
    ) -> str | BinaryIO | TextIO:
        """Write data to a file.

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
        if pa is None:
            format_name = self._get_format_name()
            raise MissingDependencyError(
                f"PyArrow is required for {format_name} support. "
                "Install with: pip install pyarrow"
            )

        try:
            compression_val = options.get("compression", self.compression)

            if not data:
                return destination

            field_names = _collect_field_names(data)

            columns: dict[str, list[Any]] = {field: [] for field in field_names}
            for record in data:
                for field in field_names:
                    columns[field].append(record.get(field))

            table = pa.table(columns)

            if isinstance(destination, (str, pathlib.Path)):
                path = pathlib.Path(destination)
                path.parent.mkdir(parents=True, exist_ok=True)
                self._write_table(table, str(path), compression_val, **options)
                return str(path) if isinstance(destination, str) else path
            else:
                mode = getattr(destination, "mode", "")
                if mode and "b" not in mode:
                    format_name = self._get_format_name()
                    raise OutputError(
                        f"{format_name} format requires binary streams, "
                        "text streams not supported"
                    )
                self._write_table(table, destination, compression_val, **options)
                return destination
        except Exception as exc:
            if isinstance(exc, (OutputError, MissingDependencyError)):
                raise
            format_name = self._get_format_name()
            raise OutputError(f"Failed to write {format_name} file: {exc}") from exc


class PyArrowStreamingWriter(StreamingWriter):
    """Base streaming writer for PyArrow-based formats."""

    def __init__(
        self,
        destination: str | BinaryIO | TextIO | None = None,
        entity_name: str = "entity",
        compression: str = "snappy",
        batch_size: int = 10000,
        **options: Any,
    ) -> None:
        """Initialize the PyArrow streaming writer.

        Args:
            destination: Path or file-like object to write to
            entity_name: Name of the entity for output files
            compression: Compression algorithm
            batch_size: Number of records per batch
            **options: Additional options for PyArrow
        """
        super().__init__(destination, entity_name, **options)

        if pa is None:
            format_name = self._get_format_name()
            raise MissingDependencyError(
                f"PyArrow is required for {format_name} streaming support. "
                "Install with: pip install pyarrow"
            )

        self.compression = compression
        self.batch_size = batch_size
        self.writers: dict[str, Any] = {}
        self.schemas: dict[str, Any] = {}
        self.buffers: dict[str, list[dict[str, Any]]] = {}
        self.base_dir: str | None = None
        self.file_paths: dict[str, str] = {}

        if isinstance(destination, str):
            self.base_dir = destination
            os.makedirs(self.base_dir, exist_ok=True)

    @abstractmethod
    def _get_format_name(self) -> str:
        """Get the format name."""
        pass

    @abstractmethod
    def _get_file_extension(self) -> str:
        """Get the file extension (e.g., '.parquet', '.orc')."""
        pass

    @abstractmethod
    def _create_writer(self, file_path: str, schema: Any) -> Any:
        """Create the format-specific writer instance."""
        pass

    @abstractmethod
    def _write_to_writer(self, writer: Any, table: Any) -> None:
        """Write table using the format-specific writer."""
        pass

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

        extension = self._get_file_extension()
        if table_name == "main":
            file_path = os.path.join(self.base_dir, f"{self.entity_name}{extension}")
        else:
            safe_name = table_name.replace(".", "_").replace("/", "_")
            file_path = os.path.join(self.base_dir, f"{safe_name}{extension}")

        self.file_paths[table_name] = file_path
        return file_path

    def _create_schema(self, records: list[dict[str, Any]]) -> Any:
        """Create PyArrow schema from records.

        Args:
            records: Records to infer schema from

        Returns:
            PyArrow schema
        """
        if not records:
            return pa.schema([])

        field_names = _collect_field_names(records)
        fields = []

        for key in field_names:
            value = None
            for record in records:
                if key in record and record[key] is not None:
                    value = record[key]
                    break

            if value is None:
                pa_type = pa.string()
            elif isinstance(value, bool):
                pa_type = pa.bool_()
            elif isinstance(value, int):
                pa_type = pa.int64()
            elif isinstance(value, float):
                pa_type = pa.float64()
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
            self.schemas[table_name] = self._create_schema(records)

        schema = self.schemas[table_name]

        columns: dict[str, list[Any]] = {field.name: [] for field in schema}

        for record in records:
            for field in schema:
                field_name = field.name
                value = record.get(field_name)

                if value is None:
                    columns[field_name].append(None)
                else:
                    try:
                        if field.type == pa.bool_():
                            columns[field_name].append(bool(value))
                        elif field.type == pa.int64():
                            columns[field_name].append(int(value))
                        elif field.type == pa.float64():
                            columns[field_name].append(float(value))
                        else:
                            columns[field_name].append(str(value))
                    except (ValueError, TypeError):
                        columns[field_name].append(None)

        arrays = [pa.array(columns[field.name], type=field.type) for field in schema]
        return pa.table(arrays, schema=schema)

    def _initialize_writer(self, table_name: str, schema: Any) -> None:
        """Initialize a writer for a table.

        Args:
            table_name: Name of the table
            schema: PyArrow schema
        """
        file_path = self._get_table_path(table_name)

        if file_path is None:
            return

        writer = self._create_writer(file_path, schema)
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
            self._write_to_writer(writer, table)

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

        if len(self.buffers[table_name]) >= self.batch_size:
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

        if len(self.buffers[table_name]) >= self.batch_size:
            self._write_buffer(table_name)

    def close(self) -> None:
        """Finalize output, flush buffered data, and clean up resources."""
        if getattr(self, "_closed", False):
            return

        for table_name in list(self.buffers.keys()):
            if self.buffers[table_name]:
                self._write_buffer(table_name)

        for writer in self.writers.values():
            if hasattr(writer, "close"):
                writer.close()

        self.writers.clear()
        self.schemas.clear()
        self.buffers.clear()
        self._closed = True


__all__ = ["PyArrowWriter", "PyArrowStreamingWriter", "PYARROW_AVAILABLE"]
