"""Base classes for PyArrow-based writers (Parquet, ORC)."""

import logging
import math
import pathlib
from abc import abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO, TextIO

from transmog.exceptions import MissingDependencyError, OutputError
from transmog.writers.base import DataWriter, StreamingWriter, _collect_field_names

logger = logging.getLogger(__name__)

try:
    import pyarrow as pa

    PYARROW_AVAILABLE = True
    _ARROW_WRITE_ERRORS: tuple[type[Exception], ...] = (OSError, pa.lib.ArrowException)
except ImportError:
    pa = None
    PYARROW_AVAILABLE = False
    _ARROW_WRITE_ERRORS: tuple[type[Exception], ...] = (OSError,)  # type: ignore[no-redef]


def _is_valid_float_for_inference(value: Any) -> bool:
    """Check if a float value is valid for type inference.

    NaN and Infinity values should not be used for type inference as they
    don't represent typical float data patterns.
    """
    if not isinstance(value, float):
        return False
    return not (math.isnan(value) or math.isinf(value))


def _convert_bool(value: Any) -> bool | None:
    """Convert a value to bool for Arrow column."""
    try:
        return bool(value)
    except (ValueError, TypeError):
        return None


def _convert_int(value: Any) -> int | None:
    """Convert a value to int for Arrow column."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _convert_float(value: Any) -> float | None:
    """Convert a value to float for Arrow column."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _convert_str(value: Any) -> str | None:
    """Convert a value to str for Arrow column."""
    try:
        return str(value)
    except (ValueError, TypeError):
        return None


_TYPE_CONVERTERS: dict[Any, Callable] = {}


def _get_type_converters() -> dict[Any, Callable]:
    """Get the mapping of PyArrow types to converter functions.

    Lazily initialized to avoid import-time dependency on PyArrow.
    """
    if not _TYPE_CONVERTERS and pa is not None:
        _TYPE_CONVERTERS[pa.bool_()] = _convert_bool
        _TYPE_CONVERTERS[pa.int64()] = _convert_int
        _TYPE_CONVERTERS[pa.float64()] = _convert_float
    return _TYPE_CONVERTERS


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
        except _ARROW_WRITE_ERRORS as exc:
            format_name = self._get_format_name()
            raise OutputError(f"Failed to write {format_name} file: {exc}") from exc


class PyArrowStreamingWriter(StreamingWriter):
    """Base streaming writer for PyArrow-based formats (Parquet, ORC)."""

    def __init__(
        self,
        destination: str | None = None,
        entity_name: str = "entity",
        compression: str = "snappy",
        stringify_values: bool = False,
        **options: Any,
    ) -> None:
        """Initialize the PyArrow streaming writer.

        Args:
            destination: Directory path to write part files to
            entity_name: Name of the entity for output files
            compression: Compression algorithm
            stringify_values: If True, all fields are strings (skip type inference)
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
        self.stringify_values = stringify_values
        self._column_buffers: dict[str, dict[str, list[Any]]] = {}

    @abstractmethod
    def _get_format_name(self) -> str:
        """Get the format name."""
        pass

    @abstractmethod
    def _get_file_extension(self) -> str:
        """Get the file extension (e.g., '.parquet', '.orc')."""
        pass

    @abstractmethod
    def _create_format_writer(self, file_path: str, schema: Any) -> Any:
        """Create the format-specific writer instance."""
        pass

    @abstractmethod
    def _write_to_format_writer(self, writer: Any, table: Any) -> None:
        """Write table using the format-specific writer."""
        pass

    # --- Schema inference (PyArrow-specific) ---

    def _create_schema(
        self, records: list[dict[str, Any]], stringify_mode: bool = False
    ) -> tuple[Any, dict[str, Callable]]:
        """Create PyArrow schema and field converters from records.

        Handles special float values (NaN, Inf) by skipping them during type
        inference, as they don't represent typical data patterns.

        Args:
            records: Records to infer schema from
            stringify_mode: If True, all fields are strings (skip type inference)

        Returns:
            Tuple of (PyArrow schema, dict mapping field names to converters)
        """
        if not records:
            return pa.schema([]), {}

        field_names = _collect_field_names(records)
        type_converters = _get_type_converters()

        if stringify_mode:
            fields = [pa.field(key, pa.string()) for key in field_names]
            converters = dict.fromkeys(field_names, _convert_str)
            logger.debug(
                "arrow schema created (stringify mode), fields=%d", len(fields)
            )
            return pa.schema(fields), converters

        fields = []
        converters = {}

        for key in field_names:
            value = None
            found_float = False

            for record in records:
                if key not in record:
                    continue

                val = record[key]
                if val is None:
                    continue

                if isinstance(val, float):
                    found_float = True
                    if _is_valid_float_for_inference(val):
                        value = val
                        break
                else:
                    value = val
                    break

            if value is None and found_float:
                pa_type = pa.float64()
            elif value is None:
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
            converters[key] = type_converters.get(pa_type, _convert_str)

        if logger.isEnabledFor(logging.DEBUG):
            types = {f.name: str(f.type) for f in fields}
            logger.debug(
                "arrow schema created, fields=%d, types=%s", len(fields), types
            )
        return pa.schema(fields), converters

    def _records_to_table(
        self,
        records: list[dict[str, Any]],
        schema: Any,
        converters: dict[str, Callable],
    ) -> Any:
        """Convert records to PyArrow table using a provided schema."""
        if not records:
            return pa.table({})

        columns: dict[str, list[Any]] = {field.name: [] for field in schema}

        for record in records:
            for field in schema:
                field_name = field.name
                value = record.get(field_name)

                if value is None:
                    columns[field_name].append(None)
                else:
                    columns[field_name].append(converters[field_name](value))

        arrays = [pa.array(columns[field.name], type=field.type) for field in schema]
        return pa.table(arrays, schema=schema)

    # --- StreamingWriter abstract method implementations ---

    def _write_part(self, file_path: str, records: list[dict[str, Any]]) -> Any:
        """Write records to a PyArrow part file and return the schema."""
        schema, converters = self._create_schema(
            records, stringify_mode=self.stringify_values
        )
        table = self._records_to_table(records, schema, converters)

        writer = self._create_format_writer(file_path, schema)
        self._write_to_format_writer(writer, table)
        if hasattr(writer, "close"):
            writer.close()

        return schema

    @staticmethod
    def _schema_to_dict(schema: Any) -> dict:
        """Serialize a PyArrow schema to a JSON-friendly dict."""
        return {"fields": [{"name": f.name, "type": str(f.type)} for f in schema]}

    @staticmethod
    def _compute_deviations(base_schema: Any, part_schema: Any) -> dict | None:
        """Compute schema deviations between a base and part schema."""
        base_fields = {f.name: str(f.type) for f in base_schema}
        part_fields = {f.name: str(f.type) for f in part_schema}

        added = sorted(name for name in part_fields if name not in base_fields)
        removed = sorted(name for name in base_fields if name not in part_fields)
        type_changes = {
            name: {"base": base_fields[name], "part": part_fields[name]}
            for name in base_fields
            if name in part_fields and base_fields[name] != part_fields[name]
        }

        if not added and not removed and not type_changes:
            return None

        result: dict[str, Any] = {}
        if added or removed:
            result["structural"] = {"added": added, "removed": removed}
        if type_changes:
            result["type"] = type_changes
        return result

    def _schema_fingerprint(self, schema: Any) -> tuple:
        """Create a hashable fingerprint from a PyArrow schema."""
        return tuple((f.name, str(f.type)) for f in schema)

    def _build_unified_schema(self, schemas: list[Any]) -> Any:
        """Build a unified PyArrow schema from all part schemas."""
        return pa.unify_schemas(schemas)

    def _rewrite_part(self, file_path: str, target_schema: Any) -> None:
        """Rewrite a part file with a new target schema.

        Delegates to subclass-specific read/rewrite methods.
        """
        self._rewrite_part_with_schema(file_path, target_schema)

    @abstractmethod
    def _rewrite_part_with_schema(self, file_path: str, target_schema: Any) -> None:
        """Rewrite a part file with a target schema (format-specific)."""
        ...

    def close(self) -> list[Path]:
        """Finalize output, flush buffered data, and clear PyArrow caches."""
        paths = super().close()
        self._column_buffers.clear()
        return paths


__all__ = ["PyArrowWriter", "PyArrowStreamingWriter", "PYARROW_AVAILABLE"]
