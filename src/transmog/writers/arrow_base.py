"""Shared native Arrow inference and part-file schema promotion."""

import json
import pathlib
from abc import abstractmethod
from typing import Any, BinaryIO, TextIO

import pyarrow as pa

from transmog.exceptions import ConfigurationError, OutputError
from transmog.writers.base import (
    DataWriter,
    StreamingWriter,
    _collect_field_names,
)

_ARROW_WRITE_ERRORS: tuple[type[Exception], ...] = (
    OSError,
    OverflowError,
    pa.lib.ArrowException,
)
_NATIVE_TYPE_ERRORS = (
    OverflowError,
    pa.ArrowInvalid,
    pa.ArrowTypeError,
    pa.ArrowNotImplementedError,
)
_JSON_ENCODING = {b"transmog.encoding": b"json"}


def _is_json_field(field: Any) -> bool:
    """Recognize the explicit wire encoding, not the content of a string."""
    return (field.metadata or {}).get(b"transmog.encoding") == b"json"


def _json_field(field: Any) -> Any:
    """Mark a whole-column JSON representation while retaining field metadata."""
    return pa.field(
        field.name,
        pa.string(),
        nullable=field.nullable,
        metadata={**(field.metadata or {}), **_JSON_ENCODING},
    )


def _json_array(values: list[Any]) -> Any:
    """Encode JSON values unambiguously; native strings retain their quotes."""
    try:
        encoded = [
            None
            if value is None
            else json.dumps(value, ensure_ascii=False, allow_nan=False)
            for value in values
        ]
    except (TypeError, ValueError) as exc:
        raise OutputError(f"Value cannot be represented as JSON: {exc}") from exc
    return pa.array(encoded, type=pa.string())


def _native_shape_is_lossless(value: Any, data_type: Any) -> bool:
    """Preserve JSON value kinds and nested keys, allowing exact numeric promotion."""
    if value is None:
        return True
    if isinstance(value, bool):
        return bool(pa.types.is_boolean(data_type))
    if isinstance(value, int):
        return bool(pa.types.is_integer(data_type) or pa.types.is_floating(data_type))
    if isinstance(value, float):
        return bool(pa.types.is_floating(data_type))
    if isinstance(value, str):
        return bool(
            pa.types.is_string(data_type) or pa.types.is_large_string(data_type)
        )
    if pa.types.is_struct(data_type):
        return (
            bool(data_type.num_fields)
            and set(value) == set(data_type.names)
            and all(
                _native_shape_is_lossless(value[field.name], field.type)
                for field in data_type
            )
        )
    if pa.types.is_list(data_type):
        return all(
            _native_shape_is_lossless(item, data_type.value_type) for item in value
        )
    return True


def _cast_native_column(column: Any, data_type: Any) -> Any:
    """Cast safely, restoring null children encoded as storage-only strings."""
    try:
        return column.cast(data_type, safe=True)
    except pa.ArrowNotImplementedError:
        return pa.array(column.to_pylist()).cast(data_type, safe=True)


def _records_to_arrow_table(records: list[dict[str, Any]]) -> Any:
    """Infer complete columns, falling back to marked JSON without losing values."""
    arrays, fields = [], []
    for name in _collect_field_names(records):
        values = [record.get(name) for record in records]
        try:
            array = pa.array(values)
        except _NATIVE_TYPE_ERRORS:
            array = None
        if array is None or not all(
            _native_shape_is_lossless(value, array.type) for value in values
        ):
            array = _json_array(values)
            field = _json_field(pa.field(name, pa.string()))
        else:
            field = pa.field(name, array.type)
        arrays.append(array)
        fields.append(field)
    return pa.table(arrays, schema=pa.schema(fields))


def _storage_type(data_type: Any) -> Any:
    """Replace unresolved null types for codecs such as ORC.

    The logical part schema retains null types so a later numeric or nested
    value can determine the unified type without a string-type conflict.
    """
    if pa.types.is_null(data_type):
        return pa.string()
    if pa.types.is_list(data_type):
        return pa.list_(_storage_type(data_type.value_type))
    if pa.types.is_struct(data_type):
        return pa.struct(
            [field.with_type(_storage_type(field.type)) for field in data_type]
        )
    return data_type


def _storage_schema(schema: Any) -> Any:
    """Return a writable schema while preserving field metadata and order."""
    return pa.schema(
        [field.with_type(_storage_type(field.type)) for field in schema],
        metadata=schema.metadata,
    )


class PyArrowWriter(DataWriter):
    """Base writer for PyArrow-based formats (Parquet, ORC)."""

    def __init__(self, compression: str, **options: Any) -> None:
        """Initialize compression and format options."""
        self.compression = compression
        self.options = options

    @abstractmethod
    def _get_format_name(self) -> str:
        """Get the format name for error messages."""
        ...

    @abstractmethod
    def _write_table(
        self,
        table: Any,
        destination: str | BinaryIO | TextIO,
        compression: str,
        **options: Any,
    ) -> None:
        """Write an Arrow table to the destination."""
        ...

    def write(
        self,
        data: list[dict[str, Any]],
        destination: str | BinaryIO | TextIO,
        **options: Any,
    ) -> str | BinaryIO | TextIO:
        """Write inferred native or marked JSON columns without a supplied schema."""
        if not isinstance(data, list):
            raise TypeError(f"Expected list of records, got {type(data).__name__}")
        if not data:
            return destination
        try:
            writer_options = {**self.options, **options}
            compression = writer_options.pop("compression", self.compression)
            table = _records_to_arrow_table(data)
            table = table.cast(_storage_schema(table.schema))
            if isinstance(destination, (str, pathlib.Path)):
                path = pathlib.Path(destination)
                path.parent.mkdir(parents=True, exist_ok=True)
                self._write_table(table, str(path), compression, **writer_options)
                return str(path) if isinstance(destination, str) else path
            mode = getattr(destination, "mode", "")
            if mode and "b" not in mode:
                raise OutputError(
                    f"{self._get_format_name()} format requires binary streams, "
                    "text streams not supported"
                )
            self._write_table(table, destination, compression, **writer_options)
            return destination
        except _ARROW_WRITE_ERRORS as exc:
            raise OutputError(
                f"Failed to write {self._get_format_name()} file: {exc}"
            ) from exc


class PyArrowStreamingWriter(StreamingWriter):
    """Base streaming writer using the same inference as batch output."""

    def __init__(
        self,
        destination: str | None = None,
        entity_name: str = "entity",
        compression: str = "snappy",
        **options: Any,
    ) -> None:
        """Initialize the part-file writer and format options."""
        super().__init__(destination, entity_name, **options)
        self.compression = compression

    @abstractmethod
    def _get_format_name(self) -> str:
        """Get the format name."""
        ...

    @abstractmethod
    def _create_format_writer(self, file_path: str, schema: Any) -> Any:
        """Create the format-specific writer."""
        ...

    @abstractmethod
    def _write_to_format_writer(self, writer: Any, table: Any) -> None:
        """Write a table using the format-specific writer."""
        ...

    def _write_part(self, file_path: str, records: list[dict[str, Any]]) -> Any:
        """Write a part, retaining its logical schema for future promotion."""
        try:
            table = _records_to_arrow_table(records)
            schema = table.schema
            table = table.cast(_storage_schema(schema))
        except (OverflowError, pa.lib.ArrowException) as exc:
            raise OutputError(
                f"Invalid {self._get_format_name()} column values: {exc}"
            ) from exc
        writer = self._create_format_writer(file_path, table.schema)
        try:
            self._write_to_format_writer(writer, table)
        finally:
            writer.close()
        return schema

    def _schema_fields(self, schema: Any) -> list[tuple[str, str]]:
        """Report physical types and encoding changes for deviation tracking."""
        return [
            (field.name, str(field.type) + (" (json)" if _is_json_field(field) else ""))
            for field in _storage_schema(schema)
        ]

    def _build_unified_schema(self, schemas: list[Any]) -> Any:
        """Infer each unified field, using JSON when native types cannot be merged."""
        names = dict.fromkeys(field.name for schema in schemas for field in schema)
        fields = []
        for name in names:
            candidates = [
                schema.field(name) for schema in schemas if name in schema.names
            ]
            if any(_is_json_field(field) for field in candidates):
                field = _json_field(candidates[0])
            else:
                try:
                    merged = pa.unify_schemas(
                        [pa.schema([candidate]) for candidate in candidates],
                        promote_options="permissive",
                    )
                    field = merged.field(name)
                except _NATIVE_TYPE_ERRORS:
                    field = _json_field(candidates[0])
            fields.append(field)
        return pa.schema(fields)

    def _unified_part_schema(self, table_name: str) -> Any:
        """Check value-dependent casts before opening or rewriting final files.

        Numeric types can be compatible while particular integers exceed the
        exact range of a double. Such columns also need automatic JSON storage.
        Only parts requiring native casts are read during this check.
        """
        schema = super()._unified_part_schema(table_name)
        base_dir = self.base_dir
        if base_dir is None:
            raise ConfigurationError(
                "Cannot resolve part values without a destination directory."
            )
        for filename, part_schema in self._part_schemas[table_name]:
            candidates = [
                field
                for field in schema
                if not _is_json_field(field)
                and field.name in part_schema.names
                and not pa.types.is_null(part_schema.field(field.name).type)
                and field.type != part_schema.field(field.name).type
            ]
            if not candidates:
                continue
            table = self._read_part_table(str(pathlib.Path(base_dir) / filename))
            for field in candidates:
                column = table[field.name]
                if not all(
                    _native_shape_is_lossless(value, field.type)
                    for value in column.to_pylist()
                ):
                    schema = schema.set(
                        schema.get_field_index(field.name), _json_field(field)
                    )
                    continue
                try:
                    _cast_native_column(column, _storage_type(field.type))
                except _NATIVE_TYPE_ERRORS:
                    schema = schema.set(
                        schema.get_field_index(field.name), _json_field(field)
                    )
        return schema

    def _promote_table_to_schema(self, table: Any, target_schema: Any) -> Any:
        """Fill fields, cast safely, and encode newly heterogeneous columns once."""
        storage_schema = _storage_schema(target_schema)
        columns = []
        for field in storage_schema:
            if field.name not in table.column_names or table[
                field.name
            ].null_count == len(table):
                column = pa.nulls(len(table), type=field.type)
            elif _is_json_field(field) and not _is_json_field(
                table.schema.field(field.name)
            ):
                column = _json_array(table[field.name].to_pylist())
            else:
                column = _cast_native_column(table[field.name], field.type)
            columns.append(column)
        return pa.table(columns, schema=storage_schema)

    @abstractmethod
    def _read_part_table(self, file_path: str) -> Any:
        """Read a part file as an Arrow table."""
        ...

    def _consolidate_parts(
        self, output_path: str, part_files: list[str], schema: Any
    ) -> None:
        """Merge parts using a promoted schema and one part in memory."""
        writer = self._create_format_writer(output_path, _storage_schema(schema))
        try:
            for part_file in part_files:
                table = self._read_part_table(part_file)
                table = self._promote_table_to_schema(table, schema)
                self._write_to_format_writer(writer, table)
        finally:
            writer.close()


__all__ = ["PyArrowWriter", "PyArrowStreamingWriter"]
