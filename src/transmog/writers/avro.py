"""Avro format writers using fastavro."""

import json
import pathlib
from typing import Any, BinaryIO, TextIO

import fastavro
from fastavro import writer as avro_writer
from fastavro.schema import SchemaParseException as _AvroSchemaError

from transmog.exceptions import OutputError
from transmog.writers.base import (
    DataWriter,
    StreamingWriter,
    _collect_field_names,
    _normalize_special_floats,
)

_AVRO_WRITE_ERRORS: tuple[type[Exception], ...] = (
    OSError,
    OverflowError,
    ValueError,
    TypeError,
    KeyError,
    _AvroSchemaError,
)

# Supported Avro compression codecs
AVRO_CODECS = ("null", "deflate", "snappy", "zstandard", "lz4", "bzip2", "xz")


def _python_type_to_avro(value: Any) -> str:
    """Map a Python value to its Avro type name."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, bytes):
        return "bytes"
    return "string"


def _infer_avro_schema(
    records: list[dict[str, Any]], name: str = "Record"
) -> dict[str, Any]:
    """Infer an Avro schema from a list of records.

    Scans all records to determine field types. Fields that have multiple
    types or contain null values are represented as union types.
    """
    if not records:
        return {"type": "record", "name": name, "fields": []}

    field_names = _collect_field_names(records)
    field_types: dict[str, set[str]] = {field: set() for field in field_names}
    field_has_null: dict[str, bool] = dict.fromkeys(field_names, False)
    field_found_float: dict[str, bool] = dict.fromkeys(field_names, False)

    for record in records:
        for field in field_names:
            if field not in record or record[field] is None:
                field_has_null[field] = True
            else:
                raw_value = record[field]
                if isinstance(raw_value, float):
                    field_found_float[field] = True

                value = _normalize_special_floats(raw_value)
                if value is None:
                    field_has_null[field] = True
                else:
                    avro_type = _python_type_to_avro(value)
                    field_types[field].add(avro_type)

    fields = []
    for field in field_names:
        types = field_types[field]
        has_null = field_has_null[field]
        found_float = field_found_float[field]

        if not types:
            if found_float:
                field_type: Any = ["null", "double"]
            else:
                field_type = ["null", "string"]
        elif len(types) == 1:
            base_type = next(iter(types))
            if has_null:
                field_type = ["null", base_type]
            else:
                field_type = base_type
        else:
            type_list = sorted(types)
            field_type = ["null"] + type_list

        fields.append({"name": field, "type": field_type})

    return {"type": "record", "name": name, "fields": fields}


def _normalize_record(record: object) -> dict[str, Any]:
    """Normalize a record for Avro output.

    - NaN/Inf → None
    - list/dict → JSON string (parseable, cross-language)
    """
    if not isinstance(record, dict):
        raise OutputError(f"Expected an Avro record, got {type(record).__name__}")
    prepared: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, (list, dict)):
            prepared[key] = json.dumps(
                _normalize_special_floats(value), ensure_ascii=False, allow_nan=False
            )
        else:
            prepared[key] = _normalize_special_floats(value, null_replacement=None)
    return prepared


def _coerce_value_to_schema(value: Any, field_type: Any) -> Any:
    """Preserve native values and select their exact Avro union branch.

    Explicit branch labels prevent fastavro from choosing double before long
    or interpreting a boolean as a number. Schema mismatches are errors, not
    invitations to truncate numbers, parse numeric strings, or discard values.
    """
    normalized = _normalize_special_floats(value)
    types = field_type if isinstance(field_type, list) else [field_type]
    native_type = _python_type_to_avro(normalized)
    if native_type not in types:
        raise OutputError(
            f"Value {normalized!r} of type {native_type} does not match {field_type}"
        )
    if normalized is None or not isinstance(field_type, list):
        return normalized
    return (native_type, normalized)


def _prepare_record_for_schema(
    record: dict[str, Any], schema: dict[str, Any]
) -> dict[str, Any]:
    """Prepare a record to match the Avro schema.

    Fills missing fields and validates native types without lossy coercion.
    """
    field_types = {f["name"]: f["type"] for f in schema["fields"]}
    prepared = {}

    for field_name, field_type in field_types.items():
        value = record.get(field_name)
        prepared[field_name] = _coerce_value_to_schema(value, field_type)

    return prepared


class AvroWriter(DataWriter):
    """Avro format writer using fastavro."""

    def __init__(self, compression: str = "snappy", sync_interval: int = 16000) -> None:
        """Initialize the Avro writer.

        Args:
            compression: Compression codec
                (null, deflate, snappy, zstandard, lz4, bzip2, xz)
            sync_interval: Approximate size of sync blocks in bytes (default: 16000)
        """
        self.codec = compression
        self.sync_interval = sync_interval

    def write(
        self,
        data: list[dict[str, Any]],
        destination: str | BinaryIO | TextIO,
        **options: Any,
    ) -> str | BinaryIO | TextIO:
        """Write data to an Avro file."""
        try:
            codec = options.get("compression", options.get("codec", self.codec))
            sync_interval = options.get("sync_interval", self.sync_interval)

            if codec not in AVRO_CODECS:
                raise OutputError(
                    f"Unsupported Avro codec: {codec}. "
                    f"Supported codecs: {', '.join(AVRO_CODECS)}"
                )

            if not data:
                return destination

            schema = _infer_avro_schema(data, name="Record")
            parsed_schema = fastavro.parse_schema(schema)

            prepared_records = [
                _prepare_record_for_schema(_normalize_record(r), schema) for r in data
            ]

            if isinstance(destination, (str, pathlib.Path)):
                path = pathlib.Path(destination)
                path.parent.mkdir(parents=True, exist_ok=True)

                with open(path, "wb") as f:
                    avro_writer(
                        f,
                        parsed_schema,
                        prepared_records,
                        codec=codec,
                        sync_interval=sync_interval,
                    )

                return str(path) if isinstance(destination, str) else path

            elif hasattr(destination, "write"):
                mode = getattr(destination, "mode", "")
                if mode and "b" not in mode:
                    raise OutputError(
                        "Avro format requires binary streams, "
                        "text streams not supported"
                    )

                avro_writer(
                    destination,
                    parsed_schema,
                    prepared_records,
                    codec=codec,
                    sync_interval=sync_interval,
                )
                return destination
            else:
                raise OutputError(f"Invalid destination type: {type(destination)}")

        except _AVRO_WRITE_ERRORS as exc:
            raise OutputError(f"Failed to write Avro file: {exc}") from exc


class AvroStreamingWriter(StreamingWriter):
    """Streaming writer for Avro format using fastavro."""

    def __init__(
        self,
        destination: str | None = None,
        entity_name: str = "entity",
        compression: str = "snappy",
        sync_interval: int = 16000,
        codec: str | None = None,
        **options: Any,
    ) -> None:
        """Initialize the Avro streaming writer.

        Args:
            destination: Output directory path
            entity_name: Name of the entity for output files
            compression: Compression codec
                (null, deflate, snappy, zstandard, lz4, bzip2, xz)
            sync_interval: Approximate size of sync blocks in bytes
            codec: Deprecated alias for compression
            **options: Additional Avro writer options
        """
        if codec is not None:
            compression = codec

        super().__init__(destination, entity_name, **options)

        if compression not in AVRO_CODECS:
            raise OutputError(
                f"Unsupported Avro codec: {compression}. "
                f"Supported codecs: {', '.join(AVRO_CODECS)}"
            )

        self.codec = compression
        self.sync_interval = sync_interval
        self._current_table_name: str = "main"

    def _get_record_name(self, table_name: str) -> str:
        """Generate a valid Avro record name from a table name."""
        record_name = table_name.replace(".", "_").replace("-", "_")
        if record_name == "main":
            record_name = self.entity_name.replace(".", "_").replace("-", "_")
        if record_name and not record_name[0].isalpha() and record_name[0] != "_":
            record_name = f"_{record_name}"
        return record_name or "Record"

    # --- StreamingWriter abstract method implementations ---

    def _get_file_extension(self) -> str:
        return ".avro"

    def _write_part(self, file_path: str, records: list[dict[str, Any]]) -> Any:
        """Write records to an Avro part file and return the schema."""
        record_name = self._get_record_name(self._current_table_name)

        normalized = [_normalize_record(r) for r in records]
        schema = _infer_avro_schema(records, name=record_name)
        parsed_schema = fastavro.parse_schema(schema)

        prepared_records = [_prepare_record_for_schema(r, schema) for r in normalized]

        with open(file_path, "wb") as f:
            avro_writer(
                f,
                parsed_schema,
                prepared_records,
                codec=self.codec,
                sync_interval=self.sync_interval,
            )

        return schema

    def _write_buffer(self, table_name: str) -> None:
        """Override to pass table_name context to _write_part."""
        self._current_table_name = table_name
        super()._write_buffer(table_name)

    def _schema_fields(self, schema: Any) -> list[tuple[str, str]]:
        """Extract (name, type_string) pairs from an Avro schema."""
        return [(f["name"], str(f["type"])) for f in schema["fields"]]

    def _build_unified_schema(self, schemas: list[Any]) -> Any:
        """Build a unified Avro schema from all part schemas."""
        all_field_types: dict[str, set[str]] = {}
        for schema in schemas:
            for field in schema["fields"]:
                types = field["type"]
                types = types if isinstance(types, list) else [types]
                all_field_types.setdefault(field["name"], set()).update(types)

        record_name = schemas[0]["name"] if schemas else "Record"
        target_fields = [
            {"name": name, "type": ["null", *sorted(types - {"null"})]}
            for name, types in all_field_types.items()
        ]

        return {"type": "record", "name": record_name, "fields": target_fields}

    def _consolidate_parts(
        self, output_path: str, part_files: list[str], schema: Any
    ) -> None:
        """Merge multiple Avro part files into a single file.

        Reads one part file at a time so peak memory is bounded by the
        largest single part rather than the entire dataset.
        """
        from fastavro.write import Writer

        parsed_schema = fastavro.parse_schema(schema)
        with open(output_path, "wb") as out_f:
            avro_out = Writer(
                out_f,
                parsed_schema,
                codec=self.codec,
                sync_interval=self.sync_interval,
            )
            for part_file in part_files:
                with open(part_file, "rb") as in_f:
                    for record in fastavro.reader(in_f):
                        prepared = _prepare_record_for_schema(
                            _normalize_record(record), schema
                        )
                        avro_out.write(prepared)
            avro_out.flush()

    def _rewrite_part(self, file_path: str, target_schema: Any) -> None:
        """Rewrite an Avro part file with a target unified schema."""
        with open(file_path, "rb") as f:
            records = list(fastavro.reader(f))

        parsed_target = fastavro.parse_schema(target_schema)
        prepared = [
            _prepare_record_for_schema(_normalize_record(r), target_schema)
            for r in records
        ]
        with open(file_path, "wb") as f:
            avro_writer(
                f,
                parsed_target,
                prepared,
                codec=self.codec,
                sync_interval=self.sync_interval,
            )


__all__ = ["AvroWriter", "AvroStreamingWriter"]
