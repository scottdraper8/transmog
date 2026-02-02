"""Avro format writers using fastavro."""

import os
import pathlib
from typing import Any, BinaryIO, TextIO

from transmog.exceptions import OutputError
from transmog.writers.base import (
    DataWriter,
    StreamingWriter,
    _collect_field_names,
    _normalize_special_floats,
    _sanitize_filename,
)

try:
    import fastavro
    from fastavro import writer as avro_writer

    AVRO_AVAILABLE = True
except ImportError:
    fastavro = None  # type: ignore[assignment]
    avro_writer = None  # type: ignore[assignment]
    AVRO_AVAILABLE = False

# Supported Avro compression codecs
AVRO_CODECS = ("null", "deflate", "snappy", "zstandard", "lz4", "bzip2", "xz")


def _python_type_to_avro(value: Any) -> str:
    """Map a Python value to its Avro type name.

    Args:
        value: Python value to map

    Returns:
        Avro type name string
    """
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
    # Default to string for all other types
    return "string"


def _infer_avro_schema(
    records: list[dict[str, Any]], name: str = "Record"
) -> dict[str, Any]:
    """Infer an Avro schema from a list of records.

    Scans all records to determine field types. Fields that have multiple
    types or contain null values are represented as union types.

    Args:
        records: List of records to infer schema from
        name: Name for the Avro record type

    Returns:
        Avro schema dictionary
    """
    if not records:
        return {
            "type": "record",
            "name": name,
            "fields": [],
        }

    field_names = _collect_field_names(records)
    field_types: dict[str, set[str]] = {field: set() for field in field_names}
    field_has_null: dict[str, bool] = dict.fromkeys(field_names, False)

    for record in records:
        for field in field_names:
            if field not in record or record[field] is None:
                field_has_null[field] = True
            else:
                value = _normalize_special_floats(record[field])
                if value is None:
                    field_has_null[field] = True
                else:
                    avro_type = _python_type_to_avro(value)
                    field_types[field].add(avro_type)

    fields = []
    for field in field_names:
        types = field_types[field]
        has_null = field_has_null[field]

        if not types:
            # Field only has null values
            field_type: Any = ["null", "string"]
        elif len(types) == 1:
            base_type = next(iter(types))
            if has_null:
                field_type = ["null", base_type]
            else:
                field_type = base_type
        else:
            # Multiple types detected - use union with null
            type_list = sorted(types)
            if has_null or "null" not in type_list:
                field_type = ["null"] + type_list
            else:
                field_type = type_list

        fields.append({"name": field, "type": field_type})

    return {
        "type": "record",
        "name": name,
        "fields": fields,
    }


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize a record for Avro output.

    Handles special float values and ensures type consistency.

    Args:
        record: Dictionary record to normalize

    Returns:
        New dictionary with normalized values
    """
    return {
        key: _normalize_special_floats(value, null_replacement=None)
        for key, value in record.items()
    }


def _coerce_value_to_schema(value: Any, field_type: Any) -> Any:
    """Coerce a value to match the expected Avro schema type.

    Args:
        value: Value to coerce
        field_type: Avro field type (string or list for union)

    Returns:
        Coerced value
    """
    normalized = _normalize_special_floats(value, null_replacement=None)

    if normalized is None:
        return None

    # Handle union types
    if isinstance(field_type, list):
        # Find the first non-null type in the union
        non_null_types = [t for t in field_type if t != "null"]
        if non_null_types:
            target_type = non_null_types[0]
        else:
            return None
    else:
        target_type = field_type

    # Coerce to target type
    if target_type == "string":
        return str(normalized)
    elif target_type == "long":
        try:
            return int(normalized)
        except (ValueError, TypeError):
            return None
    elif target_type == "double":
        try:
            return float(normalized)
        except (ValueError, TypeError):
            return None
    elif target_type == "boolean":
        if isinstance(normalized, bool):
            return normalized
        if isinstance(normalized, str):
            return normalized.lower() in ("true", "1", "yes")
        return bool(normalized)
    elif target_type == "bytes":
        if isinstance(normalized, bytes):
            return normalized
        return str(normalized).encode("utf-8")

    return normalized


def _prepare_record_for_schema(
    record: dict[str, Any], schema: dict[str, Any]
) -> dict[str, Any]:
    """Prepare a record to match the Avro schema.

    Ensures all schema fields are present and values are coerced to
    the correct types.

    Args:
        record: Record to prepare
        schema: Avro schema dictionary

    Returns:
        Prepared record matching the schema
    """
    field_types = {f["name"]: f["type"] for f in schema["fields"]}
    prepared = {}

    for field_name, field_type in field_types.items():
        value = record.get(field_name)
        prepared[field_name] = _coerce_value_to_schema(value, field_type)

    return prepared


class AvroWriter(DataWriter):
    """Avro format writer using fastavro."""

    def __init__(
        self, codec: str = "snappy", sync_interval: int = 16000, **options: Any
    ) -> None:
        """Initialize the Avro writer.

        Args:
            codec: Compression codec (null, deflate, snappy, zstandard, lz4, bzip2, xz)
            sync_interval: Approximate size of sync blocks in bytes (default: 16000)
            **options: Additional Avro writer options
        """
        self.codec = codec
        self.sync_interval = sync_interval
        self.options = options

    def write(
        self,
        data: list[dict[str, Any]],
        destination: str | BinaryIO | TextIO,
        **options: Any,
    ) -> str | BinaryIO | TextIO:
        """Write data to an Avro file.

        Args:
            data: Data to write
            destination: Path or file-like object to write to
            **options: Format-specific options (codec, etc.)

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
        """
        if not AVRO_AVAILABLE:
            raise OutputError(
                "fastavro is required for Avro support. "
                "Install with: pip install fastavro"
            )

        try:
            codec = options.get("codec", self.codec)
            sync_interval = options.get("sync_interval", self.sync_interval)

            if codec not in AVRO_CODECS:
                raise OutputError(
                    f"Unsupported Avro codec: {codec}. "
                    f"Supported codecs: {', '.join(AVRO_CODECS)}"
                )

            if not data:
                return destination

            # Infer schema from data
            schema = _infer_avro_schema(data, name="Record")
            parsed_schema = fastavro.parse_schema(schema)

            # Prepare records to match schema
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

        except Exception as exc:
            if isinstance(exc, OutputError):
                raise
            raise OutputError(f"Failed to write Avro file: {exc}") from exc


class AvroStreamingWriter(StreamingWriter):
    """Streaming writer for Avro format using fastavro."""

    def __init__(
        self,
        destination: str | BinaryIO | TextIO | None = None,
        entity_name: str = "entity",
        codec: str = "snappy",
        sync_interval: int = 16000,
        **options: Any,
    ) -> None:
        """Initialize the Avro streaming writer.

        Args:
            destination: Output file path or directory
            entity_name: Name of the entity for output files
            codec: Compression codec (null, deflate, snappy, zstandard, lz4, bzip2, xz)
            sync_interval: Approximate size of sync blocks in bytes
            **options: Additional Avro writer options
        """
        if not AVRO_AVAILABLE:
            raise OutputError(
                "fastavro is required for Avro streaming support. "
                "Install with: pip install fastavro"
            )

        super().__init__(destination, entity_name, **options)

        if codec not in AVRO_CODECS:
            raise OutputError(
                f"Unsupported Avro codec: {codec}. "
                f"Supported codecs: {', '.join(AVRO_CODECS)}"
            )

        self.codec = codec
        self.sync_interval = sync_interval
        self.base_dir: str | None = None
        self.file_objects: dict[str, BinaryIO] = {}
        self.schemas: dict[str, dict[str, Any]] = {}
        self.schema_field_sets: dict[str, set[str]] = {}
        self.buffers: dict[str, list[dict[str, Any]]] = {}
        self.should_close_files: bool = False

        if isinstance(destination, str):
            if destination.endswith(".avro"):
                os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)
                self.file_objects["main"] = open(destination, "wb")
                self.should_close_files = True
            else:
                self.base_dir = destination
                os.makedirs(self.base_dir, exist_ok=True)
                self.should_close_files = True
        elif destination is not None:
            mode = getattr(destination, "mode", "")
            if mode and "b" not in mode:
                raise OutputError(
                    "Avro format requires binary streams, text streams not supported"
                )
            self.file_objects["main"] = destination  # type: ignore[assignment]
            self.should_close_files = False

    def _get_file_for_table(self, table_name: str) -> BinaryIO:
        """Get or create a binary file object for the given table.

        Args:
            table_name: Name of the table

        Returns:
            Binary file object for writing
        """
        if table_name in self.file_objects:
            return self.file_objects[table_name]

        if self.base_dir:
            if table_name == "main":
                filename = self.entity_name
            else:
                filename = _sanitize_filename(table_name)

            file_path = os.path.join(self.base_dir, f"{filename}.avro")
            file_obj = open(file_path, "wb")

            self.file_objects[table_name] = file_obj
            return file_obj
        else:
            raise OutputError(f"Cannot create file for table {table_name}")

    def _ensure_schema(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Ensure schema is initialized for a table.

        Args:
            table_name: Name of the table
            records: Records to infer schema from

        Returns:
            Avro schema dictionary

        Raises:
            OutputError: If schema drift is detected
        """
        if table_name in self.schemas:
            return self.schemas[table_name]

        # Generate a valid Avro record name from table name
        record_name = table_name.replace(".", "_").replace("-", "_")
        if record_name == "main":
            record_name = self.entity_name.replace(".", "_").replace("-", "_")

        # Ensure name starts with letter or underscore
        if record_name and not record_name[0].isalpha() and record_name[0] != "_":
            record_name = f"_{record_name}"

        schema = _infer_avro_schema(records, name=record_name or "Record")
        self.schemas[table_name] = schema
        self.schema_field_sets[table_name] = {f["name"] for f in schema["fields"]}

        return schema

    def _check_schema_drift(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> None:
        """Check for schema drift in records.

        Args:
            table_name: Name of the table
            records: Records to check

        Raises:
            OutputError: If new fields are detected after schema initialization
        """
        if table_name not in self.schema_field_sets:
            return

        allowed_fields = self.schema_field_sets[table_name]

        for record in records:
            record_fields = set(record.keys())
            unexpected_fields = record_fields - allowed_fields
            if unexpected_fields:
                raise OutputError(
                    "Avro schema changed after initialization; "
                    f"unexpected fields {sorted(unexpected_fields)} detected "
                    f"in table '{table_name}'."
                )

    def _write_records(self, table_name: str, records: list[dict[str, Any]]) -> None:
        """Write records to the specified table.

        Args:
            table_name: Name of the table
            records: Records to write
        """
        if not records:
            return

        normalized_records = [_normalize_record(r) for r in records]

        # Check for schema drift if schema already exists
        self._check_schema_drift(table_name, normalized_records)

        # Initialize or get schema
        schema = self._ensure_schema(table_name, normalized_records)
        parsed_schema = fastavro.parse_schema(schema)

        # Prepare records to match schema
        prepared_records = [
            _prepare_record_for_schema(r, schema) for r in normalized_records
        ]

        # Get file and write
        file_obj = self._get_file_for_table(table_name)

        # Check if file is empty (new file) or has existing content
        current_pos = file_obj.tell()
        if current_pos == 0:
            # New file - write with header and add to buffer
            avro_writer(
                file_obj,
                parsed_schema,
                prepared_records,
                codec=self.codec,
                sync_interval=self.sync_interval,
            )
            # Buffer these records for rewrite on close
            if table_name not in self.buffers:
                self.buffers[table_name] = []
            self.buffers[table_name].extend(prepared_records)
        else:
            # Existing file - buffer records for rewrite on close
            if table_name not in self.buffers:
                self.buffers[table_name] = []
            self.buffers[table_name].extend(prepared_records)

    def write_main_records(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of main records.

        Args:
            records: List of main table records to write
        """
        self._write_records("main", records)

    def write_child_records(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: List of child records to write
        """
        self._write_records(table_name, records)

    def close(self) -> None:
        """Finalize output, flush buffered data, and clean up resources."""
        if getattr(self, "_closed", False):
            return

        # Write any buffered records
        for table_name, buffered_records in self.buffers.items():
            if buffered_records and table_name in self.schemas:
                schema = self.schemas[table_name]
                parsed_schema = fastavro.parse_schema(schema)
                file_obj = self.file_objects.get(table_name)

                if file_obj:
                    # Seek to beginning and rewrite entire file
                    file_obj.seek(0)
                    file_obj.truncate()
                    avro_writer(
                        file_obj,
                        parsed_schema,
                        buffered_records,
                        codec=self.codec,
                        sync_interval=self.sync_interval,
                    )

        # Flush and close files
        for file_obj in self.file_objects.values():
            if hasattr(file_obj, "flush"):
                file_obj.flush()

        if self.should_close_files:
            for file_obj in self.file_objects.values():
                if hasattr(file_obj, "close"):
                    file_obj.close()

        self.file_objects.clear()
        self.schemas.clear()
        self.schema_field_sets.clear()
        self.buffers.clear()
        self._closed = True


__all__ = ["AvroWriter", "AvroStreamingWriter", "AVRO_AVAILABLE"]
