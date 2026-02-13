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
    from fastavro.schema import SchemaParseException as _AvroSchemaError

    AVRO_AVAILABLE = True
    _AVRO_WRITE_ERRORS: tuple[type[Exception], ...] = (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        _AvroSchemaError,
    )
except ImportError:
    fastavro = None  # type: ignore[assignment]
    avro_writer = None  # type: ignore[assignment]
    AVRO_AVAILABLE = False
    _AVRO_WRITE_ERRORS: tuple[type[Exception], ...] = (OSError,)  # type: ignore[no-redef]

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
    field_found_float: dict[str, bool] = dict.fromkeys(field_names, False)

    for record in records:
        for field in field_names:
            if field not in record or record[field] is None:
                field_has_null[field] = True
            else:
                raw_value = record[field]
                # Track if field contains float values before normalization
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
            # Field only has null values - check if they were originally floats
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

    For union types, attempts to match the value to each type in the union
    in order, returning the first successful coercion. This prevents silent
    data loss when a value doesn't match the first non-null type.

    Args:
        value: Value to coerce
        field_type: Avro field type (string or list for union)

    Returns:
        Coerced value, or None if no type in the union can handle it
    """
    normalized = _normalize_special_floats(value, null_replacement=None)

    if normalized is None:
        return None

    # Handle union types - try each type in order
    if isinstance(field_type, list):
        non_null_types = [t for t in field_type if t != "null"]
        if not non_null_types:
            return None

        # Try to coerce to each type in the union
        for target_type in non_null_types:
            result = _try_coerce_to_type(normalized, target_type)
            if result is not None or (result is None and target_type == "null"):
                return result

        # If no type succeeded, return None
        return None
    else:
        # Single type
        return _try_coerce_to_type(normalized, field_type)


def _try_coerce_to_type(value: Any, target_type: str) -> Any:
    """Try to coerce a value to a specific Avro type.

    Args:
        value: Value to coerce
        target_type: Avro type name

    Returns:
        Coerced value if successful, None if coercion fails
    """
    if target_type == "string":
        return str(value)
    elif target_type == "long":
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    elif target_type == "double":
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    elif target_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes")
        return bool(value)
    elif target_type == "bytes":
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")

    return value


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

    def __init__(self, codec: str = "snappy", sync_interval: int = 16000) -> None:
        """Initialize the Avro writer.

        Args:
            codec: Compression codec (null, deflate, snappy, zstandard, lz4, bzip2, xz)
            sync_interval: Approximate size of sync blocks in bytes (default: 16000)
        """
        self.codec = codec
        self.sync_interval = sync_interval

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

        except _AVRO_WRITE_ERRORS as exc:
            raise OutputError(f"Failed to write Avro file: {exc}") from exc


class AvroStreamingWriter(StreamingWriter):
    """Streaming writer for Avro format using fastavro.

    Uses fastavro's append mode (a+b) to write records incrementally,
    avoiding memory accumulation of all records until close().
    """

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
        self.single_file_path: str | None = None
        self.file_paths: dict[str, str] = {}
        self.schemas: dict[str, dict[str, Any]] = {}
        self.schema_field_sets: dict[str, set[str]] = {}
        self.initialized_tables: set[str] = set()
        # For file-like object destinations (non-path based)
        self.file_object_dest: BinaryIO | None = None

        if isinstance(destination, str):
            if destination.endswith(".avro"):
                os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)
                self.single_file_path = destination
                self.file_paths["main"] = destination
            else:
                self.base_dir = destination
                os.makedirs(self.base_dir, exist_ok=True)
        elif destination is not None:
            mode = getattr(destination, "mode", "")
            if mode and "b" not in mode:
                raise OutputError(
                    "Avro format requires binary streams, text streams not supported"
                )
            self.file_object_dest = destination  # type: ignore[assignment]

    def _get_file_path_for_table(self, table_name: str) -> str | None:
        """Get the file path for a table.

        Args:
            table_name: Name of the table

        Returns:
            File path string, or None if using file-like object destination
        """
        if table_name in self.file_paths:
            return self.file_paths[table_name]

        if self.base_dir:
            if table_name == "main":
                filename = self.entity_name
            else:
                filename = _sanitize_filename(table_name)

            file_path = os.path.join(self.base_dir, f"{filename}.avro")
            self.file_paths[table_name] = file_path
            return file_path

        # For file-like object destinations, only "main" table is supported
        return None

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
        """Write records to the specified table using incremental streaming.

        Uses fastavro's append mode (a+b) for subsequent writes to avoid
        buffering all records in memory.

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

        # Handle file-like object destination (non-path based)
        if self.file_object_dest is not None and table_name == "main":
            # For file-like objects, we must buffer since we can't reopen
            # This is a limitation when using file-like objects directly
            if table_name not in self.initialized_tables:
                # First write - write with schema
                avro_writer(
                    self.file_object_dest,
                    parsed_schema,
                    prepared_records,
                    codec=self.codec,
                    sync_interval=self.sync_interval,
                )
                self.initialized_tables.add(table_name)
            else:
                # Subsequent writes to file-like objects not supported for append
                raise OutputError(
                    "Multiple batch writes to file-like object destinations "
                    "are not supported. Use a file path destination instead."
                )
            return

        if self.file_object_dest is not None and table_name != "main":
            raise OutputError(
                f"Cannot write child table '{table_name}' when using "
                "file-like object destination. Use a directory path instead."
            )

        # Get file path for this table
        file_path = self._get_file_path_for_table(table_name)
        if file_path is None:
            raise OutputError(f"Cannot determine file path for table {table_name}")

        if table_name not in self.initialized_tables:
            # First write - create new file with schema
            with open(file_path, "wb") as f:
                avro_writer(
                    f,
                    parsed_schema,
                    prepared_records,
                    codec=self.codec,
                    sync_interval=self.sync_interval,
                )
            self.initialized_tables.add(table_name)
        else:
            # Subsequent writes - append to existing file using a+b mode
            # fastavro reuses the schema from the existing file when schema=None
            with open(file_path, "a+b") as f:
                avro_writer(
                    f,
                    None,  # Schema is read from existing file
                    prepared_records,
                    codec=self.codec,
                    sync_interval=self.sync_interval,
                )

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
        """Finalize output and clean up resources.

        Since records are written incrementally using append mode,
        there is no buffered data to flush at close time.
        """
        if getattr(self, "_closed", False):
            return

        # Flush file-like object destination if present
        if self.file_object_dest is not None:
            if hasattr(self.file_object_dest, "flush"):
                self.file_object_dest.flush()

        # Clear metadata
        self.file_paths.clear()
        self.schemas.clear()
        self.schema_field_sets.clear()
        self.initialized_tables.clear()
        self._closed = True


__all__ = ["AvroWriter", "AvroStreamingWriter", "AVRO_AVAILABLE"]
