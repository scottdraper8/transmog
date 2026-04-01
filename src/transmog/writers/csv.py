"""CSV format writers."""

import csv
import io
import pathlib
from typing import Any, BinaryIO, TextIO, cast

from transmog.exceptions import ConfigurationError, OutputError
from transmog.writers.base import (
    DataWriter,
    StreamingWriter,
    _collect_field_names,
    _normalize_special_floats,
)

_DANGEROUS_CHARS = frozenset("=+-@|\t\r")


def _sanitize_csv_value(value: Any) -> Any:
    """Sanitize a value to prevent CSV injection attacks.

    Prefixes potentially dangerous strings with a single quote to prevent
    formula interpretation in spreadsheet applications. Handles both direct
    injection and bypass attempts using leading whitespace.

    Args:
        value: The value to sanitize

    Returns:
        Sanitized value safe for CSV output
    """
    if not isinstance(value, str) or not value:
        return value

    first_char = value[0]

    # Fast path: first char is not dangerous and not whitespace
    if first_char not in _DANGEROUS_CHARS and not first_char.isspace():
        return value

    # First char is dangerous
    if first_char in _DANGEROUS_CHARS:
        return f"'{value}"

    # First char is whitespace — check first non-whitespace char
    stripped = value.lstrip()
    if stripped and stripped[0] in _DANGEROUS_CHARS:
        return f"'{value}"

    return value


def _sanitize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Sanitize all values in a record for CSV output.

    Normalizes special float values (NaN, Inf) and prevents CSV injection.
    """
    return {
        key: _sanitize_csv_value(_normalize_special_floats(value, null_replacement=""))
        for key, value in record.items()
    }


class CsvWriter(DataWriter):
    """CSV format writer."""

    def __init__(
        self,
        include_header: bool = True,
        delimiter: str = ",",
        quotechar: str = '"',
        quoting: int = csv.QUOTE_MINIMAL,
        escapechar: str | None = None,
    ):
        """Initialize the CSV writer.

        Args:
            include_header: Whether to include column headers
            delimiter: Column delimiter character
            quotechar: Character to use for quoting
            quoting: Quoting mode (from csv module)
            escapechar: Character to use for escaping
        """
        self.include_header = include_header
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.quoting = quoting
        self.escapechar = escapechar

    def write(
        self,
        data: list[dict[str, Any]],
        destination: str | BinaryIO | TextIO,
        **options: Any,
    ) -> str | BinaryIO | TextIO:
        """Write data to a CSV file."""
        try:
            if "compression" in options:
                raise ConfigurationError(
                    "CSV writer does not support compression options"
                )

            use_header = options.get("include_header", self.include_header)
            use_delimiter = options.get("delimiter", self.delimiter)
            use_quotechar = options.get("quotechar", self.quotechar)
            use_quoting = options.get("quoting", self.quoting)
            use_escapechar = options.get("escapechar", self.escapechar)

            if not data:
                return destination

            sanitized_data = [_sanitize_record(record) for record in data]
            field_names = _collect_field_names(sanitized_data)

            if isinstance(destination, (str, pathlib.Path)):
                path = pathlib.Path(destination)
                path.parent.mkdir(parents=True, exist_ok=True)

                with open(path, "w", encoding="utf-8", newline="") as f:
                    self._write_csv_to_stream(
                        f,
                        sanitized_data,
                        field_names,
                        use_header,
                        use_delimiter,
                        use_quotechar,
                        use_quoting,
                        use_escapechar,
                    )

                return str(path) if isinstance(destination, str) else path

            elif hasattr(destination, "write"):
                mode = getattr(destination, "mode", "")
                is_binary = "b" in mode if mode else hasattr(destination, "readinto")

                if is_binary:
                    binary_output = cast(BinaryIO, destination)
                    text_wrapper = io.TextIOWrapper(
                        binary_output, encoding="utf-8", newline=""
                    )
                    self._write_csv_to_stream(
                        text_wrapper,
                        sanitized_data,
                        field_names,
                        use_header,
                        use_delimiter,
                        use_quotechar,
                        use_quoting,
                        use_escapechar,
                    )
                    text_wrapper.flush()
                    text_wrapper.detach()
                else:
                    text_output = cast(TextIO, destination)
                    self._write_csv_to_stream(
                        text_output,
                        sanitized_data,
                        field_names,
                        use_header,
                        use_delimiter,
                        use_quotechar,
                        use_quoting,
                        use_escapechar,
                    )

                return destination
            else:
                raise OutputError(f"Invalid destination type: {type(destination)}")
        except (OSError, csv.Error, ValueError) as exc:
            raise OutputError(f"Failed to write CSV file: {exc}") from exc

    def _write_csv_to_stream(
        self,
        stream: TextIO,
        table_data: list[dict[str, Any]],
        field_names: list[str],
        include_header: bool,
        delimiter: str,
        quotechar: str,
        quoting: int,
        escapechar: str | None,
    ) -> None:
        """Write CSV data directly to a stream."""
        writer_params: dict[str, Any] = {
            "fieldnames": field_names,
            "delimiter": delimiter,
            "quotechar": quotechar,
            "quoting": quoting,
        }

        if escapechar:
            writer_params["escapechar"] = escapechar

        writer: csv.DictWriter[str] = csv.DictWriter(stream, **writer_params)

        if include_header:
            writer.writeheader()

        writer.writerows(table_data)


class CsvStreamingWriter(StreamingWriter):
    """Streaming writer for CSV format.

    Writes each batch flush as a separate part file, enabling per-batch
    column discovery. A schema log tracks structural deviations (column
    presence) across parts.
    """

    def __init__(
        self,
        destination: str | None = None,
        entity_name: str = "entity",
        include_header: bool = True,
        delimiter: str = ",",
        quotechar: str = '"',
        **options: Any,
    ):
        """Initialize the CSV streaming writer.

        Args:
            destination: Output directory path
            entity_name: Name of the entity
            include_header: Whether to include column headers
            delimiter: Column delimiter character
            quotechar: Character to use for quoting
            **options: Additional CSV writer options
        """
        if "compression" in options:
            raise ConfigurationError(
                "CSV streaming writer does not support compression"
            )

        self.include_header = include_header
        self.delimiter = delimiter
        self.quotechar = quotechar

        super().__init__(destination, entity_name, **options)

    # --- StreamingWriter abstract method implementations ---

    def _get_file_extension(self) -> str:
        return ".csv"

    def _write_part(self, file_path: str, records: list[dict[str, Any]]) -> Any:
        """Write records to a CSV part file and return the field names."""
        sanitized_records = [_sanitize_record(record) for record in records]
        field_names = _collect_field_names(sanitized_records)

        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=field_names,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
            )
            if self.include_header and field_names:
                writer.writeheader()
            writer.writerows(sanitized_records)

        return field_names

    def _schema_fields(self, schema: Any) -> list[tuple[str, str]]:
        """Extract (name, type_string) pairs from a CSV schema (field name list)."""
        return [(name, "string") for name in schema]

    def _compute_deviations(self, base_schema: Any, part_schema: Any) -> dict | None:
        """Compute structural deviations between field name lists.

        Overrides the base to skip type comparison — CSV schemas have no types.
        """
        base_set = set(base_schema)
        part_set = set(part_schema)

        added = sorted(part_set - base_set)
        removed = sorted(base_set - part_set)

        if not added and not removed:
            return None

        return {"structural": {"added": added, "removed": removed}}

    def _build_unified_schema(self, schemas: list[Any]) -> Any:
        """Build a unified field name list from all part schemas."""
        unified: list[str] = []
        seen: set[str] = set()
        for fields in schemas:
            for f in fields:
                if f not in seen:
                    unified.append(f)
                    seen.add(f)
        return unified

    def _consolidate_parts(
        self, output_path: str, part_files: list[str], schema: Any
    ) -> None:
        """Merge multiple CSV part files into a single file."""
        unified_fields: list[str] = schema

        with open(output_path, "w", encoding="utf-8", newline="") as out:
            writer = csv.DictWriter(
                out,
                fieldnames=unified_fields,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
            )
            if self.include_header and unified_fields:
                writer.writeheader()

            for part_file in part_files:
                with open(part_file, encoding="utf-8", newline="") as inp:
                    reader = csv.DictReader(
                        inp, delimiter=self.delimiter, quotechar=self.quotechar
                    )
                    for row in reader:
                        writer.writerow({f: row.get(f, "") for f in unified_fields})

    def _rewrite_part(self, file_path: str, target_schema: Any) -> None:
        """Rewrite a CSV part file with a unified column set."""
        unified_fields: list[str] = target_schema

        with open(file_path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(
                f, delimiter=self.delimiter, quotechar=self.quotechar
            )
            rows = list(reader)

        for row in rows:
            for field in unified_fields:
                if field not in row:
                    row[field] = ""

        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=unified_fields,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
            )
            if self.include_header:
                writer.writeheader()
            writer.writerows(rows)


__all__ = ["CsvWriter", "CsvStreamingWriter"]
