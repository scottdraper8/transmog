"""CSV format writers."""

import csv
import io
import os
import pathlib
import sys
from typing import Any, BinaryIO, TextIO, cast

from transmog.exceptions import ConfigurationError, OutputError
from transmog.writers.base import (
    DataWriter,
    StreamingWriter,
    _collect_field_names,
    _sanitize_filename,
)


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
    if not isinstance(value, str):
        return value

    if not value:
        return value

    # Check first character and first non-whitespace character
    # to prevent bypass with leading spaces
    stripped = value.lstrip()
    if not stripped:
        return value

    first_char = value[0]
    first_nonwhitespace = stripped[0]

    dangerous_chars = ("=", "+", "-", "@", "|", "\t", "\r")

    if first_char in dangerous_chars or first_nonwhitespace in dangerous_chars:
        return f"'{value}"

    return value


def _sanitize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Sanitize all values in a record to prevent CSV injection.

    Args:
        record: Dictionary record to sanitize

    Returns:
        New dictionary with sanitized values
    """
    return {key: _sanitize_csv_value(value) for key, value in record.items()}


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
        """Write data to a CSV file.

        Args:
            data: The data to write
            destination: Path or file-like object to write to
            **options: Format-specific options (include_header, delimiter, etc.)

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
        """
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
        except Exception as exc:
            if isinstance(exc, OutputError):
                raise
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
        """Write CSV data directly to a stream.

        Args:
            stream: Text stream to write to
            table_data: The table data
            field_names: List of field names
            include_header: Whether to include headers
            delimiter: Column delimiter
            quotechar: Quote character
            quoting: Quoting mode
            escapechar: Escape character
        """
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
    """Streaming writer for CSV format."""

    def __init__(
        self,
        destination: str | BinaryIO | TextIO | None = None,
        entity_name: str = "entity",
        include_header: bool = True,
        delimiter: str = ",",
        quotechar: str = '"',
        **options: Any,
    ):
        """Initialize the CSV streaming writer.

        Args:
            destination: Output file path or file-like object
            entity_name: Name of the entity
            include_header: Whether to include column headers
            delimiter: Column delimiter character
            quotechar: Character to use for quoting
            **options: Additional CSV writer options
        """
        self.include_header = include_header
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.file_objects: dict[str, TextIO] = {}
        self.writers: dict[str, csv.DictWriter] = {}
        self.fieldnames: dict[str, list[str]] = {}
        self.fieldname_sets: dict[str, set[str]] = {}
        self.should_close_files: bool = False
        self.base_dir: str | None = None

        if "compression" in options:
            raise ConfigurationError(
                "CSV streaming writer does not support compression"
            )

        super().__init__(destination, entity_name, **options)

        if destination is None:
            self.file_objects["main"] = cast(TextIO, sys.stdout)
            self.should_close_files = False
        elif isinstance(destination, str):
            if destination.endswith(".csv"):
                os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)

                file_obj = open(destination, "w", encoding="utf-8", newline="")

                self.file_objects["main"] = file_obj
                self.should_close_files = True
            else:
                self.base_dir = destination
                os.makedirs(self.base_dir, exist_ok=True)
                self.should_close_files = True
        else:
            mode = getattr(destination, "mode", "")
            is_binary = "b" in mode if mode else hasattr(destination, "readinto")

            if is_binary:
                text_dest = io.TextIOWrapper(
                    cast(BinaryIO, destination), encoding="utf-8"
                )
                self.file_objects["main"] = text_dest
            else:
                self.file_objects["main"] = cast(TextIO, destination)
            self.should_close_files = False

    def _get_file_for_table(self, table_name: str) -> TextIO:
        """Get or create a text file object for the given table.

        Args:
            table_name: Name of the table

        Returns:
            Text file object for writing
        """
        if table_name in self.file_objects:
            return self.file_objects[table_name]

        if self.base_dir:
            if table_name == "main":
                filename = self.entity_name
            else:
                filename = _sanitize_filename(table_name)

            file_path = os.path.join(self.base_dir, f"{filename}.csv")
            file_obj = open(file_path, "w", encoding="utf-8", newline="")

            self.file_objects[table_name] = file_obj
            return file_obj
        else:
            raise OutputError(f"Cannot create file for table {table_name}")

    def _ensure_writer(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> csv.DictWriter:
        """Ensure writer and schema are initialized for a table.

        Args:
            table_name: Name of the table
            records: Records that will be written

        Returns:
            CSV writer for the table
        """
        if table_name in self.writers:
            return self.writers[table_name]

        fieldnames = _collect_field_names(records)
        file_obj = self._get_file_for_table(table_name)

        writer = csv.DictWriter(
            file_obj,
            fieldnames=fieldnames,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
        )

        self.writers[table_name] = writer
        self.fieldnames[table_name] = fieldnames
        self.fieldname_sets[table_name] = set(fieldnames)

        if self.include_header and fieldnames:
            writer.writeheader()

        return writer

    def _write_records(self, table_name: str, records: list[dict[str, Any]]) -> None:
        """Write records to the specified table in a single pass.

        Args:
            table_name: Name of the table
            records: Records to write
        """
        if not records:
            return

        sanitized_records = [_sanitize_record(record) for record in records]
        writer = self._ensure_writer(table_name, sanitized_records)
        allowed_fields = self.fieldname_sets.get(table_name, set())

        for record in sanitized_records:
            unexpected_fields = set(record.keys()) - allowed_fields
            if unexpected_fields:
                raise OutputError(
                    "CSV schema changed after header emission; "
                    f"unexpected fields {sorted(unexpected_fields)} detected "
                    f"in table '{table_name}'."
                )
            writer.writerow(record)

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

        for file_obj in self.file_objects.values():
            if hasattr(file_obj, "flush"):
                file_obj.flush()

        if self.should_close_files:
            for file_obj in self.file_objects.values():
                if hasattr(file_obj, "close"):
                    file_obj.close()

        self.file_objects.clear()
        self.writers.clear()
        self.fieldnames.clear()
        self.fieldname_sets.clear()
        self._closed = True


__all__ = ["CsvWriter", "CsvStreamingWriter"]
