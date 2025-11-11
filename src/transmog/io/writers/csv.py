"""CSV writer for Transmog output.

This module provides a CSV writer with PyArrow and standard library implementations
and unified interface with optional compression support.
"""

import csv
import gzip
import io
import logging
import os
import pathlib
import sys
from typing import Any, BinaryIO, Optional, TextIO, Union, cast

from transmog.error import OutputError
from transmog.io.writer_interface import DataWriter, StreamingWriter, sanitize_filename
from transmog.types import JsonDict

# Setup logger
logger = logging.getLogger(__name__)


class CsvWriter(DataWriter):
    """CSV format writer.

    This writer handles writing flattened data to CSV format files with
    consistent interface and optional compression support.
    """

    def __init__(
        self,
        include_header: bool = True,
        delimiter: str = ",",
        quotechar: str = '"',
        quoting: int = csv.QUOTE_MINIMAL,
        escapechar: Optional[str] = None,
        compression: Optional[str] = None,
        **options: Any,
    ):
        """Initialize the CSV writer.

        Args:
            include_header: Whether to include column headers
            delimiter: Column delimiter character
            quotechar: Character to use for quoting
            quoting: Quoting mode (from csv module)
            escapechar: Character to use for escaping
            compression: Compression method ("gzip" supported)
            **options: CSV formatting options
        """
        self.include_header = include_header
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.quoting = quoting
        self.escapechar = escapechar
        self.compression = compression
        self.options = options

    def write(
        self,
        data: list[JsonDict],
        destination: Union[str, BinaryIO, TextIO],
        **options: Any,
    ) -> Union[str, BinaryIO, TextIO]:
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
            use_header = options.get("include_header", self.include_header)
            use_delimiter = options.get("delimiter", self.delimiter)
            use_quotechar = options.get("quotechar", self.quotechar)
            use_quoting = options.get("quoting", self.quoting)
            use_escapechar = options.get("escapechar", self.escapechar)
            compression = options.get("compression", self.compression)

            if not data:
                if isinstance(destination, (str, pathlib.Path)):
                    path_str = str(destination)

                    # Add .gz extension for compressed files
                    if compression == "gzip" and not path_str.endswith(".gz"):
                        path_str += ".gz"

                    os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)

                    # Create empty file
                    if compression == "gzip":
                        with gzip.open(path_str, "wt", encoding="utf-8") as f:
                            pass
                    else:
                        with open(path_str, "w") as f:
                            pass

                    return (
                        pathlib.Path(path_str)
                        if isinstance(destination, pathlib.Path)
                        else path_str
                    )
                else:
                    return destination

            field_names_set: set[str] = set()
            for record in data:
                field_names_set.update(record.keys())
            field_names = sorted(field_names_set)

            if isinstance(destination, (str, pathlib.Path)):
                path_str = str(destination)

                # Add .gz extension for compressed files
                if compression == "gzip" and not path_str.endswith(".gz"):
                    path_str += ".gz"

                os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)

                if compression == "gzip":
                    with gzip.open(path_str, "wt", encoding="utf-8", newline="") as f:
                        self._write_csv_to_stream(
                            f,
                            data,
                            field_names,
                            use_header,
                            use_delimiter,
                            use_quotechar,
                            use_quoting,
                            use_escapechar,
                        )
                else:
                    with open(path_str, "w", encoding="utf-8", newline="") as f:
                        self._write_csv_to_stream(
                            f,
                            data,
                            field_names,
                            use_header,
                            use_delimiter,
                            use_quotechar,
                            use_quoting,
                            use_escapechar,
                        )

                return (
                    pathlib.Path(path_str)
                    if isinstance(destination, pathlib.Path)
                    else path_str
                )

            elif hasattr(destination, "write"):
                if compression == "gzip":
                    if hasattr(destination, "mode") and "b" not in getattr(
                        destination, "mode", ""
                    ):
                        raise OutputError("Cannot write compressed CSV to text stream")
                    binary_output = cast(BinaryIO, destination)
                    with gzip.open(
                        binary_output, "wt", encoding="utf-8", newline=""
                    ) as gz_file:
                        self._write_csv_to_stream(
                            gz_file,
                            data,
                            field_names,
                            use_header,
                            use_delimiter,
                            use_quotechar,
                            use_quoting,
                            use_escapechar,
                        )
                else:
                    if (
                        hasattr(destination, "mode")
                        and "b" not in getattr(destination, "mode", "")
                    ) or (
                        not hasattr(destination, "mode")
                        and hasattr(destination, "read")
                        and not hasattr(destination, "readinto")
                    ):
                        text_output = cast(TextIO, destination)
                        self._write_csv_to_stream(
                            text_output,
                            data,
                            field_names,
                            use_header,
                            use_delimiter,
                            use_quotechar,
                            use_quoting,
                            use_escapechar,
                        )
                    else:
                        binary_output = cast(BinaryIO, destination)
                        text_wrapper = io.TextIOWrapper(
                            binary_output, encoding="utf-8", newline=""
                        )
                        self._write_csv_to_stream(
                            text_wrapper,
                            data,
                            field_names,
                            use_header,
                            use_delimiter,
                            use_quotechar,
                            use_quoting,
                            use_escapechar,
                        )
                        text_wrapper.flush()
                        text_wrapper.detach()

                return destination
            else:
                raise OutputError(f"Invalid destination type: {type(destination)}")

        except Exception as e:
            logger.error(f"Error writing CSV: {e}")
            raise OutputError(f"Failed to write CSV file: {e}") from e

    def _write_csv_to_stream(
        self,
        stream: TextIO,
        table_data: list[JsonDict],
        field_names: list[str],
        include_header: bool,
        delimiter: str,
        quotechar: str,
        quoting: int,
        escapechar: Optional[str],
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
    """Streaming writer for CSV format.

    This writer allows incremental writing of data to CSV files
    without keeping the entire dataset in memory.
    """

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        include_header: bool = True,
        delimiter: str = ",",
        quotechar: str = '"',
        compression: Optional[str] = None,
        **options: Any,
    ):
        """Initialize the CSV streaming writer.

        Args:
            destination: Output file path or file-like object
            entity_name: Name of the entity
            include_header: Whether to include column headers
            delimiter: Column delimiter character
            quotechar: Character to use for quoting
            compression: Compression method ("gzip" supported)
            **options: Additional CSV writer options
        """
        super().__init__(destination, entity_name, **options)
        self.include_header = include_header
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.compression = compression
        self.file_objects: dict[str, TextIO] = {}
        self.writers: dict[str, csv.DictWriter] = {}
        self.fieldnames: dict[str, list[str]] = {}
        self.headers_written: set[str] = set()
        self.should_close_files: bool = False
        self.base_dir: Optional[str] = None

        # Initialize destination
        if destination is None:
            # Default to stdout
            self.file_objects["main"] = cast(TextIO, sys.stdout)
            self.should_close_files = False
        elif isinstance(destination, str):
            # Check if it's a single file path (has .csv extension) or directory
            if destination.endswith(".csv") or destination.endswith(".csv.gz"):
                # Single file destination
                os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)

                # Open file in appropriate mode
                if self.compression == "gzip" or destination.endswith(".csv.gz"):
                    file_obj = gzip.open(
                        destination, "wt", encoding="utf-8", newline=""
                    )
                else:
                    file_obj = open(destination, "w", encoding="utf-8", newline="")

                self.file_objects["main"] = file_obj
                self.should_close_files = True
            else:
                # Directory path
                self.base_dir = destination
                os.makedirs(self.base_dir, exist_ok=True)
                self.should_close_files = True
        else:
            # File-like object - ensure it's text mode for CSV
            if hasattr(destination, "mode") and "b" in getattr(destination, "mode", ""):
                # Binary stream - wrap in TextIOWrapper
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
            TextIO: Text file object for writing
        """
        if table_name in self.file_objects:
            return self.file_objects[table_name]

        # Create new file for table
        if self.base_dir:
            if table_name == "main":
                filename = self.entity_name
            else:
                filename = sanitize_filename(table_name)

            ext = ".csv.gz" if self.compression == "gzip" else ".csv"
            file_path = os.path.join(self.base_dir, f"{filename}{ext}")

            # Open file with appropriate compression
            if self.compression == "gzip":
                file_obj = gzip.open(file_path, "wt", encoding="utf-8", newline="")
            else:
                file_obj = open(file_path, "w", encoding="utf-8", newline="")

            self.file_objects[table_name] = file_obj
            return file_obj
        else:
            raise OutputError(f"Cannot create file for table {table_name}")

    def _get_writer_for_table(
        self, table_name: str, fieldnames: list[str]
    ) -> csv.DictWriter:
        """Get or create a CSV writer for the given table.

        Args:
            table_name: Name of the table
            fieldnames: List of field names for the CSV

        Returns:
            csv.DictWriter: CSV writer for the table
        """
        if table_name in self.writers:
            return self.writers[table_name]

        file_obj = self._get_file_for_table(table_name)

        # Create CSV writer
        writer = csv.DictWriter(
            file_obj,
            fieldnames=fieldnames,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
        )

        self.writers[table_name] = writer
        self.fieldnames[table_name] = fieldnames

        return writer

    def write_main_records(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of main records.

        Args:
            records: List of main table records to write
        """
        if not records:
            return

        # Extract field names from records
        field_names_set: set[str] = set()
        for record in records:
            field_names_set.update(record.keys())
        field_names = sorted(field_names_set)

        # Get or create writer
        writer = self._get_writer_for_table("main", field_names)

        # Write header if needed
        if "main" not in self.headers_written and self.include_header:
            writer.writeheader()
            self.headers_written.add("main")

        # Write records
        writer.writerows(records)

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

        # Extract field names from records
        field_names_set: set[str] = set()
        for record in records:
            field_names_set.update(record.keys())

        # Merge with existing field names if table already exists
        if table_name in self.fieldnames:
            field_names_set.update(self.fieldnames[table_name])

        field_names = sorted(field_names_set)

        # Get or create writer
        writer = self._get_writer_for_table(table_name, field_names)

        # Write header if needed
        if table_name not in self.headers_written and self.include_header:
            writer.writeheader()
            self.headers_written.add(table_name)

        # Write records
        writer.writerows(records)

    def finalize(self) -> None:
        """Finalize the output and flush all writers."""
        # Flush all file objects
        for file_obj in self.file_objects.values():
            if hasattr(file_obj, "flush"):
                file_obj.flush()

    def close(self) -> None:
        """Clean up resources and close all files."""
        if not getattr(self, "_finalized", False):
            self.finalize()
            self._finalized = True

        # Close all file objects if we opened them
        if self.should_close_files:
            for file_obj in self.file_objects.values():
                if hasattr(file_obj, "close"):
                    file_obj.close()

        # Clear references
        self.file_objects.clear()
        self.writers.clear()
        self.fieldnames.clear()
