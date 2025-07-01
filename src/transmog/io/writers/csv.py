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
from transmog.io.writer_interface import DataWriter, StreamingWriter, WriterUtils
from transmog.types.base import JsonDict

# Setup logger
logger = logging.getLogger(__name__)

# Check for PyArrow availability
try:
    import pyarrow as pa  # noqa: F401
    import pyarrow.csv as pa_csv  # noqa: F401

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


class CsvWriter(DataWriter):
    """CSV format writer.

    This writer handles writing flattened data to CSV format files with
    consistent interface and optional compression support.
    """

    @classmethod
    def format_name(cls) -> str:
        """Return the name of the format this writer handles."""
        return "csv"

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer's dependencies are available."""
        return True

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

    def supports_compression(self) -> bool:
        """Check if this writer supports compression.

        Returns:
            bool: True as CSV writer supports gzip compression
        """
        return True

    def get_supported_codecs(self) -> list[str]:
        """Get list of supported compression codecs.

        Returns:
            list[str]: List of supported compression methods
        """
        return ["gzip"]

    def write_table(
        self,
        table_data: list[JsonDict],
        output_path: Union[str, BinaryIO, TextIO],
        **format_options: Any,
    ) -> Union[str, BinaryIO, TextIO]:
        """Write a table to a CSV file.

        Args:
            table_data: The table data to write
            output_path: Path or file-like object to write to
            **format_options: Format-specific options (include_header, delimiter, etc.)

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
        """
        try:
            # Use provided options or instance defaults
            use_header = format_options.get("include_header", self.include_header)
            use_delimiter = format_options.get("delimiter", self.delimiter)
            use_quotechar = format_options.get("quotechar", self.quotechar)
            use_quoting = format_options.get("quoting", self.quoting)
            use_escapechar = format_options.get("escapechar", self.escapechar)
            compression = format_options.get("compression", self.compression)

            # Handle empty data case
            if not table_data:
                if isinstance(output_path, (str, pathlib.Path)):
                    path_str = str(output_path)

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
                        if isinstance(output_path, pathlib.Path)
                        else path_str
                    )
                else:
                    return output_path

            # Extract field names from all records
            field_names_set: set[str] = set()
            for record in table_data:
                field_names_set.update(record.keys())
            field_names = sorted(field_names_set)

            # Generate CSV content
            csv_content = self._generate_csv_content(
                table_data,
                field_names,
                use_header,
                use_delimiter,
                use_quotechar,
                use_quoting,
                use_escapechar,
            )

            # Apply compression if requested
            if compression == "gzip":
                csv_bytes = gzip.compress(csv_content.encode("utf-8"))
            else:
                csv_bytes = csv_content.encode("utf-8")

            # Handle file path destination
            if isinstance(output_path, (str, pathlib.Path)):
                path_str = str(output_path)

                # Add .gz extension for compressed files
                if compression == "gzip" and not path_str.endswith(".gz"):
                    path_str += ".gz"

                os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)

                with open(path_str, "wb") as f:
                    f.write(csv_bytes)

                return (
                    pathlib.Path(path_str)
                    if isinstance(output_path, pathlib.Path)
                    else path_str
                )

            # Handle file-like object destination
            elif hasattr(output_path, "write"):
                # For compressed data, always write as binary
                if compression:
                    if hasattr(output_path, "mode") and "b" not in getattr(
                        output_path, "mode", ""
                    ):
                        raise OutputError("Cannot write compressed CSV to text stream")
                    binary_output = cast(BinaryIO, output_path)
                    binary_output.write(csv_bytes)
                else:
                    # Write as text or binary based on stream type
                    if (
                        hasattr(output_path, "mode")
                        and "b" not in getattr(output_path, "mode", "")
                    ) or (
                        not hasattr(output_path, "mode")
                        and hasattr(output_path, "read")
                        and not hasattr(output_path, "readinto")
                    ):
                        text_output = cast(TextIO, output_path)
                        text_output.write(csv_content)
                    else:
                        binary_output = cast(BinaryIO, output_path)
                        binary_output.write(csv_bytes)

                return output_path
            else:
                raise OutputError(f"Invalid destination type: {type(output_path)}")

        except Exception as e:
            logger.error(f"Error writing CSV: {e}")
            raise OutputError(f"Failed to write CSV file: {e}") from e

    def _generate_csv_content(
        self,
        table_data: list[JsonDict],
        field_names: list[str],
        include_header: bool,
        delimiter: str,
        quotechar: str,
        quoting: int,
        escapechar: Optional[str],
    ) -> str:
        """Generate CSV content as a string.

        Args:
            table_data: The table data
            field_names: List of field names
            include_header: Whether to include headers
            delimiter: Column delimiter
            quotechar: Quote character
            quoting: Quoting mode
            escapechar: Escape character

        Returns:
            str: CSV content
        """
        csv_buffer = io.StringIO(newline="")
        writer_params: dict[str, Any] = {
            "fieldnames": field_names,
            "delimiter": delimiter,
            "quotechar": quotechar,
            "quoting": quoting,
        }

        if escapechar:
            writer_params["escapechar"] = escapechar

        writer: csv.DictWriter[str] = csv.DictWriter(csv_buffer, **writer_params)

        if include_header:
            writer.writeheader()

        writer.writerows(table_data)
        return csv_buffer.getvalue()

    def write_all_tables(
        self,
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        base_path: Union[str],
        entity_name: str,
        **options: Any,
    ) -> dict[str, str]:
        """Write main and child tables to CSV files.

        Args:
            main_table: The main table data
            child_tables: Dictionary of child tables
            base_path: Directory to write files to
            entity_name: Name of the entity (for main table filename)
            **options: Additional CSV formatting options

        Returns:
            Dictionary mapping table names to file paths

        Raises:
            OutputError: If writing fails
        """
        # Use the consolidated write pattern
        return WriterUtils.write_all_tables_pattern(
            main_table=main_table,
            child_tables=child_tables,
            base_path=base_path,
            entity_name=entity_name,
            format_name="csv",
            write_table_func=self.write_table,
            compression=options.get("compression", self.compression),
            **options,
        )


class CsvStreamingWriter(StreamingWriter):
    """Streaming writer for CSV format.

    This writer allows incremental writing of data to CSV files
    without keeping the entire dataset in memory, with unified interface.
    """

    @classmethod
    def format_name(cls) -> str:
        """Get the format name for this writer.

        Returns:
            str: The format name ("csv")
        """
        return "csv"

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        include_header: bool = True,
        delimiter: str = ",",
        quotechar: str = '"',
        compression: Optional[str] = None,
        buffer_size: int = 1000,
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
            buffer_size: Number of records to buffer before writing
            **options: Additional CSV writer options
        """
        super().__init__(destination, entity_name, buffer_size, **options)
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
                filename = WriterUtils.sanitize_filename(table_name)

            file_path = WriterUtils.build_output_path(
                self.base_dir, filename, "csv", self.compression
            )

            # Open file with appropriate compression
            file_obj = WriterUtils.open_output_file(
                file_path, "w", self.compression, encoding="utf-8"
            )

            # Ensure we have a TextIO object
            if not isinstance(file_obj, TextIO):
                # For gzip files, we need to ensure text mode
                if self.compression == "gzip":
                    import gzip

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

    def initialize_main_table(self, **options: Any) -> None:
        """Initialize the main table for streaming.

        Args:
            **options: Format-specific options
        """
        # Main table initialization happens when first records are written
        pass

    def initialize_child_table(self, table_name: str, **options: Any) -> None:
        """Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
            **options: Format-specific options
        """
        # Child table initialization happens when first records are written
        pass

    def write_main_records(self, records: list[dict[str, Any]], **options: Any) -> None:
        """Write a batch of main records.

        Args:
            records: List of main table records to write
            **options: Format-specific options
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

        # Update record count and report progress
        self.record_counts["main"] = self.record_counts.get("main", 0) + len(records)
        self._report_progress("main", len(records))

    def write_child_records(
        self, table_name: str, records: list[dict[str, Any]], **options: Any
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: List of child records to write
            **options: Format-specific options
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

        # Update record count and report progress
        self.record_counts[table_name] = self.record_counts.get(table_name, 0) + len(
            records
        )
        self._report_progress(table_name, len(records))

    def finalize(self, **options: Any) -> None:
        """Finalize the output and flush all writers.

        Args:
            **options: Format-specific options
        """
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
