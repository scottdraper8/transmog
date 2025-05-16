"""CSV writer for Transmog output.

This module provides a CSV writer with PyArrow and standard library implementations.
"""

import csv
import io
import logging
import os
import pathlib
from typing import Any, BinaryIO, Optional, TextIO, Union, cast

from transmog.error import MissingDependencyError, OutputError
from transmog.io.writer_factory import register_streaming_writer, register_writer
from transmog.io.writer_interface import DataWriter, StreamingWriter
from transmog.types.base import JsonDict

# Setup logger
logger = logging.getLogger(__name__)

# Check for PyArrow availability
try:
    import pyarrow as pa
    import pyarrow.csv as pa_csv

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


class CsvWriter(DataWriter):
    """CSV format writer.

    This writer handles writing flattened data to CSV format files.
    """

    @classmethod
    def format_name(cls) -> str:
        """Return the name of the format this writer handles."""
        return "csv"

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer's dependencies are available."""
        # CSV is available in the standard library
        return True

    def __init__(
        self,
        include_header: bool = True,
        delimiter: str = ",",
        quotechar: str = '"',
        quoting: int = csv.QUOTE_MINIMAL,
        escapechar: Optional[str] = None,
        **options: Any,
    ):
        """Initialize the CSV writer.

        Args:
            include_header: Whether to include column headers
            delimiter: Column delimiter character
            quotechar: Character to use for quoting
            quoting: Quoting mode (from csv module)
            escapechar: Character to use for escaping
            **options: CSV formatting options
        """
        self.include_header = include_header
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.quoting = quoting
        self.escapechar = escapechar
        self.options = options

    def write(
        self,
        data: Any,
        destination: Union[str, pathlib.Path, BinaryIO, TextIO],
        **options: Any,
    ) -> Union[str, pathlib.Path, BinaryIO, TextIO]:
        """Write data to the specified destination.

        Args:
            data: Data to write
            destination: Path or file-like object to write to
            **options: Format-specific options

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
        """
        # Combine constructor options with per-call options
        combined_options = {**self.options, **options}

        # Delegate to write_table for implementation
        return self.write_table(data, destination, **combined_options)

    def write_table(
        self,
        table_data: list[JsonDict],
        output_path: Union[str, pathlib.Path, BinaryIO, TextIO],
        include_header: Optional[bool] = None,
        delimiter: Optional[str] = None,
        quotechar: Optional[str] = None,
        quoting: Optional[int] = None,
        escapechar: Optional[str] = None,
        **options: Any,
    ) -> Union[str, pathlib.Path, BinaryIO, TextIO]:
        """Write a table to a CSV file.

        Args:
            table_data: The table data to write
            output_path: Path or file-like object to write to
            include_header: Whether to include column headers
            delimiter: Column delimiter character
            quotechar: Character to use for quoting
            quoting: Quoting mode (from csv module)
            escapechar: Character to use for escaping
            **options: Additional options

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
        """
        try:
            # Use provided options or instance defaults
            use_header = (
                include_header if include_header is not None else self.include_header
            )
            use_delimiter = delimiter if delimiter is not None else self.delimiter
            use_quotechar = quotechar if quotechar is not None else self.quotechar
            use_quoting = quoting if quoting is not None else self.quoting
            use_escapechar = escapechar if escapechar is not None else self.escapechar

            # Handle empty data case
            if not table_data:
                if isinstance(output_path, (str, pathlib.Path)):
                    # Create directory and empty file
                    path_str = str(output_path)
                    os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)
                    with open(path_str, "w") as f:
                        pass
                    return output_path
                else:
                    # For file-like objects, just return without writing
                    return output_path

            # Extract field names from the first record
            field_names = list(table_data[0].keys())

            # Determine whether we're writing to a file or file-like object
            if isinstance(output_path, (str, pathlib.Path)):
                # Convert to string if Path
                path_str = str(output_path)

                # Ensure directory exists
                os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)

                # Write to file
                with open(path_str, "w", newline="") as f:
                    writer_binary: csv.DictWriter = csv.DictWriter(
                        f,
                        fieldnames=field_names,
                        delimiter=use_delimiter,
                        quotechar=use_quotechar,
                        quoting=use_quoting,
                        escapechar=use_escapechar if use_escapechar else None,
                    )

                    if use_header:
                        writer_binary.writeheader()

                    writer_binary.writerows(table_data)

                return output_path
            else:
                # For file-like objects, determine if it's a binary stream
                is_binary = isinstance(output_path, io.BufferedIOBase) or isinstance(
                    output_path, io.BytesIO
                )

                # Additional check for file objects with mode attribute
                if not is_binary and hasattr(output_path, "mode"):
                    is_binary = "b" in output_path.mode

                if is_binary:
                    # For binary streams, generate CSV in memory and write as bytes
                    csv_buffer = io.StringIO(newline="")
                    writer_params_binary = {
                        "fieldnames": field_names,
                        "delimiter": use_delimiter,
                        "quotechar": use_quotechar,
                        "quoting": use_quoting,
                    }

                    if use_escapechar:
                        writer_params_binary["escapechar"] = use_escapechar

                    writer_binary_stream: csv.DictWriter = csv.DictWriter(
                        csv_buffer,
                        fieldnames=field_names,
                        delimiter=use_delimiter,
                        quotechar=use_quotechar,
                        quoting=use_quoting,
                        escapechar=use_escapechar if use_escapechar else None,
                    )

                    if use_header:
                        writer_binary_stream.writeheader()

                    writer_binary_stream.writerows(table_data)

                    # Convert to bytes and write to the binary stream
                    binary_data = csv_buffer.getvalue().encode("utf-8")

                    # Handle binary or text IO appropriately
                    if hasattr(output_path, "mode") and "b" not in getattr(
                        output_path, "mode", ""
                    ):
                        # For text streams, decode binary data
                        text_output = cast(TextIO, output_path)
                        text_output.write(binary_data.decode("utf-8"))
                    else:
                        # For binary streams
                        binary_output = cast(BinaryIO, output_path)
                        binary_output.write(binary_data)
                else:
                    # Text stream
                    text_output = cast(TextIO, output_path)
                    writer_params_text = {
                        "fieldnames": field_names,
                        "delimiter": use_delimiter,
                        "quotechar": use_quotechar,
                        "quoting": use_quoting,
                    }

                    if use_escapechar:
                        writer_params_text["escapechar"] = use_escapechar

                    writer_text: csv.DictWriter = csv.DictWriter(
                        text_output,
                        fieldnames=field_names,
                        delimiter=use_delimiter,
                        quotechar=use_quotechar,
                        quoting=use_quoting,
                        escapechar=use_escapechar if use_escapechar else None,
                    )

                    if use_header:
                        writer_text.writeheader()

                    writer_text.writerows(table_data)

                return output_path

        except Exception as e:
            logger.error(f"Error writing CSV: {e}")
            raise OutputError(f"Failed to write CSV file: {e}") from e

    def write_all_tables(
        self,
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        base_path: Union[str, pathlib.Path],
        entity_name: str,
        **options: Any,
    ) -> dict[str, Union[str, pathlib.Path]]:
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
        results: dict[str, Union[str, pathlib.Path]] = {}

        # Combine constructor options with per-call options
        combined_options = {**self.options, **options}

        # Ensure base directory exists
        base_path_str = str(base_path)
        os.makedirs(base_path_str, exist_ok=True)

        # Write main table
        main_path: Union[str, pathlib.Path]
        if isinstance(base_path, pathlib.Path):
            main_path = base_path / f"{entity_name}.csv"
        else:
            main_path = os.path.join(base_path_str, f"{entity_name}.csv")

        self.write_table(main_table, main_path, **combined_options)
        results["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Replace dots and slashes with underscores for file names
            safe_name = table_name.replace(".", "_").replace("/", "_")

            table_path: Union[str, pathlib.Path]
            if isinstance(base_path, pathlib.Path):
                table_path = base_path / f"{safe_name}.csv"
            else:
                table_path = os.path.join(base_path_str, f"{safe_name}.csv")

            self.write_table(table_data, table_path, **combined_options)
            results[table_name] = table_path

        return results

    def _write_table_with_pyarrow(
        self,
        table_data: list[dict[str, Any]],
        output_path: str,
        include_header: bool,
        delimiter: str,
    ) -> None:
        """Write table data to a file using PyArrow CSV writer.

        Args:
            table_data: List of records to write
            output_path: Path to write the CSV file
            include_header: Whether to include a header row
            delimiter: CSV field delimiter
        """
        if not PYARROW_AVAILABLE:
            raise MissingDependencyError(
                "PyArrow is required for this operation", package="pyarrow"
            )

        # Extract field names from the first record
        fields = list(table_data[0].keys())

        # Convert list of dicts to dict of lists
        columns: dict[str, list[Any]] = {field: [] for field in fields}
        for record in table_data:
            for field in fields:
                columns[field].append(record.get(field, None))

        # Create PyArrow arrays for each column
        arrays = []
        for field in fields:
            arrays.append(pa.array(columns[field]))

        # Create PyArrow table
        table = pa.Table.from_arrays(arrays, names=fields)

        # Write to CSV file
        with open(output_path, "wb") as f:
            pa_csv.write_csv(
                table,
                f,
                write_options=pa_csv.WriteOptions(
                    include_header=include_header, delimiter=delimiter
                ),
            )

    def _write_table_with_stdlib(
        self,
        table_data: list[dict[str, Any]],
        output_path: str,
        include_header: bool,
        delimiter: str,
        quotechar: str,
    ) -> None:
        """Write table data to a file using standard library CSV writer.

        Args:
            table_data: List of records to write
            output_path: Path to write the CSV file
            include_header: Whether to include a header row
            delimiter: CSV field delimiter
            quotechar: CSV quote character
        """
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            if not table_data:
                return

            # Extract field names from the first record
            fieldnames = list(table_data[0].keys())

            # Create CSV writer
            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                delimiter=delimiter,
                quotechar=quotechar,
                quoting=csv.QUOTE_MINIMAL,
            )

            # Write header if requested
            if include_header:
                writer.writeheader()

            # Write all records
            for record in table_data:
                writer.writerow(record)

    def _write_with_pyarrow(
        self, table_data: list[dict[str, Any]], include_header: bool, delimiter: str
    ) -> bytes:
        """Write table data using PyArrow CSV writer.

        Args:
            table_data: List of records to write
            include_header: Whether to include a header row
            delimiter: CSV field delimiter

        Returns:
            CSV data as bytes
        """
        if not PYARROW_AVAILABLE:
            raise MissingDependencyError(
                "PyArrow is required for this operation", package="pyarrow"
            )

        try:
            # Convert to PyArrow Table
            if not table_data:
                return b""

            # Extract field names from the first record
            fields = list(table_data[0].keys())

            # Convert list of dicts to dict of lists
            columns: dict[str, list[Any]] = {field: [] for field in fields}
            for record in table_data:
                for field in fields:
                    columns[field].append(record.get(field, None))

            # Create PyArrow arrays for each column
            arrays = []
            for field in fields:
                arrays.append(pa.array(columns[field]))

            # Create PyArrow table
            table = pa.Table.from_arrays(arrays, names=fields)

            # Write to CSV
            output = io.BytesIO()
            pa_csv.write_csv(
                table,
                output,
                write_options=pa_csv.WriteOptions(
                    include_header=include_header, delimiter=delimiter
                ),
            )
            return output.getvalue()
        except Exception as e:
            logger.error(f"Error writing CSV with PyArrow: {str(e)}")
            raise OutputError(f"Failed to write CSV: {str(e)}") from e

    def _write_with_stdlib(
        self,
        table_data: list[dict[str, Any]],
        include_header: bool,
        delimiter: str,
        quotechar: str,
    ) -> bytes:
        """Write table data using standard library CSV writer.

        Args:
            table_data: List of records to write
            include_header: Whether to include a header row
            delimiter: CSV field delimiter
            quotechar: CSV quote character

        Returns:
            CSV data as bytes
        """
        try:
            # Use StringIO for text operations, then convert to bytes
            output = io.StringIO()
            if not table_data:
                return b""

            # Extract field names from the first record
            fieldnames = list(table_data[0].keys())

            # Create CSV writer
            writer = csv.DictWriter(
                output,
                fieldnames=fieldnames,
                delimiter=delimiter,
                quotechar=quotechar,
                quoting=csv.QUOTE_MINIMAL,
            )

            # Write header if requested
            if include_header:
                writer.writeheader()

            # Write all records
            for record in table_data:
                writer.writerow(record)

            # Convert to bytes and return
            return output.getvalue().encode("utf-8")
        except Exception as e:
            logger.error(f"Error writing CSV with stdlib: {str(e)}")
            raise OutputError(f"Failed to write CSV: {str(e)}") from e

    def close(self) -> None:
        """Close the writer (no-op for CSV).

        Returns:
            None
        """
        pass


class CsvStreamingWriter(StreamingWriter):
    """Streaming writer for CSV output.

    Supports writing flattened tables to CSV format in a streaming manner,
    minimizing memory usage for large datasets.
    """

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        include_header: bool = True,
        delimiter: str = ",",
        quotechar: str = '"',
        **options: Any,
    ):
        """Initialize the CSV streaming writer.

        Args:
            destination: Output file path or file-like object
            entity_name: Name of the entity (used for file naming)
            include_header: Whether to include header row
            delimiter: CSV column delimiter
            quotechar: Character to use for quoting
            **options: Additional CSV formatting options
        """
        self.entity_name = entity_name
        self.include_header = include_header
        self.options = {
            "delimiter": delimiter,
            "quotechar": quotechar,
            **options,
        }

        # For file tracking
        self.writers: dict[str, csv.DictWriter] = {}
        self.text_wrappers: dict[str, io.TextIOWrapper] = {}
        self.headers: dict[str, list[str]] = {}
        self.file_objects: dict[str, TextIO] = {}
        self.file_paths: dict[str, str] = {}
        self.should_close = True

        # Use file path if string
        if isinstance(destination, str):
            self.base_dir = os.path.dirname(destination) or "."
            os.makedirs(self.base_dir, exist_ok=True)
            file_name = os.path.basename(destination) or f"{entity_name}.csv"

            # Check if destination is a directory
            if os.path.isdir(destination):
                # Use the directory as base_dir and create a default filename
                self.base_dir = destination
                file_path = os.path.join(self.base_dir, f"{entity_name}.csv")
            else:
                # Use the provided path
                file_path = os.path.join(self.base_dir, file_name)

            self.file_paths = {"main": file_path}
            self.file_objects = {
                "main": open(file_path, "w", newline="", encoding="utf-8")
            }
        elif destination is not None:
            # Handle binary streams by wrapping them in TextIOWrapper
            if isinstance(destination, (io.BufferedIOBase, io.BytesIO)) or (
                hasattr(destination, "mode") and "b" in destination.mode
            ):
                # Binary stream - wrap it for text use
                binary_dest = cast(BinaryIO, destination)
                text_wrapper = io.TextIOWrapper(
                    binary_dest,
                    encoding="utf-8",
                    write_through=True,
                    line_buffering=True,
                )
                self.text_wrappers["main"] = text_wrapper
                self.file_objects = {"main": text_wrapper}
            else:
                # Text stream
                self.file_objects = {"main": cast(TextIO, destination)}
            self.should_close = False

    def _get_file_for_table(self, table_name: str) -> TextIO:
        """Get or create a file object for the given table.

        Args:
            table_name: Name of the table

        Returns:
            File object for writing
        """
        if table_name in self.file_objects:
            return self.file_objects[table_name]

        # Create a new file
        if hasattr(self, "base_dir"):
            # Make sure paths are safe
            safe_name = table_name.replace("/", "_").replace("\\", "_")
            file_path = os.path.join(self.base_dir, f"{safe_name}.csv")
            self.file_paths[table_name] = file_path
            file_obj = open(file_path, "w", newline="", encoding="utf-8")
            self.file_objects[table_name] = file_obj
            return file_obj
        else:
            raise OutputError(
                f"Cannot create file for table {table_name}: "
                f"no base directory specified"
            )

    def _get_writer_for_table(
        self, table_name: str, fieldnames: list[str]
    ) -> csv.DictWriter:
        """Get a CSV DictWriter for a table.

        Args:
            table_name: Name of the table
            fieldnames: Field names for the CSV header

        Returns:
            CSV DictWriter instance
        """
        if table_name in self.writers:
            return self.writers[table_name]

        file_obj = self._get_file_for_table(table_name)

        # Determine if working with a binary stream
        is_binary = (hasattr(file_obj, "mode") and "b" in file_obj.mode) or isinstance(
            file_obj, io.BytesIO
        )

        # For binary mode files, use TextIOWrapper for text-based CSV writing
        if is_binary:
            binary_file = cast(BinaryIO, file_obj)
            text_file = io.TextIOWrapper(
                binary_file, encoding="utf-8", write_through=True, line_buffering=True
            )

            # Create the CSV writer with the text wrapper
            writer = csv.DictWriter(text_file, fieldnames=fieldnames, **self.options)

            # Store both objects to prevent garbage collection
            self.text_wrappers[table_name] = text_file
        else:
            # Standard text mode file
            writer = csv.DictWriter(file_obj, fieldnames=fieldnames, **self.options)

        if self.include_header:
            writer.writeheader()

        # Store the writer and headers
        self.writers[table_name] = writer
        self.headers[table_name] = fieldnames

        return writer

    def initialize_main_table(self, **options: Any) -> None:
        """Initialize the main table for streaming.

        Args:
            **options: Format-specific options

        Returns:
            None
        """
        # Nothing to do here - writer will be created when first records are written
        pass

    def initialize_child_table(self, table_name: str, **options: Any) -> None:
        """Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
            **options: Format-specific options

        Returns:
            None
        """
        # Nothing to do here - writer will be created when first records are written
        pass

    def write_main_records(self, records: list[dict[str, Any]], **options: Any) -> None:
        """Write a batch of main records.

        Args:
            records: Batch of records to write
            **options: Format-specific options

        Returns:
            None
        """
        if not records:
            return

        # Get fieldnames from the first record
        fieldnames = list(records[0].keys())

        # Get or create the writer
        writer = self._get_writer_for_table("main", fieldnames)

        # Write all records
        for record in records:
            writer.writerow(record)

        # Flush to ensure data is written
        self.file_objects["main"].flush()

    def write_child_records(
        self, table_name: str, records: list[dict[str, Any]], **options: Any
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: Batch of records to write
            **options: Format-specific options

        Returns:
            None
        """
        if not records:
            return

        # Get fieldnames from the first record
        fieldnames = list(records[0].keys())

        # Get or create the writer
        writer = self._get_writer_for_table(table_name, fieldnames)

        # Write all records
        for record in records:
            writer.writerow(record)

        # Flush to ensure data is written
        self.file_objects[table_name].flush()

    def finalize(self, **options: Any) -> None:
        """Finalize all tables.

        Args:
            **options: Format-specific options

        Returns:
            None
        """
        # Just flush all files
        for file_obj in self.file_objects.values():
            file_obj.flush()

    def close(self) -> None:
        """Close any resources used by the writer.

        Returns:
            None
        """
        if self.should_close:
            for file_obj in self.file_objects.values():
                try:
                    file_obj.close()
                except Exception as e:
                    logger.debug(f"Error closing file: {e}")

        self.writers = {}
        self.headers = {}


# Register the writers
register_writer("csv", CsvWriter)
register_streaming_writer("csv", CsvStreamingWriter)
