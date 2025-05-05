"""
CSV writer for Transmog output.

This module provides a CSV writer with PyArrow and standard library implementations.
"""

import os
import logging
import io
import csv
import gzip
import bz2
import lzma
from typing import Any, Dict, List, Optional, Union, BinaryIO, Tuple, TextIO
from io import StringIO

from transmog.io.writer_interface import DataWriter, StreamingWriter
from transmog.naming.conventions import sanitize_column_names
from transmog.config.settings import settings
from transmog.error import OutputError, MissingDependencyError
from transmog.io.writer_factory import register_writer, register_streaming_writer

# Configure logger
logger = logging.getLogger(__name__)

# Check for PyArrow availability
try:
    import pyarrow as pa
    import pyarrow.csv as pa_csv

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


class CsvWriter(DataWriter):
    """
    Writer for CSV output.

    Supports writing flattened tables to CSV format.
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
        use_pyarrow: bool = True,
        **options,
    ):
        """
        Initialize the CSV writer.

        Args:
            include_header: Whether to include a header row
            delimiter: CSV field delimiter
            quotechar: CSV quote character
            use_pyarrow: Whether to use PyArrow for CSV writing (if available)
            **options: Additional CSV writer options
        """
        self.include_header = include_header
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.use_pyarrow = use_pyarrow and PYARROW_AVAILABLE
        self.options = options

    def write_table(
        self, table_data: List[Dict[str, Any]], output_path: str, **options
    ) -> str:
        """
        Write table data to a CSV file.

        Args:
            table_data: List of records to write
            output_path: Path to write the CSV file
            **options: Additional options to override instance options

        Returns:
            Path to the written file
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

        # Generate CSV content
        merged_options = {**self.options, **options}
        include_header = merged_options.get("include_header", self.include_header)
        delimiter = merged_options.get("delimiter", self.delimiter)
        quotechar = merged_options.get("quotechar", self.quotechar)
        use_pyarrow = merged_options.get("use_pyarrow", self.use_pyarrow)

        # Try PyArrow first if enabled and available
        try:
            if use_pyarrow and PYARROW_AVAILABLE and table_data:
                self._write_table_with_pyarrow(
                    table_data,
                    output_path,
                    include_header=include_header,
                    delimiter=delimiter,
                )
            else:
                self._write_table_with_stdlib(
                    table_data,
                    output_path,
                    include_header=include_header,
                    delimiter=delimiter,
                    quotechar=quotechar,
                )
        except Exception as e:
            logger.error(f"Error writing CSV to {output_path}: {str(e)}")
            raise OutputError(f"Failed to write CSV: {str(e)}")

        return output_path

    def write_all_tables(
        self,
        main_table: List[Dict[str, Any]],
        child_tables: Dict[str, List[Dict[str, Any]]],
        base_path: str,
        entity_name: str,
        **options,
    ) -> Dict[str, str]:
        """Write main and child tables to the output format.

        Args:
            main_table: Main table data
            child_tables: Dict of child table name to table data
            base_path: Base directory for output
            entity_name: Name of the main entity
            **options: Format-specific options

        Returns:
            Dict mapping table names to output file paths
        """
        # Ensure base directory exists
        os.makedirs(base_path, exist_ok=True)

        result = {}

        # Write main table
        main_file_path = os.path.join(base_path, f"{entity_name}.csv")
        result["main"] = self.write_table(
            table_data=main_table, output_path=main_file_path, **options
        )

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Skip empty tables
            if not table_data:
                continue

            child_file_path = os.path.join(base_path, f"{table_name}.csv")
            result[table_name] = self.write_table(
                table_data=table_data, output_path=child_file_path, **options
            )

        return result

    def _write_table_with_pyarrow(
        self,
        table_data: List[Dict[str, Any]],
        output_path: str,
        include_header: bool,
        delimiter: str,
    ) -> None:
        """
        Write table data to a file using PyArrow CSV writer.

        Args:
            table_data: List of records to write
            output_path: Path to write the CSV file
            include_header: Whether to include a header row
            delimiter: CSV field delimiter
        """
        if not PYARROW_AVAILABLE:
            raise MissingDependencyError("PyArrow is required for this operation")

        # Extract field names from the first record
        fields = list(table_data[0].keys())

        # Convert list of dicts to dict of lists
        columns = {field: [] for field in fields}
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
        table_data: List[Dict[str, Any]],
        output_path: str,
        include_header: bool,
        delimiter: str,
        quotechar: str,
    ) -> None:
        """
        Write table data to a file using standard library CSV writer.

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
        self, table_data: List[Dict[str, Any]], include_header: bool, delimiter: str
    ) -> bytes:
        """
        Write table data using PyArrow CSV writer.

        Args:
            table_data: List of records to write
            include_header: Whether to include a header row
            delimiter: CSV field delimiter

        Returns:
            CSV data as bytes
        """
        if not PYARROW_AVAILABLE:
            raise MissingDependencyError("PyArrow is required for this operation")

        try:
            # Convert to PyArrow Table
            if not table_data:
                return b""

            # Extract field names from the first record
            fields = list(table_data[0].keys())

            # Convert list of dicts to dict of lists
            columns = {field: [] for field in fields}
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
            raise OutputError(f"Failed to write CSV: {str(e)}")

    def _write_with_stdlib(
        self,
        table_data: List[Dict[str, Any]],
        include_header: bool,
        delimiter: str,
        quotechar: str,
    ) -> bytes:
        """
        Write table data using standard library CSV writer.

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
            raise OutputError(f"Failed to write CSV: {str(e)}")

    def close(self) -> None:
        """
        Close the writer (no-op for CSV).

        Returns:
            None
        """
        pass


class CsvStreamingWriter(StreamingWriter):
    """
    Streaming writer for CSV output.

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
        **options,
    ):
        """
        Initialize the CSV streaming writer.

        Args:
            destination: Output file path or file-like object
            entity_name: Name of the entity
            include_header: Whether to include a header row
            delimiter: CSV field delimiter
            quotechar: CSV quote character
            **options: Additional CSV writer options
        """
        self.entity_name = entity_name
        self.include_header = include_header
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.options = options

        # Open or use the destination
        if destination is None:
            # Default to stdout for demo/testing
            import sys

            self.file_objects = {"main": sys.stdout}
            self.file_paths = {}
            self.should_close = False
        elif isinstance(destination, str):
            # It's a base directory path
            self.file_objects = {}
            self.file_paths = {}
            self.base_dir = destination
            os.makedirs(self.base_dir, exist_ok=True)
            self.should_close = True
        else:
            # It's a file-like object
            self.file_objects = {"main": destination}
            self.file_paths = {}
            self.should_close = False

        # Keep track of CSV writers
        self.writers = {}
        self.headers = {}

    def _get_file_for_table(self, table_name: str) -> TextIO:
        """
        Get or create a file object for the given table.

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
                f"Cannot create file for table {table_name}: no base directory specified"
            )

    def _get_writer_for_table(
        self, table_name: str, fieldnames: List[str]
    ) -> csv.DictWriter:
        """
        Get or create a CSV writer for the given table.

        Args:
            table_name: Name of the table
            fieldnames: Field names for the CSV

        Returns:
            CSV writer
        """
        if table_name in self.writers:
            return self.writers[table_name]

        # Create a new writer
        file_obj = self._get_file_for_table(table_name)
        writer = csv.DictWriter(
            file_obj,
            fieldnames=fieldnames,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
            quoting=csv.QUOTE_MINIMAL,
        )
        self.writers[table_name] = writer
        self.headers[table_name] = fieldnames

        # Write header if requested
        if self.include_header:
            writer.writeheader()
            file_obj.flush()

        return writer

    def initialize_main_table(self, **options) -> None:
        """
        Initialize the main table for streaming.

        Args:
            **options: Format-specific options

        Returns:
            None
        """
        # Nothing to do here - writer will be created when first records are written
        pass

    def initialize_child_table(self, table_name: str, **options) -> None:
        """
        Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
            **options: Format-specific options

        Returns:
            None
        """
        # Nothing to do here - writer will be created when first records are written
        pass

    def write_main_records(self, records: List[Dict[str, Any]], **options) -> None:
        """
        Write a batch of main records.

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
        self, table_name: str, records: List[Dict[str, Any]], **options
    ) -> None:
        """
        Write a batch of child records.

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

    def finalize(self, **options) -> None:
        """
        Finalize all tables.

        Args:
            **options: Format-specific options

        Returns:
            None
        """
        # Just flush all files
        for file_obj in self.file_objects.values():
            file_obj.flush()

    def close(self) -> None:
        """
        Close any resources used by the writer.

        Returns:
            None
        """
        if self.should_close:
            for file_obj in self.file_objects.values():
                try:
                    file_obj.close()
                except Exception:
                    pass

        self.writers = {}
        self.headers = {}


# Register the writers
register_writer("csv", CsvWriter)
register_streaming_writer("csv", CsvStreamingWriter)
