"""
Parquet writer for Transmog output.

This module provides a Parquet writer using PyArrow.
"""

import os
import importlib.util
import pathlib
from typing import Any, Dict, List, Optional, Union, BinaryIO, TextIO, Tuple, Set
import logging

from transmog.io.writer_interface import DataWriter, StreamingWriter
from transmog.error import OutputError, MissingDependencyError, logger
from transmog.types.base import JsonDict
from transmog.io.writer_factory import register_writer, register_streaming_writer

# Try to import PyArrow at module level, but don't fail if it's not available
# Instead, set these variables to None so we can check them later
pa = None
pq = None
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    # PyArrow not available - will be checked in is_available()
    pass


class ParquetWriter(DataWriter):
    """
    Parquet format writer.

    This writer handles writing flattened data to Parquet format files.
    Requires PyArrow to be installed.
    """

    def __init__(self, compression: str = "snappy", **options):
        """
        Initialize the Parquet writer.

        Args:
            compression: Compression format (snappy, gzip, brotli, etc.)
            **options: Additional Parquet writer options
        """
        self.compression = compression
        self.options = options

    @classmethod
    def format_name(cls) -> str:
        """
        Get the format name for this writer.

        Returns:
            str: The format name ("parquet")
        """
        return "parquet"

    def write(
        self, data: Any, destination: Union[str, pathlib.Path, BinaryIO], **options
    ) -> Any:
        """
        Write data to the specified destination.

        Args:
            data: Data to write
            destination: Path or file-like object to write to
            **options: Format-specific options

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
            MissingDependencyError: If PyArrow is not available
        """
        # Combine constructor options with per-call options
        combined_options = {**self.options, **options}

        # Delegate to write_table for implementation
        return self.write_table(data, destination, **combined_options)

    def write_table(
        self,
        table_data: List[JsonDict],
        output_path: Union[str, pathlib.Path, BinaryIO],
        compression: Optional[str] = None,
        **options,
    ) -> Union[str, pathlib.Path, BinaryIO]:
        """
        Write table data to a Parquet file.

        Args:
            table_data: Table data to write
            output_path: Path or file-like object to write to
            compression: Compression method
            **options: Additional Parquet options

        Returns:
            Path to the written file or file-like object

        Raises:
            MissingDependencyError: If PyArrow is not available
            OutputError: If writing fails
        """
        if not pa:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support. "
                "Install with: pip install pyarrow"
            )

        try:
            # Use options or fall back to instance defaults
            compression_val = (
                compression if compression is not None else self.compression
            )

            # Handle empty data
            if not table_data:
                # Create an empty table
                empty_table = pa.table({})

                # Write to file or file-like object
                if isinstance(output_path, (str, pathlib.Path)):
                    # Convert Path to string if needed
                    path_str = str(output_path)

                    # Ensure directory exists
                    os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)
                    pq.write_table(empty_table, path_str, compression=compression_val)
                else:
                    pq.write_table(
                        empty_table, output_path, compression=compression_val
                    )

                return output_path

            # Convert data to PyArrow Table
            # Extract columns from dictionary
            columns = {}
            for key in table_data[0].keys():
                columns[key] = [record.get(key) for record in table_data]

            table = pa.table(columns)

            # Write to file or file-like object
            if isinstance(output_path, (str, pathlib.Path)):
                # Convert Path to string if needed
                path_str = str(output_path)

                # Ensure directory exists
                os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)
                pq.write_table(table, path_str, compression=compression_val, **options)
            else:
                pq.write_table(
                    table, output_path, compression=compression_val, **options
                )

            return output_path

        except Exception as e:
            logger.error(f"Error writing Parquet: {e}")
            raise OutputError(f"Failed to write Parquet file: {e}")

    def write_all_tables(
        self,
        main_table: List[JsonDict],
        child_tables: Dict[str, List[JsonDict]],
        base_path: Union[str, pathlib.Path],
        entity_name: str,
        **options,
    ) -> Dict[str, Union[str, pathlib.Path]]:
        """
        Write main and child tables to Parquet files.

        Args:
            main_table: The main table data
            child_tables: Dictionary of child tables
            base_path: Directory to write files to
            entity_name: Name of the entity (for main table filename)
            **options: Additional Parquet formatting options

        Returns:
            Dictionary mapping table names to file paths

        Raises:
            MissingDependencyError: If PyArrow is not available
            OutputError: If writing fails
        """
        if not pa:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support. "
                "Install with: pip install pyarrow"
            )

        results = {}

        # Ensure base directory exists
        base_path_str = str(base_path)
        os.makedirs(base_path_str, exist_ok=True)

        # Write main table
        if isinstance(base_path, pathlib.Path):
            main_path = base_path / f"{entity_name}.parquet"
        else:
            main_path = os.path.join(base_path_str, f"{entity_name}.parquet")

        self.write_table(main_table, main_path, **options)
        results["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Replace dots and slashes with underscores for file names
            safe_name = table_name.replace(".", "_").replace("/", "_")

            if isinstance(base_path, pathlib.Path):
                file_path = base_path / f"{safe_name}.parquet"
            else:
                file_path = os.path.join(base_path_str, f"{safe_name}.parquet")

            self.write_table(table_data, file_path, **options)
            results[table_name] = file_path

        return results

    @classmethod
    def is_available(cls) -> bool:
        """
        Check if this writer is available.

        Returns:
            bool: True if PyArrow is available
        """
        return pa is not None


class ParquetStreamingWriter(StreamingWriter):
    """
    Streaming writer for Parquet format using PyArrow.

    This writer allows incremental writing of data to Parquet files
    without keeping the entire dataset in memory.
    """

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        compression: str = "snappy",
        row_group_size: int = 10000,
        **options,
    ):
        """
        Initialize the Parquet streaming writer.

        Args:
            destination: Directory path for output files
            entity_name: Name of the entity being processed
            compression: Compression algorithm (snappy, gzip, brotli, zstd, or None)
            row_group_size: Number of rows per row group
            **options: Additional Parquet writer options

        Raises:
            MissingDependencyError: If PyArrow is not available
        """
        super().__init__(destination, entity_name, **options)

        if not pa:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support. "
                "Install with: pip install pyarrow"
            )

        self.compression = compression
        self.row_group_size = row_group_size
        self.options = options

        # Track writers and schemas for each table
        self.writers = {}
        self.schemas = {}

        # Track column names for each table to maintain consistent schema
        self.table_columns = {}

        # Buffers to accumulate records before writing row groups
        self.buffers = {}

        # Output file paths for reporting
        self.output_files = {}

        # Initialize the destination directory
        if isinstance(destination, str):
            os.makedirs(destination, exist_ok=True)
            self.base_path = destination
        else:
            # If it's a file-like object, we'll use it for the main table only
            # and warn about child tables
            self.base_path = None
            logger.warning(
                "File-like destination provided to ParquetStreamingWriter. "
                "Only the main table will be written to this destination. "
                "Child tables will be ignored."
            )

    def initialize_main_table(self) -> None:
        """
        Initialize the main table for streaming.
        This complies with the StreamingWriter interface.
        """
        # Initialize buffer for main table if not already done
        if "main" not in self.buffers:
            self.buffers["main"] = []

    def _get_table_path(self, table_name: str) -> str:
        """
        Get the file path for a table.

        Args:
            table_name: Name of the table

        Returns:
            File path for the table

        Raises:
            OutputError: If base_path is None (file-like destination)
        """
        if self.base_path is None:
            raise OutputError(
                "Cannot write multiple tables when destination is a file-like object"
            )

        # For the main table, use the entity name
        if table_name == "main":
            return os.path.join(self.base_path, f"{self.entity_name}.parquet")

        # For child tables, use the sanitized table name
        safe_name = table_name.replace(".", "_").replace("/", "_")
        return os.path.join(self.base_path, f"{safe_name}.parquet")

    def _create_schema(self, records: List[JsonDict], table_name: str) -> pa.Schema:
        """
        Create a PyArrow schema from sample records.

        Args:
            records: Sample records to infer schema from
            table_name: Name of the table

        Returns:
            PyArrow Schema object
        """
        # Get all column names from the records
        all_columns = set()
        for record in records:
            all_columns.update(record.keys())

        # If we already have columns for this table, make sure we include them
        if table_name in self.table_columns:
            all_columns.update(self.table_columns[table_name])

        # Store column names for this table
        self.table_columns[table_name] = sorted(list(all_columns))

        # Create a sample with one record to infer types
        sample_data = {}
        for col in all_columns:
            # Find first non-None value for each column
            sample_value = None
            for record in records:
                if col in record and record[col] is not None:
                    sample_value = record[col]
                    break

            # Use empty list if no values found
            sample_data[col] = [sample_value] if sample_value is not None else [None]

        # Create a sample table to infer schema
        sample_table = pa.table(sample_data)
        return sample_table.schema

    def _records_to_table(self, records: List[JsonDict], table_name: str) -> pa.Table:
        """
        Convert records to a PyArrow Table with consistent schema.

        Args:
            records: Records to convert
            table_name: Name of the table

        Returns:
            PyArrow Table
        """
        if not records:
            # Return empty table with schema if available
            if table_name in self.schemas:
                return pa.table({}, schema=self.schemas[table_name])
            return pa.table({})

        # Ensure we have all expected columns, filling missing values with None
        columns = {}
        for col in self.table_columns.get(table_name, []):
            columns[col] = [record.get(col) for record in records]

        # Create the table
        return pa.table(columns, schema=self.schemas.get(table_name))

    def _initialize_writer(
        self, table_name: str, schema: pa.Schema, append: bool = False
    ) -> None:
        """
        Initialize a Parquet writer for a table.

        Args:
            table_name: Name of the table
            schema: PyArrow Schema for the table
            append: Whether to append to an existing file
        """
        # Get the output path
        if table_name not in self.output_files:
            if table_name == "main" and self.base_path is None:
                # Using provided file-like object for main table
                output_path = self.destination
            else:
                output_path = self._get_table_path(table_name)
                self.output_files[table_name] = output_path
        else:
            output_path = self.output_files[table_name]

        # Check if we should append to an existing file
        if append and isinstance(output_path, str) and os.path.exists(output_path):
            try:
                # Open the existing file to get metadata
                existing_file = pq.ParquetFile(output_path)

                # Create writer in append mode
                writer = pq.ParquetWriter(
                    output_path,
                    schema=schema,
                    compression=self.compression,
                    append=True,
                    **self.options,
                )
            except Exception as e:
                # If append fails for any reason, create a new file
                logger.warning(
                    f"Failed to append to existing Parquet file: {e}. Creating new file."
                )
                writer = pq.ParquetWriter(
                    output_path,
                    schema=schema,
                    compression=self.compression,
                    **self.options,
                )
        else:
            # Create a new writer
            writer = pq.ParquetWriter(
                output_path, schema=schema, compression=self.compression, **self.options
            )

        self.writers[table_name] = writer
        self.schemas[table_name] = schema
        if table_name not in self.buffers:
            self.buffers[table_name] = []

    def _write_buffer(self, table_name: str) -> None:
        """
        Write buffered records to a row group.

        Args:
            table_name: Name of the table
        """
        if not self.buffers.get(table_name, []):
            return

        # Convert records to table
        table = self._records_to_table(self.buffers[table_name], table_name)

        # Write the table as a row group
        if table_name in self.writers:
            self.writers[table_name].write_table(table)

            # Update schema if it has changed
            if table_name in self.schemas:
                self.schemas[table_name] = table.schema

        # Clear the buffer
        self.buffers[table_name] = []

    def write_main_records(self, records: List[JsonDict]) -> None:
        """
        Write a batch of main records.

        Args:
            records: List of main table records to write
        """
        if not records:
            return

        table_name = "main"

        # Initialize writer if needed
        if table_name not in self.writers:
            schema = self._create_schema(records, table_name)
            self._initialize_writer(table_name, schema)
        else:
            # Check if we need to update schema with new columns
            new_columns = set()
            for record in records:
                for key in record.keys():
                    if key not in self.table_columns.get(table_name, []):
                        new_columns.add(key)

            if new_columns:
                # Need to handle schema evolution
                # First flush any buffered records with the old schema
                if self.buffers.get(table_name, []):
                    self._write_buffer(table_name)

                # Close the old writer
                if table_name in self.writers:
                    self.writers[table_name].close()
                    del self.writers[table_name]

                # Update table columns
                self.table_columns[table_name] = sorted(
                    list(set(self.table_columns[table_name]) | new_columns)
                )

                # Create updated schema
                schema = self._create_schema(records, table_name)

                # Create a new writer with the updated schema
                # But append to the existing file
                output_path = self.output_files.get(table_name)
                self._initialize_writer(table_name, schema, append=True)

        # Add records to buffer
        self.buffers[table_name].extend(records)

        # Write if buffer exceeds row group size
        if len(self.buffers[table_name]) >= self.row_group_size:
            self._write_buffer(table_name)

    def initialize_child_table(self, table_name: str) -> None:
        """
        Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
        """
        # Initialization happens when first records are written
        self.buffers.setdefault(table_name, [])

    def write_child_records(self, table_name: str, records: List[JsonDict]) -> None:
        """
        Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: List of child records to write
        """
        if not records or self.base_path is None:
            return

        # Initialize writer if needed
        if table_name not in self.writers and table_name not in self.buffers:
            self.initialize_child_table(table_name)

        if table_name not in self.writers and records:
            schema = self._create_schema(records, table_name)
            self._initialize_writer(table_name, schema)
        elif table_name in self.writers:
            # Check if we need to update schema with new columns
            new_columns = set()
            for record in records:
                for key in record.keys():
                    if key not in self.table_columns.get(table_name, []):
                        new_columns.add(key)

            if new_columns:
                # Need to handle schema evolution
                # First flush any buffered records with the old schema
                if self.buffers.get(table_name, []):
                    self._write_buffer(table_name)

                # Close the old writer
                if table_name in self.writers:
                    self.writers[table_name].close()
                    del self.writers[table_name]

                # Update table columns
                self.table_columns[table_name] = sorted(
                    list(set(self.table_columns[table_name]) | new_columns)
                )

                # Create updated schema
                schema = self._create_schema(records, table_name)

                # Create a new writer with the updated schema
                # But append to the existing file
                self._initialize_writer(table_name, schema, append=True)

        # Add records to buffer
        self.buffers[table_name].extend(records)

        # Write if buffer exceeds row group size
        if len(self.buffers[table_name]) >= self.row_group_size:
            self._write_buffer(table_name)

    def finalize(self) -> None:
        """
        Finalize all tables and close writers.
        """
        # Write any remaining records in buffers
        for table_name in list(self.buffers.keys()):
            if self.buffers[table_name]:
                # Initialize writer if needed (possible if only a few records)
                if table_name not in self.writers and self.buffers[table_name]:
                    schema = self._create_schema(self.buffers[table_name], table_name)
                    self._initialize_writer(table_name, schema)

                # Write remaining records
                self._write_buffer(table_name)

        # Close all writers
        for writer in self.writers.values():
            writer.close()

        # Clear buffers
        self.buffers.clear()

        # Mark as finalized
        self.writers.clear()
        self.initialized = False


# Register the writer
register_writer("parquet", ParquetWriter)
register_streaming_writer("parquet", ParquetStreamingWriter)
