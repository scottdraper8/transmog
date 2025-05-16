"""Parquet writer for Transmog output.

This module provides a Parquet writer using PyArrow.
"""

import os
import pathlib
from typing import Any, BinaryIO, Optional, TextIO, Union

from transmog.error import MissingDependencyError, OutputError, logger
from transmog.io.writer_factory import register_streaming_writer, register_writer
from transmog.io.writer_interface import DataWriter, StreamingWriter
from transmog.types.base import JsonDict

# Import PyArrow conditionally
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None


class ParquetWriter(DataWriter):
    """Parquet format writer.

    This writer handles writing flattened data to Parquet format files.
    Requires PyArrow to be installed.
    """

    def __init__(self, compression: str = "snappy", **options: Any) -> None:
        """Initialize the Parquet writer.

        Args:
            compression: Compression format (snappy, gzip, brotli, etc.)
            **options: Additional Parquet writer options
        """
        self.compression = compression
        self.options = options

    @classmethod
    def format_name(cls) -> str:
        """Get the format name for this writer.

        Returns:
            str: The format name ("parquet")
        """
        return "parquet"

    def write(
        self, data: Any, destination: Union[str, pathlib.Path, BinaryIO], **options: Any
    ) -> Any:
        """Write data to the specified destination.

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
        table_data: list[JsonDict],
        output_path: Union[str, pathlib.Path, BinaryIO],
        compression: Optional[str] = None,
        **options: Any,
    ) -> Union[str, pathlib.Path, BinaryIO]:
        """Write table data to a Parquet file.

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
        if pa is None or pq is None:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support. "
                "Install with: pip install pyarrow",
                package="pyarrow",
            )

        try:
            # Use options or fall back to instance defaults
            compression_val = (
                compression if compression is not None else self.compression
            )

            # Handle empty data
            if not table_data:
                # Create an empty table
                if pa is not None:
                    empty_table = pa.table({})
                else:
                    raise MissingDependencyError(
                        "PyArrow is required for Parquet support.",
                        package="pyarrow",
                    )

                # Write to file or file-like object
                if isinstance(output_path, (str, pathlib.Path)):
                    # Convert Path to string if needed
                    path_str = str(output_path)

                    # Ensure directory exists
                    os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)
                    if pq is not None:
                        pq.write_table(
                            empty_table, path_str, compression=compression_val
                        )
                else:
                    if pq is not None:
                        pq.write_table(
                            empty_table, output_path, compression=compression_val
                        )

                return output_path

            # Convert data to PyArrow Table
            # Extract columns from dictionary
            columns: dict[str, list[Any]] = {}
            for key in table_data[0].keys():
                columns[key] = [record.get(key) for record in table_data]

            if pa is not None:
                table = pa.table(columns)
            else:
                raise MissingDependencyError(
                    "PyArrow is required for Parquet support.",
                    package="pyarrow",
                )

            # Write to file or file-like object
            if isinstance(output_path, (str, pathlib.Path)):
                # Convert Path to string if needed
                path_str = str(output_path)

                # Ensure directory exists
                os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)
                if pq is not None:
                    pq.write_table(
                        table, path_str, compression=compression_val, **options
                    )
            else:
                if pq is not None:
                    pq.write_table(
                        table, output_path, compression=compression_val, **options
                    )

            return output_path

        except Exception as e:
            logger.error(f"Error writing Parquet: {e}")
            raise OutputError(f"Failed to write Parquet file: {e}") from e

    def write_all_tables(
        self,
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        base_path: Union[str, pathlib.Path],
        entity_name: str,
        **options: Any,
    ) -> dict[str, Union[str, pathlib.Path]]:
        """Write main and child tables to Parquet files.

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
        if pa is None:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support. "
                "Install with: pip install pyarrow",
                package="pyarrow",
            )

        results: dict[str, Union[str, pathlib.Path]] = {}

        # Ensure base directory exists
        base_path_str = str(base_path)
        os.makedirs(base_path_str, exist_ok=True)

        # Write main table
        if isinstance(base_path, pathlib.Path):
            main_path: Union[str, pathlib.Path] = base_path / f"{entity_name}.parquet"
        else:
            main_path = os.path.join(base_path_str, f"{entity_name}.parquet")

        self.write_table(main_table, main_path, **options)
        results["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Replace dots and slashes with underscores for file names
            safe_name = table_name.replace(".", "_").replace("/", "_")

            if isinstance(base_path, pathlib.Path):
                file_path: Union[str, pathlib.Path] = base_path / f"{safe_name}.parquet"
            else:
                file_path = os.path.join(base_path_str, f"{safe_name}.parquet")

            self.write_table(table_data, file_path, **options)
            results[table_name] = file_path

        return results

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer is available.

        Returns:
            bool: True if PyArrow is available
        """
        return pa is not None


class ParquetStreamingWriter(StreamingWriter):
    """Streaming writer for Parquet format using PyArrow.

    This writer allows incremental writing of data to Parquet files
    without keeping the entire dataset in memory.
    """

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        compression: str = "snappy",
        row_group_size: int = 10000,
        **options: Any,
    ) -> None:
        """Initialize the Parquet streaming writer.

        Args:
            destination: Path or file-like object to write to
            entity_name: Name of the entity for output files
            compression: Compression algorithm ("snappy", "gzip", etc.)
            row_group_size: Number of records per row group
            **options: Additional options for PyArrow
        """
        # Initialize destination attribute with correct type
        self.destination: Optional[Union[str, BinaryIO, TextIO]] = None
        self.base_path: Optional[str] = None

        # Handle file-like objects
        if destination is not None and hasattr(destination, "write"):
            self.base_path = None
            self.destination = destination
            # For file-like objects, only the main table can be written
            logger.warning(
                "File-like destination provided to ParquetStreamingWriter. "
                "Only the main table will be written to this destination. "
                "Child tables will be ignored."
            )
        else:
            # Directory path
            self.base_path = str(destination) if destination is not None else None
            self.destination = destination

        self.entity_name = entity_name
        self.compression = compression
        self.row_group_size = row_group_size
        self.options = options

        # Storage for data
        self.buffers: dict[str, list[JsonDict]] = {}
        self.writers: dict[str, Any] = {}
        self.schemas: dict[str, Any] = {}
        self.table_columns: dict[str, list[str]] = {}
        self.output_files: dict[str, str] = {}

        if pa is None:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support. "
                "Install with: pip install pyarrow",
                package="pyarrow",
            )

    def initialize_main_table(self) -> None:
        """Initialize the main table for streaming.

        This complies with the StreamingWriter interface.
        """
        # Initialize buffer for main table if not already done
        if "main" not in self.buffers:
            self.buffers["main"] = []

    def _get_table_path(self, table_name: str) -> Optional[str]:
        """Get the file path for a table.

        Args:
            table_name: Name of the table

        Returns:
            File path for the table, or None for file-like destination
        """
        if self.base_path is None:
            # When using a file-like object, only the main table can be written
            # Return None to indicate this is a file-like destination
            if table_name != "main":
                logger.warning(
                    f"Attempting to write table {table_name} when destination is a "
                    f"file-like object. Only the main table will be written."
                )
            return None

        # For the main table, use the entity name
        if table_name == "main":
            return os.path.join(self.base_path, f"{self.entity_name}.parquet")

        # For child tables, use the sanitized table name
        safe_name = table_name.replace(".", "_").replace("/", "_")
        return os.path.join(self.base_path, f"{safe_name}.parquet")

    def _create_schema(self, records: list[JsonDict], table_name: str) -> Any:
        """Create a PyArrow schema from sample records.

        Args:
            records: Sample records to infer schema from
            table_name: Name of the table

        Returns:
            PyArrow Schema object
        """
        # Get all column names from the records
        all_columns: set[str] = set()
        for record in records:
            all_columns.update(record.keys())

        # If we already have columns for this table, make sure we include them
        if table_name in self.table_columns:
            all_columns.update(self.table_columns[table_name])

        # Store column names for this table
        self.table_columns[table_name] = sorted(all_columns)

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
        if pa is not None:
            sample_table = pa.table(sample_data)
            return sample_table.schema
        else:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support.",
                package="pyarrow",
            )

    def _records_to_table(self, records: list[JsonDict], table_name: str) -> Any:
        """Convert records to a PyArrow Table with consistent schema.

        Args:
            records: Records to convert
            table_name: Name of the table

        Returns:
            PyArrow Table
        """
        if not records:
            # Return empty table with schema if available
            if table_name in self.schemas and pa is not None:
                return pa.Table.from_arrays([], schema=self.schemas[table_name])
            else:
                # No schema available, create a minimal table
                if pa is not None:
                    return pa.table({})
                else:
                    raise MissingDependencyError(
                        "PyArrow is required for Parquet support.",
                        package="pyarrow",
                    )

        # Ensure we have a schema for this table
        if table_name not in self.schemas:
            schema = self._create_schema(records, table_name)
            self.schemas[table_name] = schema

        # Get all columns for this table
        if table_name not in self.table_columns:
            # If we don't have stored columns yet, extract from the schema
            if pa is not None and table_name in self.schemas:
                self.table_columns[table_name] = self.schemas[table_name].names
            else:
                # Get all columns from the records
                all_columns: set[str] = set()
                for record in records:
                    all_columns.update(record.keys())
                self.table_columns[table_name] = sorted(all_columns)

        # Extract values for all columns
        data = {}
        for col in self.table_columns[table_name]:
            data[col] = [record.get(col) for record in records]

        # Create a PyArrow table
        if pa is not None:
            return pa.table(data, schema=self.schemas.get(table_name))
        else:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support.",
                package="pyarrow",
            )

    def _initialize_writer(
        self, table_name: str, schema: Any, append: bool = False
    ) -> None:
        """Initialize a Parquet writer for a table.

        Args:
            table_name: Name of the table
            schema: PyArrow schema
            append: Whether to append to an existing file

        Raises:
            OutputError: If trying to write a non-main table to a file-like object
        """
        # Store the schema
        self.schemas[table_name] = schema

        # Handle file-like object destination (can only write main table)
        if self.destination is not None and hasattr(self.destination, "write"):
            if table_name != "main":
                logger.warning(
                    f"Cannot write {table_name} table to file-like object, "
                    f"only main table is supported."
                )
                return

            # Write main table to the file-like object
            if pq is not None:
                self.writers[table_name] = pq.ParquetWriter(
                    self.destination,
                    schema,
                    compression=self.compression,
                    **self.options,
                )
            else:
                raise MissingDependencyError(
                    "PyArrow is required for Parquet support.",
                    package="pyarrow",
                )
            return

        # For directory destination, get the file path for this table
        file_path = self._get_table_path(table_name)

        # Skip if we can't write this table (file-like destination for non-main table)
        if file_path is None:
            return

        self.output_files[table_name] = file_path

        # Create a file writer
        if pq is not None:
            if append and os.path.exists(file_path):
                self.writers[table_name] = pq.ParquetWriter(
                    file_path,
                    schema,
                    compression=self.compression,
                    append=True,
                    **self.options,
                )
            else:
                self.writers[table_name] = pq.ParquetWriter(
                    file_path,
                    schema,
                    compression=self.compression,
                    **self.options,
                )
        else:
            raise MissingDependencyError(
                "PyArrow is required for Parquet support.",
                package="pyarrow",
            )

    def _write_buffer(self, table_name: str) -> None:
        """Write buffered records to a table.

        Args:
            table_name: Name of the table
        """
        if table_name not in self.buffers or not self.buffers[table_name]:
            return  # Nothing to write

        # For file-like destination, only write main table
        if self.base_path is None and table_name != "main":
            # Clear the buffer without writing
            self.buffers[table_name] = []
            return

        if table_name not in self.writers:
            # First time writing this table
            records = self.buffers[table_name]
            if not records:
                return  # No records to write

            # Create a table from the records
            table = self._records_to_table(records, table_name)

            # Initialize the writer with the table's schema
            if table is not None:
                self._initialize_writer(table_name, table.schema)

            # Write the records if we were able to initialize a writer
            if table_name in self.writers and self.writers[table_name] is not None:
                self.writers[table_name].write_table(table)
        else:
            # Writer already initialized, just write records
            records = self.buffers[table_name]
            if records:
                table = self._records_to_table(records, table_name)
                if table_name in self.writers and self.writers[table_name] is not None:
                    self.writers[table_name].write_table(table)

        # Clear the buffer
        self.buffers[table_name] = []

    def write_main_records(self, records: list[JsonDict]) -> None:
        """Write records to the main table.

        Args:
            records: Records to write
        """
        if not records:
            return

        # Add records to the buffer
        if "main" not in self.buffers:
            self.buffers["main"] = []
        self.buffers["main"].extend(records)

        # Write a row group if we've reached the threshold
        if len(self.buffers["main"]) >= self.row_group_size:
            self._write_buffer("main")

    def initialize_child_table(self, table_name: str) -> None:
        """Initialize a child table for writing.

        Args:
            table_name: Name of the child table
        """
        # Initialize buffer for this table
        if table_name not in self.buffers:
            self.buffers[table_name] = []

    def write_child_records(self, table_name: str, records: list[JsonDict]) -> None:
        """Write records to a child table.

        Args:
            table_name: Name of the child table
            records: Records to write
        """
        if not records:
            return

        # Initialize if not done already
        if table_name not in self.buffers:
            self.initialize_child_table(table_name)

        # Add records to the buffer
        self.buffers[table_name].extend(records)

        # Write a row group if we've reached the threshold
        if len(self.buffers[table_name]) >= self.row_group_size:
            self._write_buffer(table_name)

    def finalize(self) -> None:
        """Finalize all tables, writing any remaining records.

        This complies with the StreamingWriter interface.
        """
        # Write any remaining buffered records for all tables
        for table_name in list(self.buffers.keys()):
            self._write_buffer(table_name)

        # Close all writers
        self.close()

    def close(self) -> None:
        """Close all writers.

        This releases resources and finalizes files.
        """
        # Close all open writers
        for writer in self.writers.values():
            if writer is not None:
                writer.close()

        # Clear writers and buffers
        self.writers.clear()
        self.buffers.clear()

    def get_output_files(self) -> dict[str, str]:
        """Get the output file paths for all written tables.

        Returns:
            Dictionary mapping table names to output file paths
        """
        return self.output_files


# Register the writer
register_writer("parquet", ParquetWriter)
register_streaming_writer("parquet", ParquetStreamingWriter)
