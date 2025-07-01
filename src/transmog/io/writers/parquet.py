"""Parquet writer for Transmog output.

This module provides a Parquet writer using PyArrow with unified interface
and native compression support.
"""

import os
import pathlib
from typing import Any, BinaryIO, Optional, TextIO, Union

from transmog.error import MissingDependencyError, OutputError, logger
from transmog.io.writer_interface import DataWriter, StreamingWriter, WriterUtils
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

    This writer handles writing flattened data to Parquet format files with
    unified interface and native compression support via PyArrow.
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

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer is available.

        Returns:
            bool: True if PyArrow is available
        """
        return pa is not None

    def supports_compression(self) -> bool:
        """Check if this writer supports compression.

        Returns:
            bool: True as Parquet writer supports multiple compression codecs
        """
        return True

    def get_supported_codecs(self) -> list[str]:
        """Get list of supported compression codecs.

        Returns:
            list[str]: List of supported compression methods
        """
        return ["snappy", "gzip", "brotli", "lz4", "zstd", "none"]

    def write_table(
        self,
        table_data: list[JsonDict],
        output_path: Union[str, BinaryIO, TextIO],
        **format_options: Any,
    ) -> Union[str, BinaryIO, TextIO]:
        """Write table data to a Parquet file.

        Args:
            table_data: Table data to write
            output_path: Path or file-like object to write to
            **format_options: Format-specific options (compression, etc.)

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
            compression_val = format_options.get("compression", self.compression)

            # Handle empty data
            if not table_data:
                empty_table = pa.table({})

                # Write to file or file-like object
                if isinstance(output_path, (str, pathlib.Path)):
                    path_str = str(output_path)
                    os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)
                    pq.write_table(empty_table, path_str, compression=compression_val)
                    return (
                        pathlib.Path(path_str)
                        if isinstance(output_path, pathlib.Path)
                        else path_str
                    )
                else:
                    # For file-like objects, TextIO is not supported by PyArrow
                    if hasattr(output_path, "mode") and "b" not in getattr(
                        output_path, "mode", ""
                    ):
                        raise OutputError(
                            "Parquet format requires binary streams, "
                            "text streams not supported"
                        )
                    pq.write_table(
                        empty_table, output_path, compression=compression_val
                    )
                    return output_path

            # Convert data to PyArrow Table
            columns: dict[str, list[Any]] = {}
            for key in table_data[0].keys():
                columns[key] = [record.get(key) for record in table_data]

            table = pa.table(columns)

            # Write to file or file-like object
            if isinstance(output_path, (str, pathlib.Path)):
                path_str = str(output_path)
                os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)
                pq.write_table(
                    table, path_str, compression=compression_val, **format_options
                )
                return (
                    pathlib.Path(path_str)
                    if isinstance(output_path, pathlib.Path)
                    else path_str
                )
            else:
                # For file-like objects, TextIO is not supported by PyArrow
                if hasattr(output_path, "mode") and "b" not in getattr(
                    output_path, "mode", ""
                ):
                    raise OutputError(
                        "Parquet format requires binary streams, "
                        "text streams not supported"
                    )
                pq.write_table(
                    table, output_path, compression=compression_val, **format_options
                )
                return output_path

        except Exception as e:
            logger.error(f"Error writing Parquet: {e}")
            raise OutputError(f"Failed to write Parquet file: {e}") from e

    def write_all_tables(
        self,
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        base_path: Union[str],
        entity_name: str,
        **options: Any,
    ) -> dict[str, str]:
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

        # Write main table
        main_filename = WriterUtils.sanitize_filename(entity_name)
        main_path = WriterUtils.build_output_path(
            base_path, main_filename, "parquet", None
        )
        self.write_table(main_table, main_path, **options)

        # Write child tables
        paths = {"main": main_path}
        for table_name, table_data in child_tables.items():
            child_filename = WriterUtils.sanitize_filename(table_name)
            child_path = WriterUtils.build_output_path(
                base_path, child_filename, "parquet", None
            )
            self.write_table(table_data, child_path, **options)
            paths[table_name] = child_path

        return paths


class ParquetStreamingWriter(StreamingWriter):
    """Streaming writer for Parquet format using PyArrow.

    This writer allows incremental writing of data to Parquet files
    without keeping the entire dataset in memory, with unified interface.
    """

    @classmethod
    def format_name(cls) -> str:
        """Get the format name for this writer.

        Returns:
            str: The format name ("parquet")
        """
        return "parquet"

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        compression: str = "snappy",
        row_group_size: int = 10000,
        buffer_size: int = 1000,
        **options: Any,
    ) -> None:
        """Initialize the Parquet streaming writer.

        Args:
            destination: Path or file-like object to write to
            entity_name: Name of the entity for output files
            compression: Compression algorithm ("snappy", "gzip", etc.)
            row_group_size: Number of records per row group
            buffer_size: Number of records to buffer before writing
            **options: Additional options for PyArrow
        """
        super().__init__(destination, entity_name, buffer_size, **options)

        if pa is None:
            raise MissingDependencyError(
                "PyArrow is required for Parquet streaming support. "
                "Install with: pip install pyarrow",
                package="pyarrow",
            )

        self.compression = compression
        self.row_group_size = row_group_size
        self.writers: dict[str, Any] = {}
        self.schemas: dict[str, Any] = {}
        self.buffers: dict[str, list[JsonDict]] = {}
        self.base_dir: Optional[str] = None
        self.file_paths: dict[str, str] = {}
        self.finalized: bool = False

        # Initialize destination
        if isinstance(destination, str):
            # Directory path
            self.base_dir = destination
            os.makedirs(self.base_dir, exist_ok=True)
        elif destination is not None:
            # For file-like objects, warn about limitations
            if hasattr(destination, "mode") and "b" not in getattr(
                destination, "mode", ""
            ):
                logger.warning(
                    "Parquet streaming writer requires binary streams "
                    "for file-like objects"
                )

    def initialize_main_table(self, **options: Any) -> None:
        """Initialize the main table for streaming.

        Args:
            **options: Format-specific options
        """
        # Main table initialization happens when first records are written
        pass

    def _get_table_path(self, table_name: str) -> Optional[str]:
        """Get the file path for a table.

        Args:
            table_name: Name of the table

        Returns:
            Optional[str]: File path or None if using file-like object
        """
        if self.base_dir is None:
            return None

        if table_name in self.file_paths:
            return self.file_paths[table_name]

        if table_name == "main":
            file_path = os.path.join(self.base_dir, f"{self.entity_name}.parquet")
        else:
            safe_name = table_name.replace(".", "_").replace("/", "_")
            file_path = os.path.join(self.base_dir, f"{safe_name}.parquet")

        self.file_paths[table_name] = file_path
        return file_path

    def _create_schema(self, records: list[JsonDict], table_name: str) -> Any:
        """Create PyArrow schema from records.

        Args:
            records: Sample records to infer schema from
            table_name: Name of the table

        Returns:
            PyArrow schema
        """
        if not records:
            return pa.schema([])

        # Infer schema from first record
        sample_record = records[0]
        fields = []

        for key, value in sample_record.items():
            if value is None:
                # Default to string for null values
                pa_type = pa.string()
            elif isinstance(value, bool):
                pa_type = pa.bool_()
            elif isinstance(value, int):
                pa_type = pa.int64()
            elif isinstance(value, float):
                pa_type = pa.float64()
            elif isinstance(value, str):
                pa_type = pa.string()
            else:
                # Default to string for complex types
                pa_type = pa.string()

            fields.append(pa.field(key, pa_type))

        return pa.schema(fields)

    def _records_to_table(self, records: list[JsonDict], table_name: str) -> Any:
        """Convert records to PyArrow table.

        Args:
            records: Records to convert
            table_name: Name of the table

        Returns:
            PyArrow table
        """
        if not records:
            return pa.table({})

        # Get or create schema
        if table_name not in self.schemas:
            self.schemas[table_name] = self._create_schema(records, table_name)

        schema = self.schemas[table_name]

        # Convert records to columnar format
        columns: dict[str, list[Any]] = {}
        for field in schema:
            field_name = field.name
            columns[field_name] = []

        for record in records:
            for field in schema:
                field_name = field.name
                value = record.get(field_name)

                # Handle type conversion
                if value is None:
                    columns[field_name].append(None)
                elif field.type == pa.bool_():
                    columns[field_name].append(
                        bool(value) if value is not None else None
                    )
                elif field.type == pa.int64():
                    try:
                        columns[field_name].append(
                            int(value) if value is not None else None
                        )
                    except (ValueError, TypeError):
                        columns[field_name].append(None)
                elif field.type == pa.float64():
                    try:
                        columns[field_name].append(
                            float(value) if value is not None else None
                        )
                    except (ValueError, TypeError):
                        columns[field_name].append(None)
                else:
                    # String or default
                    columns[field_name].append(
                        str(value) if value is not None else None
                    )

        # Create arrays and table
        arrays = []
        for field in schema:
            field_name = field.name
            arrays.append(pa.array(columns[field_name], type=field.type))

        return pa.table(arrays, schema=schema)

    def _initialize_writer(
        self, table_name: str, schema: Any, append: bool = False
    ) -> None:
        """Initialize a Parquet writer for a table.

        Args:
            table_name: Name of the table
            schema: PyArrow schema
            append: Whether to append to existing file
        """
        file_path = self._get_table_path(table_name)

        if file_path is None:
            # Cannot create streaming writer without file path
            logger.warning(
                f"Cannot create Parquet streaming writer for table "
                f"{table_name} without file path"
            )
            return

        try:
            if append and os.path.exists(file_path):
                # For append mode, we would need to read existing schema
                # For simplicity, we'll overwrite for now
                pass

            writer = pq.ParquetWriter(
                file_path, schema, compression=self.compression, **self.options
            )
            self.writers[table_name] = writer
            self.schemas[table_name] = schema

        except Exception as e:
            logger.error(f"Failed to initialize Parquet writer for {table_name}: {e}")
            raise OutputError(f"Failed to initialize Parquet writer: {e}") from e

    def _write_buffer(self, table_name: str) -> None:
        """Write buffered records to file.

        Args:
            table_name: Name of the table
        """
        if table_name not in self.buffers or not self.buffers[table_name]:
            return

        records = self.buffers[table_name]
        table = self._records_to_table(records, table_name)

        # Initialize writer if needed
        if table_name not in self.writers:
            self._initialize_writer(table_name, table.schema)

        # Write table
        writer = self.writers.get(table_name)
        if writer:
            writer.write_table(table)

        # Clear buffer and report progress
        self.buffers[table_name] = []
        self._report_progress(table_name, len(records))

    def write_main_records(self, records: list[JsonDict], **options: Any) -> None:
        """Write a batch of main records.

        Args:
            records: List of main table records to write
            **options: Format-specific options
        """
        if not records:
            return

        table_name = "main"

        # Initialize buffer if needed
        if table_name not in self.buffers:
            self.buffers[table_name] = []

        # Add records to buffer
        self.buffers[table_name].extend(records)

        # Write buffer if it's full
        if len(self.buffers[table_name]) >= self.row_group_size:
            self._write_buffer(table_name)

        # Update record count
        self.record_counts[table_name] = self.record_counts.get(table_name, 0) + len(
            records
        )

    def initialize_child_table(self, table_name: str, **options: Any) -> None:
        """Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
            **options: Format-specific options
        """
        # Child table initialization happens when first records are written
        pass

    def write_child_records(
        self, table_name: str, records: list[JsonDict], **options: Any
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: List of child records to write
            **options: Format-specific options
        """
        if not records:
            return

        # Initialize buffer if needed
        if table_name not in self.buffers:
            self.buffers[table_name] = []

        # Add records to buffer
        self.buffers[table_name].extend(records)

        # Write buffer if it's full
        if len(self.buffers[table_name]) >= self.row_group_size:
            self._write_buffer(table_name)

        # Update record count
        self.record_counts[table_name] = self.record_counts.get(table_name, 0) + len(
            records
        )

    def finalize(self, **options: Any) -> None:
        """Finalize the output and close all writers.

        Args:
            **options: Format-specific options
        """
        if self.finalized:
            return

        # Write any remaining buffered data
        for table_name in list(self.buffers.keys()):
            if self.buffers[table_name]:
                self._write_buffer(table_name)

        # Close all writers
        for writer in self.writers.values():
            if hasattr(writer, "close"):
                writer.close()

        self.finalized = True

    def close(self) -> None:
        """Clean up resources and close all writers."""
        if not self.finalized:
            self.finalize()

        # Clear references
        self.writers.clear()
        self.schemas.clear()
        self.buffers.clear()

    def get_output_files(self) -> dict[str, str]:
        """Get the output file paths.

        Returns:
            dict[str, str]: Mapping of table names to file paths
        """
        return self.file_paths.copy()
