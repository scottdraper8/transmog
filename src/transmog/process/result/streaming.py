"""Streaming functionality for ProcessingResult.

Contains methods for streaming result data to various output destinations
with memory-efficient processing.
"""

import os
from typing import TYPE_CHECKING, Any, BinaryIO, Optional, Union

from transmog.error.exceptions import MissingDependencyError, OutputError
from transmog.io.writer_factory import create_streaming_writer, is_format_available

from .utils import _check_pyarrow_available

if TYPE_CHECKING:
    from .core import ProcessingResult


class ResultStreaming:
    """Handles streaming ProcessingResult data to output destinations."""

    def __init__(self, result: "ProcessingResult"):
        """Initialize with a ProcessingResult instance.

        Args:
            result: ProcessingResult instance to stream
        """
        self.result = result

    def stream_to_parquet(
        self,
        base_path: str,
        compression: str = "snappy",
        row_group_size: int = 10000,
        **kwargs: Any,
    ) -> dict[str, str]:
        """Stream all tables to Parquet files with memory-efficient processing.

        Args:
            base_path: Base path for output files
            compression: Compression algorithm (snappy, gzip, brotli, lz4)
            row_group_size: Number of rows per row group
            **kwargs: Additional Parquet writer options

        Returns:
            Dictionary of table names to file paths

        Raises:
            MissingDependencyError: If PyArrow is not available
            OutputError: If streaming fails
        """
        if not _check_pyarrow_available():
            raise MissingDependencyError(
                "PyArrow is required for Parquet streaming",
                package="pyarrow",
                feature="parquet",
            )

        # Create the base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Get all tables
        tables = {"main": self.result.main_table, **self.result.child_tables}

        # Keep track of the paths
        file_paths: dict[str, str] = {}

        try:
            import pyarrow.parquet as pq

            # Process each table
            for table_name, records in tables.items():
                # Skip empty tables
                if not records:
                    continue

                # Create the formatted table name
                formatted_name = self.result.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.parquet")

                # Convert records to PyArrow table
                arrow_table = self.result._dict_list_to_pyarrow(records)

                # Create a ParquetWriter for streaming
                with pq.ParquetWriter(
                    file_path, arrow_table.schema, compression=compression, **kwargs
                ) as writer:
                    # Write data in chunks for memory efficiency
                    total_rows = len(arrow_table)
                    for start_idx in range(0, total_rows, row_group_size):
                        end_idx = min(start_idx + row_group_size, total_rows)
                        chunk = arrow_table.slice(start_idx, end_idx - start_idx)
                        writer.write_table(chunk)

                # Record the file path
                file_paths[table_name] = file_path

            return file_paths
        except Exception as e:
            raise OutputError(
                f"Failed to stream Parquet files: {e}",
                output_format="parquet",
                path=base_path,
            ) from e

    def stream_to_output(
        self,
        format_name: str,
        output_destination: Optional[Union[str, BinaryIO]] = None,
        **options: Any,
    ) -> None:
        """Stream results to an output destination.

        Args:
            format_name: Format to stream in
            output_destination: Output destination (file path or file-like object)
            **options: Format-specific options

        Raises:
            OutputError: If streaming fails
        """
        if not is_format_available(format_name):
            raise OutputError(
                f"Format '{format_name}' is not available",
                output_format=format_name,
            )

        # Create a writer for the format
        writer = create_streaming_writer(format_name, destination=output_destination)

        try:
            # Write all tables
            if hasattr(writer, "write_all_tables"):
                writer.write_all_tables(
                    self.result.main_table,
                    self.result.child_tables,
                    self.result.entity_name,
                    **options,
                )
            else:
                # Fall back to individual writes
                writer.write_main_records(self.result.main_table, **options)
                for table_name, table_data in self.result.child_tables.items():
                    writer.write_child_records(table_name, table_data, **options)
        except Exception as e:
            raise OutputError(
                f"Failed to stream {format_name} data: {e}",
                output_format=format_name,
            ) from e
