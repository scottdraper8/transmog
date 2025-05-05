"""
Parquet writer for Transmog output.

This module provides a Parquet writer using PyArrow.
"""

import os
import importlib.util
from typing import Any, Dict, List, Optional, Union, BinaryIO

from transmog.io.writer_interface import DataWriter
from transmog.error import OutputError, MissingDependencyError, logger
from transmog.types.base import JsonDict
from transmog.io.writer_factory import register_writer

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

    def write(self, data: Any, destination: Union[str, BinaryIO], **options) -> Any:
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
        if isinstance(destination, str):
            return self.write_table(data, destination, **combined_options)
        else:
            return self.write_table(data, destination, **combined_options)

    def write_table(
        self,
        table_data: List[JsonDict],
        output_path: Union[str, BinaryIO],
        compression: Optional[str] = None,
        **options,
    ) -> Union[str, BinaryIO]:
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
                if isinstance(output_path, str):
                    # Ensure directory exists
                    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
                    pq.write_table(
                        empty_table, output_path, compression=compression_val
                    )
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
            if isinstance(output_path, str):
                # Ensure directory exists
                os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
                pq.write_table(
                    table, output_path, compression=compression_val, **options
                )
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
        base_path: str,
        entity_name: str,
        **options,
    ) -> Dict[str, str]:
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
        os.makedirs(base_path, exist_ok=True)

        # Write main table
        main_path = os.path.join(base_path, f"{entity_name}.parquet")
        self.write_table(main_table, main_path, **options)
        results["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Replace dots and slashes with underscores for file names
            safe_name = table_name.replace(".", "_").replace("/", "_")
            file_path = os.path.join(base_path, f"{safe_name}.parquet")
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


# Register the writer
register_writer("parquet", ParquetWriter)
