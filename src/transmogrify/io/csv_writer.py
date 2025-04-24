"""
CSV writer for Transmogrify output.

This module provides a CSV writer with PyArrow and standard library implementations.
"""

import os
import logging
import io
import csv
import gzip
import bz2
import lzma
from typing import Any, Dict, List, Optional, Union, BinaryIO, Tuple
from io import StringIO

from src.transmogrify.io.writer_interface import DataWriter
from src.transmogrify.naming.conventions import sanitize_column_names
from src.transmogrify.config.settings import settings

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
    """Writer for CSV format output with performance tiers."""

    @classmethod
    def format_name(cls) -> str:
        """Return the name of the format this writer handles."""
        return "csv"

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer's dependencies are available."""
        return True  # CSV is always available through standard library

    @classmethod
    def has_advanced_features(cls) -> bool:
        """Check if advanced CSV features are available through PyArrow."""
        return PYARROW_AVAILABLE

    def write_table(
        self,
        table_data: List[Dict[str, Any]],
        output_path: Union[str, BinaryIO],
        compression: Optional[str] = None,
        include_header: bool = True,
        sanitize_header: bool = True,
        **kwargs,
    ) -> str:
        """
        Write a table to CSV format.

        Args:
            table_data: List of records to write
            output_path: Path to write the output file or a file-like object
            compression: Compression format (None, gzip, bz2, xz)
            include_header: Whether to include header row
            sanitize_header: Whether to sanitize header names
            **kwargs: Additional options

        Returns:
            Path to the written file (if output_path is a string) or the original output_path
        """
        # Check if output_path is a file-like object
        is_file_like = hasattr(output_path, "write") and callable(output_path.write)

        # For path-based outputs, ensure directory exists
        if not is_file_like:
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        # Check for empty data
        if not table_data:
            if is_file_like:
                # Just write an empty string to file-like object
                output_path.write(b"")
                return output_path
            else:
                self._write_empty_file(output_path, compression)
                return output_path

        try:
            # First try to use PyArrow if available
            if PYARROW_AVAILABLE:
                try:
                    logger.debug("Using PyArrow for CSV writing")
                    return self._write_with_pyarrow(
                        table_data=table_data,
                        output_path=output_path,
                        compression=compression,
                        include_header=include_header,
                        sanitize_header=sanitize_header,
                        **kwargs,
                    )
                except Exception as e:
                    logger.warning(f"Error using PyArrow CSV writer: {e}")
                    logger.warning("Falling back to standard library implementation")

            # If PyArrow isn't available or failed, use the standard library
            return self._write_with_stdlib(
                table_data=table_data,
                output_path=output_path,
                compression=compression,
                include_header=include_header,
                sanitize_header=sanitize_header,
                **kwargs,
            )
        except (OSError, IOError) as e:
            # Re-raise any I/O errors
            raise OSError(f"Error writing CSV file: {e}") from e

    def _write_empty_file(
        self, output_path: str, compression: Optional[str] = None
    ) -> None:
        """
        Write an empty file with appropriate compression.

        Args:
            output_path: Path to write the output file
            compression: Compression format (None, gzip, bz2, xz)
        """
        # Get opener and mode
        opener, mode = self._get_file_opener(compression, is_write=True)

        with opener(output_path, mode) as f:
            f.write("")

    def _get_file_opener(
        self, compression: Optional[str], is_write: bool = False
    ) -> Tuple[Any, str]:
        """
        Get the appropriate file opener and mode based on compression.

        Args:
            compression: Compression format (None, gzip, bz2, xz)
            is_write: Whether this is for writing (vs reading)

        Returns:
            Tuple of (file_opener, mode)
        """
        mode = "wt" if is_write else "rt"
        binary_mode = "wb" if is_write else "rb"

        if not compression:
            return open, mode
        elif compression == "gzip":
            return gzip.open, binary_mode
        elif compression == "bz2":
            return bz2.open, binary_mode
        elif compression in ("xz", "lzma"):
            return lzma.open, binary_mode
        else:
            logger.warning(f"Unsupported compression format: {compression}")
            return open, mode

    def _write_with_pyarrow(
        self,
        table_data: List[Dict[str, Any]],
        output_path: Union[str, BinaryIO],
        compression: Optional[str],
        include_header: bool,
        sanitize_header: bool = True,
        separator: str = "_",
        **kwargs,
    ) -> Union[str, BinaryIO]:
        """
        Write table using PyArrow.

        Args:
            table_data: List of records to write
            output_path: Path to write the output file or a file-like object
            compression: Compression format (None, gzip, bz2, xz)
            include_header: Whether to include header row
            sanitize_header: Whether to sanitize header names
            separator: Separator character for name sanitization
            **kwargs: Additional options

        Returns:
            Path to the written file or the original file-like object
        """
        # Check if output_path is a file-like object
        is_file_like = hasattr(output_path, "write") and callable(output_path.write)

        # Process column names
        if not table_data:
            if is_file_like:
                output_path.write(b"")
                return output_path
            else:
                self._write_empty_file(output_path, compression)
                return output_path

        # Extract column names
        column_names = list(table_data[0].keys())

        # Sanitize column names if requested
        if sanitize_header:
            # Create a mapping of original keys to sanitized keys
            original_keys = list(table_data[0].keys())
            sanitized_keys = sanitize_column_names(
                original_keys, separator=separator, sql_safe=True
            )
            key_mapping = {
                original_keys[i]: sanitized_keys[i] for i in range(len(original_keys))
            }

            # Rebuild data with sanitized keys
            sanitized_data = []
            for record in table_data:
                sanitized_record = {}
                for orig_key, value in record.items():
                    sanitized_key = key_mapping.get(orig_key, orig_key)
                    sanitized_record[sanitized_key] = value
                sanitized_data.append(sanitized_record)

            table_data = sanitized_data
            column_names = sanitized_keys

        # Convert to PyArrow Table
        columns = {}
        for col in column_names:
            columns[col] = [record.get(col) for record in table_data]

        table = pa.Table.from_pydict(columns)

        # Build write options
        write_options = pa_csv.WriteOptions(include_header=include_header)

        # Handle file-like output
        if is_file_like:
            # Write to in-memory buffer first
            buffer = io.BytesIO()
            pa_csv.write_csv(table, buffer, write_options)
            buffer.seek(0)
            # Copy to output
            output_path.write(buffer.getvalue())
            return output_path

        # Handle path-based output with compression
        elif compression:
            # Write to in-memory buffer first
            buffer = io.BytesIO()
            pa_csv.write_csv(table, buffer, write_options)
            buffer.seek(0)

            # Get appropriate opener for compression
            opener, mode = self._get_file_opener(compression, is_write=True)

            # Write compressed data
            with opener(output_path, mode) as f:
                f.write(buffer.read())
        else:
            # Uncompressed output - write directly to file
            pa_csv.write_csv(table, output_path, write_options)

        return output_path

    def _write_with_stdlib(
        self,
        table_data: List[Dict[str, Any]],
        output_path: Union[str, BinaryIO],
        compression: Optional[str],
        include_header: bool,
        sanitize_header: bool = True,
        separator: str = "_",
        **kwargs,
    ) -> Union[str, BinaryIO]:
        """
        Write table using standard library CSV module.

        Args:
            table_data: List of records to write
            output_path: Path to write the output file or a file-like object
            compression: Compression format (None, gzip, bz2, xz)
            include_header: Whether to include header row
            sanitize_header: Whether to sanitize header names
            separator: Separator character for name sanitization
            **kwargs: Additional options

        Returns:
            Path to the written file or the original file-like object
        """
        # Check if output_path is a file-like object
        is_file_like = hasattr(output_path, "write") and callable(output_path.write)

        if not table_data:
            if is_file_like:
                output_path.write(b"")
                return output_path
            else:
                self._write_empty_file(output_path, compression)
                return output_path

        # Extract header
        if table_data:
            keys = list(table_data[0].keys())
        else:
            keys = []

        # Sanitize header names if requested
        columns = keys
        if sanitize_header:
            columns = [sanitize_key(key, separator=separator) for key in keys]

        # Map original keys to sanitized keys
        key_map = dict(zip(keys, columns))

        try:
            if is_file_like:
                # Write directly to file-like object
                writer = csv.DictWriter(StringIO(), fieldnames=columns, **kwargs)
                output_buffer = StringIO()
                writer = csv.DictWriter(output_buffer, fieldnames=columns, **kwargs)

                if include_header:
                    writer.writeheader()

                for row in table_data:
                    sanitized_row = {}
                    for k, v in row.items():
                        sanitized_row[key_map[k]] = v
                    writer.writerow(sanitized_row)

                # Get the string content and convert to bytes
                output_path.write(output_buffer.getvalue().encode("utf-8"))
                return output_path
            else:
                # Create parent directory if not exists
                output_dir = os.path.dirname(output_path)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir)

                # Get file opener based on compression
                file_opener = self._get_file_opener(compression)

                with file_opener(output_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=columns, **kwargs)

                    if include_header:
                        writer.writeheader()

                    for row in table_data:
                        sanitized_row = {}
                        for k, v in row.items():
                            sanitized_row[key_map[k]] = v
                        writer.writerow(sanitized_row)

                return output_path

        except (IOError, OSError) as e:
            raise IOError(f"Failed to write CSV: {str(e)}")

    def write_all_tables(
        self,
        main_table: List[Dict[str, Any]],
        child_tables: Dict[str, List[Dict[str, Any]]],
        base_path: str,
        entity_name: str,
        **kwargs,
    ) -> Dict[str, str]:
        """
        Write all tables to CSV format.

        Args:
            main_table: Main records
            child_tables: Child table records
            base_path: Base path for output files
            entity_name: Entity name for main table
            **kwargs: Additional options

        Returns:
            Dictionary mapping table names to output paths
        """
        result = {}

        # Ensure output directory exists
        os.makedirs(base_path, exist_ok=True)

        # Write main table
        main_path = os.path.join(base_path, f"{entity_name}.csv")
        result["main"] = self.write_table(main_table, main_path, **kwargs)

        # Write child tables
        for table_name, table_data in child_tables.items():
            table_path = os.path.join(base_path, f"{table_name}.csv")
            result[table_name] = self.write_table(table_data, table_path, **kwargs)

        return result
