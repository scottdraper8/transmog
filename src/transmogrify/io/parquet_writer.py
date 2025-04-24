"""
Parquet writer for Transmogrify output.

This module provides a Parquet writer using PyArrow.
"""

import os
import importlib.util
from typing import Any, Dict, List, Optional

from src.transmogrify.io.writer_interface import DataWriter

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
    """Writer for Parquet format output using PyArrow."""

    @classmethod
    def format_name(cls) -> str:
        """Return the name of the format this writer handles."""
        return "parquet"

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer's dependencies are available."""
        # Check if already imported
        if pa is not None:
            return True

        # Use importlib to check if pyarrow is available without importing it
        return importlib.util.find_spec("pyarrow") is not None

    def write_table(
        self,
        table_data: List[Dict[str, Any]],
        output_path: str,
        compression: str = "snappy",
        **kwargs,
    ) -> str:
        """
        Write a single table to Parquet format.

        Args:
            table_data: List of records to write
            output_path: Path to write the output file
            compression: Compression format (snappy, gzip, None, etc.)
            **kwargs: Additional PyArrow Parquet options

        Returns:
            Path to the written file
        """
        if not self.is_available():
            raise ImportError(
                "PyArrow is required for Parquet output. "
                "Install with: pip install pyarrow"
            )

        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        # Convert to PyArrow Table and write directly
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create a temporary ProcessingResult to use its conversion methods
        from src.transmogrify.core.processing_result import ProcessingResult

        temp_result = ProcessingResult(
            main_table=table_data, child_tables={}, entity_name="temp"
        )

        # Get the table as a PyArrow Table
        try:
            # Convert to PyArrow Table
            tables = temp_result.to_pyarrow_tables()
            table = tables["main"]

            # Write the parquet file
            pq.write_table(table, output_path, compression=compression, **kwargs)
        except Exception as e:
            # Fall back to older implementation if there's an issue
            if not table_data:
                # Handle empty table case - create an empty table with the expected schema
                empty_table = pa.table({})
                pq.write_table(
                    empty_table, output_path, compression=compression, **kwargs
                )
                return output_path

            # Convert to PyArrow Table
            # First convert dictionary to a dict of lists for more efficient conversion
            columns = {}
            for key in table_data[0].keys():
                columns[key] = [
                    str(record.get(key)) if record.get(key) is not None else None
                    for record in table_data
                ]

            table = pa.Table.from_pydict(columns)

            # Write the parquet file
            pq.write_table(table, output_path, compression=compression, **kwargs)

        return output_path

    def write_all_tables(
        self,
        main_table: List[Dict[str, Any]],
        child_tables: Dict[str, List[Dict[str, Any]]],
        base_path: str,
        entity_name: str,
        compression: str = "snappy",
        **kwargs,
    ) -> Dict[str, str]:
        """
        Write main and child tables to Parquet format.

        Args:
            main_table: Main table data
            child_tables: Dict of child table name to table data
            base_path: Base directory for output
            entity_name: Name of the main entity
            compression: Compression format
            **kwargs: Additional PyArrow Parquet options

        Returns:
            Dict mapping table names to output file paths
        """
        if not self.is_available():
            raise ImportError(
                "PyArrow is required for Parquet output. "
                "Install with: pip install pyarrow"
            )

        # Create a ProcessingResult object to use its conversion and writing methods
        from src.transmogrify.core.processing_result import ProcessingResult

        result = ProcessingResult(
            main_table=main_table, child_tables=child_tables, entity_name=entity_name
        )

        # Use the ProcessingResult's method to write all tables
        return result.write_all_parquet(
            base_path=base_path, compression=compression, **kwargs
        )
