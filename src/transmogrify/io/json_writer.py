"""
JSON writer for Transmogrify output.

This module provides a JSON writer with tiered performance based on available libraries.
"""

import os
from typing import Any, Dict, List, Optional, Union

from src.transmogrify.io.writer_interface import DataWriter


class JsonWriter(DataWriter):
    """Writer for JSON format output with tiered performance."""

    @classmethod
    def format_name(cls) -> str:
        """Return the name of the format this writer handles."""
        return "json"

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer's dependencies are available."""
        return True  # JSON is always available through standard library

    @classmethod
    def get_performance_tier(cls) -> str:
        """Return the available JSON implementation tier."""
        try:
            import orjson

            return "high-performance"
        except ImportError:
            return "standard"

    def write_table(
        self,
        table_data: List[Dict[str, Any]],
        output_path: str,
        indent: Optional[int] = 2,
        **kwargs,
    ) -> str:
        """
        Write a single table to JSON format.

        Args:
            table_data: List of records to write
            output_path: Path to write the output file
            indent: Indentation level (None for no indentation)
            **kwargs: Additional options

        Returns:
            Path to the written file
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        # Create a temporary ProcessingResult to use its conversion methods
        from src.transmogrify.core.processing_result import ProcessingResult

        temp_result = ProcessingResult(
            main_table=table_data, child_tables={}, entity_name="temp"
        )

        try:
            # Get JSON bytes using the ProcessingResult methods
            json_bytes = temp_result.to_json_bytes(indent=indent, **kwargs)["main"]

            # Write bytes directly to file
            with open(output_path, "wb") as f:
                f.write(json_bytes)

        except Exception as e:
            # Fall back to older implementation if there's an issue
            # Try orjson first (fastest)
            try:
                import orjson

                # orjson has a different API - it returns bytes
                with open(output_path, "w", encoding="utf-8") as f:
                    if indent is not None:
                        # Need to decode to string for indentation
                        import json

                        data_str = orjson.dumps(table_data, **kwargs).decode("utf-8")
                        json_obj = json.loads(data_str)
                        json.dump(json_obj, f, indent=indent)
                    else:
                        # No indentation needed
                        f.write(orjson.dumps(table_data, **kwargs).decode("utf-8"))

            except ImportError:
                # Fall back to standard library
                import json

                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(table_data, f, indent=indent, **kwargs)

        return output_path

    def write_all_tables(
        self,
        main_table: List[Dict[str, Any]],
        child_tables: Dict[str, List[Dict[str, Any]]],
        base_path: str,
        entity_name: str,
        **kwargs,
    ) -> Dict[str, str]:
        """
        Write main and child tables to JSON format.

        Args:
            main_table: Main table data
            child_tables: Dict of child table name to table data
            base_path: Base directory for output
            entity_name: Name of the main entity
            **kwargs: Format-specific options

        Returns:
            Dict mapping table names to output file paths
        """
        # Create a ProcessingResult object to use its conversion and writing methods
        from src.transmogrify.core.processing_result import ProcessingResult

        result = ProcessingResult(
            main_table=main_table, child_tables=child_tables, entity_name=entity_name
        )

        # Use the ProcessingResult's method to write all tables
        return result.write_all_json(base_path=base_path, **kwargs)
