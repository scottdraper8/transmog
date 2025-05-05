"""
JSON writer for Transmog output.

This module provides a JSON writer with tiered performance based on available libraries.
"""

import os
import json
import logging
import io
from typing import Any, Dict, List, Optional, Union, BinaryIO, TextIO

# Import writer interfaces
from transmog.io.writer_interface import DataWriter, StreamingWriter
from transmog.config.settings import settings
from transmog.error import OutputError, MissingDependencyError
from transmog.io.writer_factory import register_writer, register_streaming_writer

# Configure logger
logger = logging.getLogger(__name__)

# Check for orjson availability
try:
    import orjson

    ORJSON_AVAILABLE = True
except ImportError:
    ORJSON_AVAILABLE = False


class JsonWriter(DataWriter):
    """
    Writer for JSON output.

    Supports writing flattened tables to JSON format.
    """

    @classmethod
    def format_name(cls) -> str:
        """Return the name of the format this writer handles."""
        return "json"

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer's dependencies are available."""
        # JSON is available through standard library
        return True

    @classmethod
    def get_performance_tier(cls) -> str:
        """Get the performance tier of the writer."""
        try:
            import orjson

            return "high"
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
            indent: Number of spaces to indent (None for no indentation)
            **kwargs: Additional options

        Returns:
            Path to the written file
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

            # Try using orjson if available
            if ORJSON_AVAILABLE:
                try:
                    serialized = self._write_with_orjson(table_data, indent)
                    with open(output_path, "wb") as f:
                        f.write(serialized)
                    return output_path
                except Exception as e:
                    logger.warning(
                        f"Failed to write JSON with orjson: {e}. Falling back to stdlib."
                    )
                    # Fall back to standard library

            # Use standard library json
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(table_data, f, indent=indent, ensure_ascii=False)

            return output_path
        except Exception as e:
            logger.error(f"Error writing JSON: {e}")
            raise OutputError(f"Failed to write JSON: {e}")

    def _write_with_orjson(
        self, table_data: List[Dict[str, Any]], indent: Optional[int]
    ) -> bytes:
        """
        Write table data using orjson.

        Args:
            table_data: List of records to write
            indent: Number of spaces to indent (None for no indentation)

        Returns:
            Serialized JSON as bytes
        """
        if not ORJSON_AVAILABLE:
            raise MissingDependencyError("orjson is required for this operation")

        try:
            options = 0
            if indent is not None:
                options |= orjson.OPT_INDENT_2

            return orjson.dumps(table_data, option=options)
        except Exception as e:
            logger.error(f"Error writing JSON with orjson: {e}")
            raise OutputError(f"Failed to write JSON with orjson: {e}")

    def write_all_tables(
        self,
        main_table: List[Dict[str, Any]],
        child_tables: Dict[str, List[Dict[str, Any]]],
        base_path: str,
        entity_name: str = "entity",
        indent: Optional[int] = 2,
        **kwargs,
    ) -> Dict[str, str]:
        """
        Write main and child tables to JSON format.

        Args:
            main_table: Main table data
            child_tables: Dict of child table name to table data
            base_path: Base directory for output
            entity_name: Name of the main entity
            indent: Number of spaces to indent (None for no indentation)
            **kwargs: Additional options

        Returns:
            Dict mapping table names to output file paths
        """
        result_paths = {}

        # Ensure the base directory exists
        os.makedirs(base_path, exist_ok=True)

        # Write main table
        main_path = os.path.join(base_path, f"{entity_name}.json")
        self.write_table(main_table, main_path, indent=indent, **kwargs)
        result_paths["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            safe_name = table_name.replace("/", "_").replace("\\", "_")
            table_path = os.path.join(base_path, f"{safe_name}.json")
            self.write_table(table_data, table_path, indent=indent, **kwargs)
            result_paths[table_name] = table_path

        return result_paths


class JsonStreamingWriter(StreamingWriter):
    """
    Streaming writer for JSON output.

    Supports writing flattened tables to JSON format in a streaming manner,
    minimizing memory usage for large datasets.
    """

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        indent: Optional[int] = 2,
        use_orjson: bool = True,
        **options,
    ):
        """
        Initialize the JSON streaming writer.

        Args:
            destination: Output file path or file-like object
            entity_name: Name of the entity
            indent: Number of spaces to indent (None for no indentation)
            use_orjson: Whether to use orjson if available
            **options: Additional JSON writer options
        """
        self.entity_name = entity_name
        self.indent = indent
        self.use_orjson = use_orjson and ORJSON_AVAILABLE
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

        # Keep track of table states
        self.initialized_tables = set()
        self.record_counts = {}

    def _get_file_for_table(self, table_name: str) -> Union[BinaryIO, TextIO]:
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

            # Use entity_name for the main table
            if table_name == "main":
                file_path = os.path.join(self.base_dir, f"{self.entity_name}.json")
            else:
                file_path = os.path.join(self.base_dir, f"{safe_name}.json")

            self.file_paths[table_name] = file_path

            if self.use_orjson:
                # Binary mode for orjson
                file_obj = open(file_path, "wb")
            else:
                # Text mode for standard json
                file_obj = open(file_path, "w", encoding="utf-8")

            self.file_objects[table_name] = file_obj
            return file_obj
        else:
            raise OutputError(
                f"Cannot create file for table {table_name}: no base directory specified"
            )

    def initialize_main_table(self, **options) -> None:
        """
        Initialize the main table for streaming.

        Args:
            **options: Format-specific options

        Returns:
            None
        """
        self._initialize_table("main", **options)

    def initialize_child_table(self, table_name: str, **options) -> None:
        """
        Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
            **options: Format-specific options

        Returns:
            None
        """
        self._initialize_table(table_name, **options)

    def _initialize_table(self, table_name: str, **options) -> None:
        """
        Initialize a table by writing the opening bracket.

        Args:
            table_name: Name of the table
            **options: Format-specific options

        Returns:
            None
        """
        if table_name in self.initialized_tables:
            return

        file_obj = self._get_file_for_table(table_name)

        # Determine if we're working with a binary or text stream
        is_binary = hasattr(file_obj, "mode") and "b" in file_obj.mode

        if self.use_orjson:
            # orjson doesn't support streaming, so we need to handle it manually
            if is_binary:
                file_obj.write(b"[")
            else:
                file_obj.write("[")
        else:
            # Standard json
            file_obj.write("[")

        file_obj.flush()
        self.initialized_tables.add(table_name)
        self.record_counts[table_name] = 0

    def write_main_records(self, records: List[Dict[str, Any]], **options) -> None:
        """
        Write a batch of main records.

        Args:
            records: Batch of records to write
            **options: Format-specific options

        Returns:
            None
        """
        self._write_records("main", records, **options)

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
        self._write_records(table_name, records, **options)

    def _write_records(
        self, table_name: str, records: List[Dict[str, Any]], **options
    ) -> None:
        """
        Write a batch of records to a table.

        Args:
            table_name: Name of the table
            records: Batch of records to write
            **options: Format-specific options

        Returns:
            None
        """
        if not records:
            return

        # Make sure the table is initialized
        if table_name not in self.initialized_tables:
            self._initialize_table(table_name, **options)

        file_obj = self._get_file_for_table(table_name)
        record_count = self.record_counts[table_name]

        # Determine if we're working with a binary or text stream
        is_binary = hasattr(file_obj, "mode") and "b" in file_obj.mode

        # Write each record
        for record in records:
            # Add a comma if this isn't the first record
            if record_count > 0:
                if is_binary:
                    file_obj.write(b",")
                else:
                    file_obj.write(",")

                # Add newline for pretty printing if indent is specified
                if self.indent is not None:
                    if is_binary:
                        file_obj.write(b"\n")
                    else:
                        file_obj.write("\n")

            # Write the record
            if self.use_orjson and is_binary:
                # Use orjson for each individual record
                options = 0
                if self.indent is not None:
                    options |= orjson.OPT_INDENT_2
                serialized = orjson.dumps(record, option=options)
                # Remove the first and last bytes which are the [ and ]
                serialized = (
                    serialized[1:-1] if serialized.startswith(b"[") else serialized
                )
                file_obj.write(serialized)
            else:
                # Use standard json or convert bytes to string
                if self.use_orjson and not is_binary:
                    # If we're using orjson but not a binary stream, convert to string
                    options = 0
                    if self.indent is not None:
                        options |= orjson.OPT_INDENT_2
                    serialized = orjson.dumps(record, option=options)
                    serialized = (
                        serialized[1:-1] if serialized.startswith(b"[") else serialized
                    )
                    file_obj.write(serialized.decode("utf-8"))
                else:
                    # Use standard json
                    json_str = json.dumps(
                        record, indent=self.indent, ensure_ascii=False
                    )
                    file_obj.write(json_str)

            record_count += 1

        # Update record count
        self.record_counts[table_name] = record_count

        # Flush to ensure data is written
        file_obj.flush()

    def finalize(self, **options) -> None:
        """
        Finalize all tables by writing closing brackets.

        Args:
            **options: Format-specific options

        Returns:
            None
        """
        for table_name in self.initialized_tables:
            file_obj = self.file_objects[table_name]

            # Determine if we're working with a binary or text stream
            is_binary = hasattr(file_obj, "mode") and "b" in file_obj.mode

            # Add newline for pretty printing if indent is specified
            if self.indent is not None:
                if is_binary:
                    file_obj.write(b"\n")
                else:
                    file_obj.write("\n")

            # Write closing bracket
            if is_binary:
                file_obj.write(b"]")
            else:
                file_obj.write("]")

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

        self.initialized_tables = set()
        self.record_counts = {}


# Register the writer
register_writer("json", JsonWriter)
register_streaming_writer("json", JsonStreamingWriter)
