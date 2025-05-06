"""
JSON writer for Transmog output.

This module provides a JSON writer with tiered performance based on available libraries.
"""

import os
import json
import logging
import io
import pathlib
from typing import Any, Dict, List, Optional, Union, BinaryIO, TextIO

# Import writer interfaces
from transmog.io.writer_interface import DataWriter, StreamingWriter
from transmog.config.settings import settings
from transmog.error import OutputError, MissingDependencyError
from transmog.io.writer_factory import register_writer, register_streaming_writer
from transmog.types.base import JsonDict

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
    JSON format writer.

    This writer handles writing data to JSON format files.
    """

    def __init__(self, indent: Optional[int] = 2, use_orjson: bool = True, **options):
        """
        Initialize the JSON writer.

        Args:
            indent: Indentation level for pretty-printing (None for no indentation)
            use_orjson: Whether to use orjson for better performance (if available)
            **options: Additional JSON writer options
        """
        self.indent = indent
        self.use_orjson = use_orjson and ORJSON_AVAILABLE
        self.options = options

    @classmethod
    def format_name(cls) -> str:
        """
        Get the format name for this writer.

        Returns:
            str: The format name ("json")
        """
        return "json"

    @classmethod
    def is_orjson_available(cls) -> bool:
        """
        Check if orjson is available for accelerated JSON serialization.

        Returns:
            bool: True if orjson is available, False otherwise
        """
        return ORJSON_AVAILABLE

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
        """
        # Combine constructor options with per-call options
        combined_options = {**self.options, **options}

        # Delegate to write_table for implementation
        return self.write_table(data, destination, **combined_options)

    def write_table(
        self,
        table_data: List[JsonDict],
        output_path: Union[str, pathlib.Path, BinaryIO, TextIO],
        indent: Optional[int] = None,
        **options,
    ) -> Union[str, pathlib.Path, BinaryIO, TextIO]:
        """
        Write table data to a JSON file.

        Args:
            table_data: The table data to write
            output_path: Path or file-like object to write to
            indent: Indentation level (None for no indentation)
            **options: Additional options

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
        """
        try:
            # Use options or fall back to instance defaults
            indent_val = indent if indent is not None else self.indent
            use_orjson = options.get("use_orjson", self.use_orjson)

            # Convert data to JSON bytes
            if use_orjson and ORJSON_AVAILABLE:
                json_bytes = orjson.dumps(
                    table_data, option=orjson.OPT_INDENT_2 if indent_val else 0
                )
            else:
                json_string = json.dumps(table_data, indent=indent_val)
                json_bytes = json_string.encode("utf-8")

            # Determine whether we're writing to a file or file-like object
            if isinstance(output_path, (str, pathlib.Path)):
                # Convert Path to string if needed
                path_str = str(output_path)

                # Ensure directory exists
                os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)

                # Write to file
                with open(path_str, "wb") as f:
                    f.write(json_bytes)

                return output_path
            else:
                # Write to file-like object
                if hasattr(output_path, "write"):
                    # Check if it's a text or binary stream
                    if hasattr(output_path, "mode") and "b" not in output_path.mode:
                        output_path.write(json_bytes.decode("utf-8"))
                    else:
                        output_path.write(json_bytes)
                    return output_path
                else:
                    raise OutputError(f"Invalid destination type: {type(output_path)}")

        except Exception as e:
            logger.error(f"Error writing JSON: {e}")
            raise OutputError(f"Failed to write JSON file: {e}")

    def write_all_tables(
        self,
        main_table: List[JsonDict],
        child_tables: Dict[str, List[JsonDict]],
        base_path: Union[str, pathlib.Path],
        entity_name: str,
        **options,
    ) -> Dict[str, Union[str, pathlib.Path]]:
        """
        Write main and child tables to JSON files.

        Args:
            main_table: The main table data
            child_tables: Dictionary of child tables
            base_path: Directory to write files to
            entity_name: Name of the entity (for main table filename)
            **options: Additional JSON formatting options

        Returns:
            Dictionary mapping table names to file paths

        Raises:
            OutputError: If writing fails
        """
        results = {}

        # Ensure base directory exists
        base_path_str = str(base_path)
        os.makedirs(base_path_str, exist_ok=True)

        # Write main table
        if isinstance(base_path, pathlib.Path):
            main_path = base_path / f"{entity_name}.json"
        else:
            main_path = os.path.join(base_path_str, f"{entity_name}.json")

        self.write_table(main_table, main_path, **options)
        results["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Replace dots and slashes with underscores for file names
            safe_name = table_name.replace(".", "_").replace("/", "_")

            if isinstance(base_path, pathlib.Path):
                file_path = base_path / f"{safe_name}.json"
            else:
                file_path = os.path.join(base_path_str, f"{safe_name}.json")

            self.write_table(table_data, file_path, **options)
            results[table_name] = file_path

        return results

    @classmethod
    def is_available(cls) -> bool:
        """
        Check if this writer is available.

        Returns:
            bool: Always True for JSON writer as it uses standard library
        """
        return True


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
