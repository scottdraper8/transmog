"""JSON writer for Transmog output.

This module provides a JSON writer with tiered performance based on available libraries.
"""

import io
import json
import logging
import os
import pathlib
import sys
from typing import Any, BinaryIO, Optional, TextIO, Union, cast

from transmog.error import OutputError
from transmog.io.writer_factory import register_streaming_writer, register_writer

# Import writer interfaces
from transmog.io.writer_interface import DataWriter, StreamingWriter
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
    """JSON format writer.

    This writer handles writing data to JSON format files.
    """

    def __init__(
        self, indent: Optional[int] = 2, use_orjson: bool = True, **options: Any
    ) -> None:
        """Initialize the JSON writer.

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
        """Get the format name for this writer.

        Returns:
            str: The format name ("json")
        """
        return "json"

    @classmethod
    def is_orjson_available(cls) -> bool:
        """Check if orjson is available for accelerated JSON serialization.

        Returns:
            bool: True if orjson is available, False otherwise
        """
        return ORJSON_AVAILABLE

    def write(
        self,
        data: Any,
        destination: Union[str, pathlib.Path, BinaryIO, TextIO],
        **options: Any,
    ) -> Union[str, pathlib.Path, BinaryIO, TextIO]:
        """Write data to the specified destination.

        Args:
            data: Data to write
            destination: Path or file-like object to write to
            **options: Format-specific options

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
        """
        # Merge options
        combined_options = {**self.options, **options}

        # Delegate to write_table for implementation
        return self.write_table(data, destination, **combined_options)

    def write_table(
        self,
        table_data: list[JsonDict],
        output_path: Union[str, pathlib.Path, BinaryIO, TextIO],
        indent: Optional[int] = None,
        **options: Any,
    ) -> Union[str, pathlib.Path, BinaryIO, TextIO]:
        """Write table data to a JSON file.

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
            # Resolve configuration
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

            # Handle file path destination
            if isinstance(output_path, (str, pathlib.Path)):
                path_str = str(output_path)
                os.makedirs(os.path.dirname(path_str) or ".", exist_ok=True)

                with open(path_str, "wb") as f:
                    f.write(json_bytes)

                return output_path
            # Handle file-like object destination
            elif hasattr(output_path, "write"):
                # Text stream handling
                if hasattr(output_path, "mode") and "b" not in getattr(
                    output_path, "mode", ""
                ):
                    text_output = cast(TextIO, output_path)
                    text_output.write(json_bytes.decode("utf-8"))
                # Binary stream handling
                else:
                    binary_output = cast(BinaryIO, output_path)
                    binary_output.write(json_bytes)
                return output_path
            else:
                raise OutputError(f"Invalid destination type: {type(output_path)}")

        except Exception as e:
            logger.error(f"Error writing JSON: {e}")
            raise OutputError(f"Failed to write JSON file: {e}") from e

    def write_all_tables(
        self,
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        base_path: Union[str, pathlib.Path],
        entity_name: str,
        **options: Any,
    ) -> dict[str, Union[str, pathlib.Path]]:
        """Write main and child tables to JSON files.

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
        results: dict[str, Union[str, pathlib.Path]] = {}

        # Create output directory
        base_path_str = str(base_path)
        os.makedirs(base_path_str, exist_ok=True)

        # Write main table
        main_path: Union[str, pathlib.Path]
        if isinstance(base_path, pathlib.Path):
            main_path = base_path / f"{entity_name}.json"
        else:
            main_path = os.path.join(base_path_str, f"{entity_name}.json")

        self.write_table(main_table, main_path, **options)
        results["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Sanitize filename
            safe_name = table_name.replace(".", "_").replace("/", "_")

            table_path: Union[str, pathlib.Path]
            if isinstance(base_path, pathlib.Path):
                table_path = base_path / f"{safe_name}.json"
            else:
                table_path = os.path.join(base_path_str, f"{safe_name}.json")

            self.write_table(table_data, table_path, **options)
            results[table_name] = table_path

        return results

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer is available.

        Returns:
            bool: Always True for JSON writer as it uses standard library
        """
        return True


class JsonStreamingWriter(StreamingWriter):
    """Streaming writer for JSON output.

    Supports writing flattened tables to JSON format in a streaming manner,
    minimizing memory usage for large datasets.
    """

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        indent: Optional[int] = 2,
        use_orjson: bool = True,
        **options: Any,
    ) -> None:
        """Initialize the JSON streaming writer.

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
        self.file_objects: dict[str, Union[BinaryIO, TextIO]] = {}
        self.file_paths: dict[str, str] = {}
        self.initialized_tables: set[str] = set()
        self.record_counts: dict[str, int] = {}
        self.should_close: bool = False
        self.base_dir: Optional[str] = None

        # Initialize destination
        if destination is None:
            # Default to stdout
            self.file_objects = {"main": sys.stdout}
            self.should_close = False
        elif isinstance(destination, str):
            # Directory path
            self.base_dir = destination
            os.makedirs(self.base_dir, exist_ok=True)
            self.should_close = True
        else:
            # File-like object
            self.file_objects = {"main": destination}
            self.should_close = False

    def _get_file_for_table(self, table_name: str) -> Union[BinaryIO, TextIO]:
        """Get or create a file object for the given table.

        Args:
            table_name: Name of the table

        Returns:
            File object for writing
        """
        if table_name in self.file_objects:
            return self.file_objects[table_name]

        # Create new file when base directory is specified
        if self.base_dir is not None:
            # Sanitize path
            safe_name = table_name.replace("/", "_").replace("\\", "_")

            # Main table uses entity_name
            if table_name == "main":
                file_path = os.path.join(self.base_dir, f"{self.entity_name}.json")
            else:
                file_path = os.path.join(self.base_dir, f"{safe_name}.json")

            self.file_paths[table_name] = file_path

            # Open appropriate file mode based on serializer
            if self.use_orjson:
                file_obj: Union[BinaryIO, TextIO] = open(file_path, "wb")
            else:
                file_obj = open(file_path, "w", encoding="utf-8")

            self.file_objects[table_name] = file_obj
            return file_obj
        else:
            raise OutputError(
                f"Cannot create file for table {table_name}: "
                f"no base directory specified"
            )

    def initialize_main_table(self, **options: Any) -> None:
        """Initialize the main table for streaming.

        Args:
            **options: Format-specific options

        Returns:
            None
        """
        self._initialize_table("main", **options)

    def initialize_child_table(self, table_name: str, **options: Any) -> None:
        """Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
            **options: Format-specific options

        Returns:
            None
        """
        self._initialize_table(table_name, **options)

    def _initialize_table(self, table_name: str, **options: Any) -> None:
        """Initialize a table by writing the opening bracket.

        Args:
            table_name: Name of the table
            **options: Format-specific options

        Returns:
            None
        """
        if table_name in self.initialized_tables:
            return

        file_obj = self._get_file_for_table(table_name)

        # Detect binary stream by checking mode or instance type
        is_binary = (
            hasattr(file_obj, "mode") and "b" in getattr(file_obj, "mode", "")
        ) or isinstance(file_obj, (io.BytesIO, io.BufferedIOBase))

        if is_binary:
            binary_file = cast(BinaryIO, file_obj)
            binary_file.write(b"[")
        else:
            text_file = cast(TextIO, file_obj)
            text_file.write("[")

        file_obj.flush()
        self.initialized_tables.add(table_name)
        self.record_counts[table_name] = 0

    def write_main_records(self, records: list[dict[str, Any]], **options: Any) -> None:
        """Write a batch of main records.

        Args:
            records: Batch of records to write
            **options: Format-specific options

        Returns:
            None
        """
        self._write_records("main", records, **options)

    def write_child_records(
        self, table_name: str, records: list[dict[str, Any]], **options: Any
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: Batch of records to write
            **options: Format-specific options

        Returns:
            None
        """
        self._write_records(table_name, records, **options)

    def _write_records(
        self, table_name: str, records: list[dict[str, Any]], **options: Any
    ) -> None:
        """Write a batch of records to a table.

        Args:
            table_name: Name of the table
            records: Batch of records to write
            **options: Format-specific options

        Returns:
            None
        """
        if not records:
            return

        # Initialize table if needed
        if table_name not in self.initialized_tables:
            self._initialize_table(table_name, **options)

        file_obj = self._get_file_for_table(table_name)
        record_count = self.record_counts.get(table_name, 0)

        # Detect binary stream by checking mode or instance type
        is_binary = (
            hasattr(file_obj, "mode") and "b" in getattr(file_obj, "mode", "")
        ) or isinstance(file_obj, (io.BytesIO, io.BufferedIOBase))

        # Process each record
        for record in records:
            # Add separator for non-first records
            if record_count > 0:
                if is_binary:
                    binary_file = cast(BinaryIO, file_obj)
                    binary_file.write(b",")
                else:
                    text_file = cast(TextIO, file_obj)
                    text_file.write(",")

                # Add newline for indented output
                if self.indent is not None:
                    if is_binary:
                        binary_file = cast(BinaryIO, file_obj)
                        binary_file.write(b"\n")
                    else:
                        text_file = cast(TextIO, file_obj)
                        text_file.write("\n")

            # Serialize record
            if self.use_orjson and ORJSON_AVAILABLE:
                json_bytes = orjson.dumps(
                    record, option=orjson.OPT_INDENT_2 if self.indent else 0
                )
                if is_binary:
                    binary_file = cast(BinaryIO, file_obj)
                    binary_file.write(json_bytes)
                else:
                    text_file = cast(TextIO, file_obj)
                    text_file.write(json_bytes.decode("utf-8"))
            else:
                json_str = json.dumps(record, indent=self.indent)
                if is_binary:
                    binary_file = cast(BinaryIO, file_obj)
                    binary_file.write(json_str.encode("utf-8"))
                else:
                    text_file = cast(TextIO, file_obj)
                    text_file.write(json_str)

            record_count += 1

        # Update record counter
        self.record_counts[table_name] = record_count
        file_obj.flush()

    def finalize(self, **options: Any) -> None:
        """Finalize all tables by writing closing brackets.

        Args:
            **options: Format-specific options

        Returns:
            None
        """
        # Close open tables
        for table_name in list(self.initialized_tables):
            # Skip already closed tables
            if table_name not in self.file_objects:
                continue

            file_obj = self.file_objects[table_name]

            # Detect binary stream
            is_binary = (
                hasattr(file_obj, "mode") and "b" in getattr(file_obj, "mode", "")
            ) or isinstance(file_obj, (io.BytesIO, io.BufferedIOBase))

            # Write closing bracket
            if is_binary:
                binary_file = cast(BinaryIO, file_obj)
                binary_file.write(b"\n]")
            else:
                text_file = cast(TextIO, file_obj)
                text_file.write("\n]")

            file_obj.flush()

        # Close file handles
        if self.should_close:
            self.close()

    def close(self) -> None:
        """Close all file objects.

        Returns:
            None
        """
        # Skip standard streams when closing
        for table_name, file_obj in list(self.file_objects.items()):
            if file_obj not in (sys.stdout, sys.stderr, sys.stdin):
                file_obj.close()
                del self.file_objects[table_name]


# Register writers with factory
register_writer("json", JsonWriter)
register_streaming_writer("json", JsonStreamingWriter)
