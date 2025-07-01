"""JSON writer for Transmog output.

This module provides a JSON writer with tiered performance based on available libraries.
"""

import gzip
import json
import logging
import os
import sys
from typing import Any, BinaryIO, Optional, TextIO, Union, cast

from transmog.error import OutputError

# Import writer interfaces
from transmog.io.writer_interface import DataWriter, StreamingWriter, WriterUtils
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

    This writer handles writing data to JSON format files with consistent
    interface and optional compression support.
    """

    def __init__(
        self,
        indent: Optional[int] = 2,
        use_orjson: bool = True,
        compression: Optional[str] = None,
        **options: Any,
    ) -> None:
        """Initialize the JSON writer.

        Args:
            indent: Indentation level for pretty-printing (None for no indentation)
            use_orjson: Whether to use orjson for better performance (if available)
            compression: Compression method ("gzip" supported)
            **options: Additional JSON writer options
        """
        self.indent = indent
        self.use_orjson = use_orjson and ORJSON_AVAILABLE
        self.compression = compression
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

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer is available.

        Returns:
            bool: Always True for JSON writer as it uses standard library
        """
        return True

    def supports_compression(self) -> bool:
        """Check if this writer supports compression.

        Returns:
            bool: True as JSON writer supports gzip compression
        """
        return True

    def get_supported_codecs(self) -> list[str]:
        """Get list of supported compression codecs.

        Returns:
            list[str]: List of supported compression methods
        """
        return ["gzip"]

    def write_table(
        self,
        table_data: list[JsonDict],
        output_path: Union[str, BinaryIO, TextIO],
        **format_options: Any,
    ) -> Union[str, BinaryIO, TextIO]:
        """Write table data to a JSON file.

        Args:
            table_data: The table data to write
            output_path: Path or file-like object to write to
            **format_options: Format-specific options (indent, use_orjson, compression)

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
        """
        # Merge options
        options = {**self.options, **format_options}
        indent = options.get("indent", self.indent)
        use_orjson = options.get("use_orjson", self.use_orjson)
        compression = options.get("compression", self.compression)

        try:
            if isinstance(output_path, str):
                # File path - handle directory creation and compression
                os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

                with WriterUtils.open_output_file(
                    output_path, "wb" if compression == "gzip" else "w", compression
                ) as f:
                    self._write_json_data(table_data, f, indent, use_orjson)
                return output_path
            else:
                # File-like object
                self._write_json_data(table_data, output_path, indent, use_orjson)
                return output_path

        except Exception as e:
            logger.error(f"Error writing JSON: {e}")
            raise OutputError(f"Failed to write JSON file: {e}") from e

    def _write_json_data(
        self,
        data: list[JsonDict],
        file_obj: Union[BinaryIO, TextIO],
        indent: Optional[int],
        use_orjson: bool,
    ) -> None:
        """Write JSON data to file object.

        Args:
            data: Data to write
            file_obj: File object to write to
            indent: Indentation level
            use_orjson: Whether to use orjson
        """
        if use_orjson and ORJSON_AVAILABLE:
            # Use orjson for better performance
            json_bytes = orjson.dumps(data, option=orjson.OPT_INDENT_2 if indent else 0)
            if hasattr(file_obj, "mode") and "b" in getattr(file_obj, "mode", ""):
                # Binary file
                binary_file = cast(BinaryIO, file_obj)
                binary_file.write(json_bytes)
            else:
                # Text file
                text_file = cast(TextIO, file_obj)
                text_file.write(json_bytes.decode("utf-8"))
        else:
            # Use standard json module
            json_str = json.dumps(data, indent=indent, ensure_ascii=False)
            if hasattr(file_obj, "mode") and "b" in getattr(file_obj, "mode", ""):
                # Binary file
                binary_file = cast(BinaryIO, file_obj)
                binary_file.write(json_str.encode("utf-8"))
            else:
                # Text file
                text_file = cast(TextIO, file_obj)
                text_file.write(json_str)

    def write_all_tables(
        self,
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        base_path: Union[str],
        entity_name: str,
        **options: Any,
    ) -> dict[str, str]:
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
        # Use the consolidated write pattern
        return WriterUtils.write_all_tables_pattern(
            main_table=main_table,
            child_tables=child_tables,
            base_path=base_path,
            entity_name=entity_name,
            format_name="json",
            write_table_func=lambda table_data, output_path: self.write_table(
                table_data, output_path, **options
            ),
            compression=options.get("compression", self.compression),
        )


class JsonStreamingWriter(StreamingWriter):
    """Streaming writer for JSON output.

    Supports writing flattened tables to JSON format in a streaming manner,
    minimizing memory usage for large datasets with unified interface.
    """

    @classmethod
    def format_name(cls) -> str:
        """Get the format name for this writer.

        Returns:
            str: The format name ("json")
        """
        return "json"

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        indent: Optional[int] = 2,
        use_orjson: bool = True,
        compression: Optional[str] = None,
        buffer_size: int = 1000,
        **options: Any,
    ) -> None:
        """Initialize the JSON streaming writer.

        Args:
            destination: Output file path or file-like object
            entity_name: Name of the entity
            indent: Number of spaces to indent (None for no indentation)
            use_orjson: Whether to use orjson if available
            compression: Compression method ("gzip" supported)
            buffer_size: Number of records to buffer before writing
            **options: Additional JSON writer options
        """
        super().__init__(destination, entity_name, buffer_size, **options)
        self.indent = indent
        self.use_orjson = use_orjson and ORJSON_AVAILABLE
        self.compression = compression
        self.file_objects: dict[str, Union[BinaryIO, TextIO]] = {}
        self.file_paths: dict[str, str] = {}
        self.initialized_tables: set[str] = set()
        self.should_close: bool = False
        self.base_dir: Optional[str] = None
        self.finalized: bool = False

        # Initialize destination
        if destination is None:
            # Default to stdout
            self.file_objects = {"main": cast(TextIO, sys.stdout)}
            self.should_close = False
        elif isinstance(destination, str):
            # Check if it's a single file path (has .json extension) or directory
            if destination.endswith(".json") or destination.endswith(".json.gz"):
                # Single file destination
                os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)

                # Open file in appropriate mode
                mode = (
                    "wb"
                    if self.compression == "gzip" or destination.endswith(".json.gz")
                    else "w"
                )
                file_obj = open(destination, mode)

                self.file_objects = {"main": cast(Union[BinaryIO, TextIO], file_obj)}
                self.should_close = True
            else:
                # Directory path
                self.base_dir = destination
                os.makedirs(self.base_dir, exist_ok=True)
                self.should_close = True
        else:
            # File-like object
            self.file_objects = {"main": cast(Union[BinaryIO, TextIO], destination)}
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

        # Create new file for table
        if self.base_dir:
            # Determine file extension based on compression
            extension = ".json.gz" if self.compression == "gzip" else ".json"

            if table_name == "main":
                filename = f"{self.entity_name}{extension}"
            else:
                safe_name = table_name.replace(".", "_").replace("/", "_")
                filename = f"{safe_name}{extension}"

            file_path = os.path.join(self.base_dir, filename)
            self.file_paths[table_name] = file_path

            # Open file in appropriate mode
            mode = "wb" if self.compression else "w"
            file_obj = open(file_path, mode)
            typed_file_obj = cast(Union[BinaryIO, TextIO], file_obj)
            self.file_objects[table_name] = typed_file_obj
            return typed_file_obj
        else:
            raise OutputError(f"Cannot create file for table {table_name}")

    def initialize_main_table(self, **options: Any) -> None:
        """Initialize the main table for streaming.

        Args:
            **options: Format-specific options
        """
        self._initialize_table("main", **options)

    def initialize_child_table(self, table_name: str, **options: Any) -> None:
        """Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
            **options: Format-specific options
        """
        self._initialize_table(table_name, **options)

    def _initialize_table(self, table_name: str, **options: Any) -> None:
        """Initialize a table for streaming output.

        Args:
            table_name: Name of the table
            **options: Format-specific options
        """
        if table_name in self.initialized_tables:
            return

        file_obj = self._get_file_for_table(table_name)

        # Write opening bracket
        if self.compression:
            # For compressed files, write as binary
            binary_obj = cast(BinaryIO, file_obj)
            data = b"[\n" if self.indent else b"["
            if self.compression == "gzip":
                # Initialize gzip stream
                self._gzip_files = getattr(self, "_gzip_files", {})
                self._gzip_files[table_name] = gzip.GzipFile(
                    fileobj=binary_obj, mode="wb"
                )
                self._gzip_files[table_name].write(data)
            else:
                binary_obj.write(data)
        else:
            # Write as text
            if hasattr(file_obj, "mode") and "b" not in getattr(file_obj, "mode", ""):
                text_obj = cast(TextIO, file_obj)
                text_obj.write("[\n" if self.indent else "[")
            else:
                binary_obj = cast(BinaryIO, file_obj)
                data = b"[\n" if self.indent else b"["
                binary_obj.write(data)

        self.initialized_tables.add(table_name)

    def write_main_records(self, records: list[dict[str, Any]], **options: Any) -> None:
        """Write a batch of main records.

        Args:
            records: List of main table records to write
            **options: Format-specific options
        """
        self._write_records("main", records, **options)

    def write_child_records(
        self, table_name: str, records: list[dict[str, Any]], **options: Any
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: List of child records to write
            **options: Format-specific options
        """
        self._write_records(table_name, records, **options)

    def _write_records(
        self, table_name: str, records: list[dict[str, Any]], **options: Any
    ) -> None:
        """Write records to the specified table.

        Args:
            table_name: Name of the table
            records: List of records to write
            **options: Format-specific options
        """
        if not records:
            return

        # Initialize table if needed
        if table_name not in self.initialized_tables:
            self._initialize_table(table_name, **options)

        # Get configuration
        indent_val = options.get("indent", self.indent)
        use_orjson = options.get("use_orjson", self.use_orjson)

        # Check if we need a comma separator
        need_comma = self.record_counts.get(table_name, 0) > 0

        # Prepare JSON data
        json_parts = []

        for i, record in enumerate(records):
            if need_comma or i > 0:
                json_parts.append(",")

            if indent_val:
                json_parts.append("\n  ")

            # Serialize record
            if use_orjson and ORJSON_AVAILABLE:
                record_json = orjson.dumps(record).decode("utf-8")
            else:
                record_json = json.dumps(record, indent=None)

            json_parts.append(record_json)

        json_content = "".join(json_parts)

        # Write to file
        if self.compression == "gzip":
            gzip_file = getattr(self, "_gzip_files", {}).get(table_name)
            if gzip_file:
                gzip_file.write(json_content.encode("utf-8"))
        else:
            file_obj = self.file_objects[table_name]
            if hasattr(file_obj, "mode") and "b" not in getattr(file_obj, "mode", ""):
                text_obj = cast(TextIO, file_obj)
                text_obj.write(json_content)
            else:
                binary_obj = cast(BinaryIO, file_obj)
                binary_obj.write(json_content.encode("utf-8"))

        # Update record count and report progress
        self.record_counts[table_name] = self.record_counts.get(table_name, 0) + len(
            records
        )
        self._report_progress(table_name, len(records))

    def finalize(self, **options: Any) -> None:
        """Finalize all tables and close files.

        Args:
            **options: Format-specific options
        """
        if self.finalized:
            return

        # Close all JSON arrays
        for table_name in self.initialized_tables:
            if self.compression == "gzip":
                gzip_file = getattr(self, "_gzip_files", {}).get(table_name)
                if gzip_file:
                    closing_bytes = b"\n]" if self.indent else b"]"
                    gzip_file.write(closing_bytes)
                    gzip_file.close()
            else:
                file_obj = self.file_objects.get(table_name)
                if file_obj:
                    closing_str = "\n]" if self.indent else "]"
                    if hasattr(file_obj, "mode") and "b" not in getattr(
                        file_obj, "mode", ""
                    ):
                        text_obj = cast(TextIO, file_obj)
                        text_obj.write(closing_str)
                    else:
                        binary_obj = cast(BinaryIO, file_obj)
                        closing_bytes = closing_str.encode("utf-8")
                        binary_obj.write(closing_bytes)

        self.finalized = True

    def close(self) -> None:
        """Clean up resources and close all files."""
        if not self.finalized:
            self.finalize()

        # Close all file objects if we opened them
        if self.should_close:
            for file_obj in self.file_objects.values():
                if hasattr(file_obj, "close"):
                    file_obj.close()

        # Clear references
        self.file_objects.clear()
        if hasattr(self, "_gzip_files"):
            self._gzip_files.clear()
