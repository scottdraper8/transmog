"""Writer interfaces for Transmog.

This module defines abstract base classes for data writers with unified interfaces.
"""

import gzip
import os
from abc import ABC, abstractmethod
from typing import Any, BinaryIO, Callable, Literal, Optional, TextIO, Union, cast

from transmog.types.base import JsonDict
from transmog.types.io_types import StreamingWriterProtocol, WriterProtocol

logger = __import__("logging").getLogger(__name__)


class WriterUtils:
    """Utility class for common writer operations.

    Consolidates repetitive file writing patterns, directory creation logic,
    and file-like object handling patterns used across different writers.
    """

    @staticmethod
    def ensure_directory_exists(path: str) -> None:
        """Ensure a directory exists, creating it if necessary.

        Args:
            path: Directory path to create
        """
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

    @staticmethod
    def sanitize_filename(name: str) -> str:
        """Sanitize a string for use as a filename.

        Args:
            name: Raw name to sanitize

        Returns:
            Sanitized filename safe for filesystem use
        """
        import re

        sanitized = re.sub(r"[^\w\-_.]", "_", name)
        sanitized = re.sub(r"_{2,}", "_", sanitized)
        return sanitized.strip("_")

    @staticmethod
    def get_file_extension(format_name: str, compression: Optional[str] = None) -> str:
        """Get appropriate file extension for format and compression.

        Args:
            format_name: Format name (e.g., 'json', 'csv', 'parquet')
            compression: Optional compression method

        Returns:
            File extension including compression suffix if applicable
        """
        base_ext = f".{format_name.lower()}"
        if compression == "gzip":
            return f"{base_ext}.gz"
        elif compression == "snappy" and format_name.lower() == "parquet":
            return base_ext  # Snappy is internal to parquet
        else:
            return base_ext

    @staticmethod
    def build_output_path(
        base_path: str,
        filename: str,
        format_name: str,
        compression: Optional[str] = None,
    ) -> str:
        """Build a complete output file path.

        Args:
            base_path: Base directory path
            filename: Base filename (without extension)
            format_name: Format name for extension
            compression: Optional compression method

        Returns:
            Complete file path with appropriate extension
        """
        extension = WriterUtils.get_file_extension(format_name, compression)
        return os.path.join(base_path, f"{filename}{extension}")

    @staticmethod
    def build_table_paths(
        base_path: str,
        entity_name: str,
        child_tables: dict[str, Any],
        format_name: str,
        compression: Optional[str] = None,
    ) -> dict[str, str]:
        """Build file paths for main and child tables.

        Args:
            base_path: Base directory path
            entity_name: Name of the main entity
            child_tables: Dictionary of child table names to data
            format_name: Format name for extensions
            compression: Optional compression method

        Returns:
            Dictionary mapping table names to file paths
        """
        paths = {}

        # Main table path
        paths["main"] = WriterUtils.build_output_path(
            base_path, entity_name, format_name, compression
        )

        # Child table paths
        for table_name in child_tables.keys():
            safe_name = WriterUtils.sanitize_filename(table_name)
            paths[table_name] = WriterUtils.build_output_path(
                base_path, safe_name, format_name, compression
            )

        return paths

    @staticmethod
    def open_output_file(
        file_path: str,
        mode: str = "w",
        compression: Optional[str] = None,
        encoding: str = "utf-8",
    ) -> Union[BinaryIO, TextIO]:
        """Open an output file with appropriate mode and compression.

        Args:
            file_path: Path to the file
            mode: File open mode ('w', 'wb', etc.)
            compression: Optional compression method
            encoding: Text encoding for text files

        Returns:
            Opened file object
        """
        if compression == "gzip":
            if "b" in mode:
                return cast(BinaryIO, gzip.open(file_path, mode))
            else:
                return cast(TextIO, gzip.open(file_path, mode, encoding=encoding))
        else:
            if "b" in mode:
                return cast(BinaryIO, open(file_path, mode))
            else:
                return cast(TextIO, open(file_path, mode, encoding=encoding))

    @staticmethod
    def write_all_tables_pattern(
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        base_path: str,
        entity_name: str,
        format_name: str,
        write_table_func: Callable[[list[JsonDict], Union[str, BinaryIO, TextIO]], Any],
        compression: Optional[str] = None,
        **options: Any,
    ) -> dict[str, str]:
        """Common pattern for writing main and child tables.

        Args:
            main_table: Main table data
            child_tables: Dictionary of child tables
            base_path: Base directory for output files
            entity_name: Name of the entity
            format_name: Format name for file extensions
            write_table_func: Function to write individual tables
            compression: Optional compression method
            **options: Additional format-specific options

        Returns:
            Dictionary mapping table names to file paths
        """
        # Ensure output directory exists
        WriterUtils.ensure_directory_exists(base_path)

        # Build all file paths
        paths = WriterUtils.build_table_paths(
            base_path, entity_name, child_tables, format_name, compression
        )

        # Write main table
        write_table_func(main_table, paths["main"])

        # Write child tables
        for table_name, table_data in child_tables.items():
            write_table_func(table_data, paths[table_name])

        return paths


class CommonWriterOptions:
    """Common options available across all writers."""

    def __init__(
        self,
        compression: Optional[str] = None,
        encoding: str = "utf-8",
        buffer_size: Optional[int] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
        **format_options: Any,
    ):
        """Initialize common writer options.

        Args:
            compression: Compression method ("gzip", "snappy", etc.)
            encoding: Text encoding
            buffer_size: Buffer size for streaming operations
            progress_callback: Callback for progress reporting
            **format_options: Format-specific options
        """
        self.compression = compression
        self.encoding = encoding
        self.buffer_size = buffer_size
        self.progress_callback = progress_callback
        self.format_options = format_options


class DataWriter(ABC, WriterProtocol):
    """Abstract base class for data writers.

    Data writers handle writing processed data to various output formats
    with standardized interfaces and consistent behavior.
    """

    @abstractmethod
    def write_table(
        self,
        table_data: list[JsonDict],
        output_path: Union[str, BinaryIO, TextIO],
        **format_options: Any,
    ) -> Union[str, BinaryIO, TextIO]:
        """Write table data to the specified destination.

        Args:
            table_data: The table data to write
            output_path: Path or file-like object to write to
            **format_options: Format-specific options

        Returns:
            Path to the written file or file-like object

        Raises:
            OutputError: If writing fails
            MissingDependencyError: If required dependencies are unavailable
        """
        pass

    def write(
        self, data: Any, destination: Union[str, BinaryIO, TextIO], **options: Any
    ) -> Union[str, BinaryIO, TextIO]:
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
        return self.write_table(data, destination, **options)

    @abstractmethod
    def write_all_tables(
        self,
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        base_path: Union[str],
        entity_name: str,
        **options: Any,
    ) -> dict[str, str]:
        """Write main and child tables to files.

        Args:
            main_table: The main table data
            child_tables: Dictionary of child tables
            base_path: Directory to write files to
            entity_name: Name of the entity
            **options: Format-specific options

        Returns:
            Dictionary mapping table names to file paths
        """
        pass

    @classmethod
    @abstractmethod
    def format_name(cls) -> str:
        """Get the format name for this writer.

        Returns:
            str: The format name
        """
        pass

    @classmethod
    @abstractmethod
    def is_available(cls) -> bool:
        """Check if this writer's dependencies are available.

        Returns:
            bool: True if the writer can be used
        """
        pass

    def supports_compression(self) -> bool:
        """Check if this writer supports compression.

        Returns:
            bool: True if compression is supported
        """
        return False

    def get_supported_codecs(self) -> list[str]:
        """Get list of supported compression codecs.

        Returns:
            list[str]: List of supported compression methods
        """
        return []


class UnifiedStreamingWriter:
    """Unified base class for streaming writers with consistent behavior."""

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        buffer_size: int = 1000,
        progress_callback: Optional[Callable[[int], None]] = None,
        **options: Any,
    ):
        """Initialize the unified streaming writer.

        Args:
            destination: Output destination
            entity_name: Name of the entity being processed
            buffer_size: Number of records to buffer before writing
            progress_callback: Callback for progress reporting
            **options: Format-specific options
        """
        self.destination = destination
        self.entity_name = entity_name
        self.buffer_size = buffer_size
        self.progress_callback = progress_callback
        self.options = options
        self.initialized = False
        self.child_tables: dict[str, Any] = {}
        self.record_counts: dict[str, int] = {}

    def set_progress_callback(self, callback: Callable[[int], None]) -> None:
        """Set progress callback for reporting.

        Args:
            callback: Function to call with record count updates
        """
        self.progress_callback = callback

    def configure_memory_limits(self, max_memory: int) -> None:
        """Configure memory usage limits.

        Args:
            max_memory: Maximum memory to use in bytes
        """
        # Adjust buffer size based on memory constraints
        estimated_record_size = 1024  # Conservative estimate
        max_buffer_size = max(100, max_memory // estimated_record_size)
        self.buffer_size = min(self.buffer_size, max_buffer_size)

    def _report_progress(self, table_name: str, count: int) -> None:
        """Report progress if callback is set.

        Args:
            table_name: Name of the table being processed
            count: Number of records processed
        """
        if self.progress_callback:
            self.record_counts[table_name] = (
                self.record_counts.get(table_name, 0) + count
            )
            total_records = sum(self.record_counts.values())
            self.progress_callback(total_records)


class StreamingWriter(UnifiedStreamingWriter, StreamingWriterProtocol):
    """Abstract base class for streaming writers.

    Streaming writers handle writing data incrementally without
    storing the entire dataset in memory, with unified behavior.
    """

    @abstractmethod
    def write_main_records(self, records: list[JsonDict], **options: Any) -> None:
        """Write a batch of main records.

        Args:
            records: List of main table records to write
            **options: Format-specific options
        """
        pass

    @abstractmethod
    def initialize_child_table(self, table_name: str, **options: Any) -> None:
        """Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
            **options: Format-specific options
        """
        pass

    @abstractmethod
    def write_child_records(
        self, table_name: str, records: list[JsonDict], **options: Any
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: List of child records to write
            **options: Format-specific options
        """
        pass

    @abstractmethod
    def finalize(self, **options: Any) -> None:
        """Finalize the output.

        This method is called after all data has been written.

        Args:
            **options: Format-specific options
        """
        pass

    def close(self) -> None:
        """Clean up resources and complete writing.

        Calls finalize() to ensure proper resource cleanup.
        """
        if not getattr(self, "_finalized", False):
            self.finalize()
            self._finalized = True

    def __enter__(self) -> "StreamingWriter":
        """Support for context manager protocol."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Finalize when exiting context."""
        self.close()
        return False  # Don't suppress exceptions
