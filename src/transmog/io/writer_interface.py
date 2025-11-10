"""Writer interfaces for Transmog.

This module defines abstract base classes for data writers.
"""

import os
import re
from abc import ABC, abstractmethod
from typing import Any, BinaryIO, Literal, Optional, TextIO, Union

from transmog.types.base import JsonDict
from transmog.types.io_types import StreamingWriterProtocol, WriterProtocol


class WriterUtils:
    """Utility functions for writer operations."""

    @staticmethod
    def sanitize_filename(name: str) -> str:
        """Sanitize a string for use as a filename.

        Args:
            name: Raw name to sanitize

        Returns:
            Sanitized filename
        """
        sanitized = re.sub(r"[^\w\-_.]", "_", name)
        sanitized = re.sub(r"_{2,}", "_", sanitized)
        return sanitized.strip("_")

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
            Complete file path
        """
        ext = f".{format_name.lower()}"
        if compression == "gzip":
            ext += ".gz"
        return os.path.join(base_path, f"{filename}{ext}")


class DataWriter(ABC, WriterProtocol):
    """Abstract base class for data writers."""

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


class StreamingWriter(ABC, StreamingWriterProtocol):
    """Abstract base class for streaming writers."""

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        buffer_size: int = 1000,
        **options: Any,
    ):
        """Initialize the streaming writer.

        Args:
            destination: Output destination
            entity_name: Name of the entity being processed
            buffer_size: Number of records to buffer before writing
            **options: Format-specific options
        """
        self.destination = destination
        self.entity_name = entity_name
        self.buffer_size = buffer_size
        self.options = options
        self.record_counts: dict[str, int] = {}

    def _report_progress(self, table_name: str, count: int) -> None:
        """Track record counts for internal use.

        Args:
            table_name: Name of the table being processed
            count: Number of records processed
        """
        self.record_counts[table_name] = self.record_counts.get(table_name, 0) + count

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

        Args:
            **options: Format-specific options
        """
        pass

    def close(self) -> None:
        """Clean up resources."""
        if not getattr(self, "_finalized", False):
            self.finalize()
            self._finalized = True

    def __enter__(self) -> "StreamingWriter":
        """Support for context manager protocol."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Finalize when exiting context."""
        self.close()
        return False
