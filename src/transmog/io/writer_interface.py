"""
Writer interfaces for Transmog.

This module defines abstract base classes for data writers.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, BinaryIO, TextIO

from transmog.types.base import JsonDict
from transmog.types.io_types import WriterProtocol, StreamingWriterProtocol


class DataWriter(ABC, WriterProtocol):
    """
    Abstract base class for data writers.

    Data writers handle writing processed data to various output formats.
    """

    @abstractmethod
    def write(self, data: Any, destination: Union[str, BinaryIO], **options) -> Any:
        """
        Write data to the specified destination.

        Args:
            data: Data to write
            destination: Path or file-like object to write to
            **options: Format-specific options

        Returns:
            Format-specific result
        """
        pass


class StreamingWriter(ABC, StreamingWriterProtocol):
    """
    Abstract base class for streaming writers.

    Streaming writers handle writing data incrementally without
    storing the entire dataset in memory.
    """

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        **options,
    ):
        """
        Initialize the streaming writer.

        Args:
            destination: Output destination (file path or file-like object)
            entity_name: Name of the entity being processed
            **options: Format-specific options
        """
        self.destination = destination
        self.entity_name = entity_name
        self.options = options
        self.initialized = False
        self.child_tables = {}

    @abstractmethod
    def write_main_records(self, records: List[JsonDict]) -> None:
        """
        Write a batch of main records.

        Args:
            records: List of main table records to write
        """
        pass

    @abstractmethod
    def initialize_child_table(self, table_name: str) -> None:
        """
        Initialize a child table for streaming.

        Args:
            table_name: Name of the child table
        """
        pass

    @abstractmethod
    def write_child_records(self, table_name: str, records: List[JsonDict]) -> None:
        """
        Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: List of child records to write
        """
        pass

    @abstractmethod
    def finalize(self) -> None:
        """
        Finalize the output.

        This method is called after all data has been written.
        """
        pass

    def __enter__(self):
        """Support for context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Finalize when exiting context."""
        self.finalize()
        return False  # Don't suppress exceptions
