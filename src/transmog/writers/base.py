"""Base classes for data writers."""

import re
from abc import ABC, abstractmethod
from typing import Any, BinaryIO, Literal, TextIO


def _collect_field_names(data: list[dict[str, Any]]) -> list[str]:
    """Collect all unique field names from data.

    Args:
        data: List of records

    Returns:
        Sorted list of unique field names
    """
    if not data:
        return []

    field_names: set[str] = set()
    for record in data:
        field_names.update(record.keys())

    return sorted(field_names)


def _sanitize_filename(name: str) -> str:
    """Sanitize a string for use as a filename.

    Args:
        name: String to sanitize

    Returns:
        Sanitized filename string
    """
    sanitized = re.sub(r"[^\w\-_.]", "_", name)
    sanitized = re.sub(r"_{2,}", "_", sanitized)
    return sanitized.strip("_")


class DataWriter(ABC):
    """Abstract base class for data writers."""

    @abstractmethod
    def write(
        self,
        data: list[dict[str, Any]],
        destination: str | BinaryIO | TextIO,
        **options: Any,
    ) -> str | BinaryIO | TextIO:
        """Write data to the specified destination.

        Args:
            data: List of dictionaries to write
            destination: File path or file-like object
            **options: Format-specific options

        Returns:
            Path to written file or file-like object
        """
        pass


class StreamingWriter(ABC):
    """Abstract base class for streaming writers."""

    def __init__(
        self,
        destination: str | BinaryIO | TextIO | None = None,
        entity_name: str = "entity",
        **options: Any,
    ):
        """Initialize the streaming writer.

        Args:
            destination: Output destination (path or file-like object)
            entity_name: Name of the entity
            **options: Format-specific options
        """
        self.destination = destination
        self.entity_name = entity_name
        self.options = options

    @abstractmethod
    def write_main_records(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of main records.

        Args:
            records: Main table records to write
        """
        pass

    @abstractmethod
    def write_child_records(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: Child records to write
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Finalize output, flush buffered data, and clean up resources."""
        pass

    def __enter__(self) -> "StreamingWriter":
        """Support for context manager protocol."""
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> Literal[False]:
        """Finalize when exiting context."""
        self.close()
        return False


__all__ = ["DataWriter", "StreamingWriter"]
