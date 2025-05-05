"""
IO type interfaces for Transmog.

This module defines interfaces for IO operations to break circular dependencies.
"""

from typing import (
    Protocol,
    Dict,
    List,
    Any,
    Optional,
    Union,
    BinaryIO,
    TextIO,
    Callable,
    Type,
)

from .base import JsonDict


class WriterProtocol(Protocol):
    """Protocol for data writers."""

    def write(self, data: Any, destination: Union[str, BinaryIO], **options) -> Any:
        """Write data to the destination."""
        ...


class StreamingWriterProtocol(Protocol):
    """Protocol for streaming writers."""

    def write_main_records(self, records: List[JsonDict]) -> None:
        """Write main table records."""
        ...

    def initialize_child_table(self, table_name: str) -> None:
        """Initialize a child table."""
        ...

    def write_child_records(self, table_name: str, records: List[JsonDict]) -> None:
        """Write child table records."""
        ...

    def finalize(self) -> None:
        """Finalize the output."""
        ...


class WriterRegistryProtocol(Protocol):
    """Protocol for writer registries."""

    def register_writer(self, format_name: str, writer_factory: Any) -> None:
        """Register a writer for a format."""
        ...

    def get_writer(self, format_name: str) -> Optional[WriterProtocol]:
        """Get a writer for a format."""
        ...

    def is_format_available(self, format_name: str) -> bool:
        """Check if a format has a registered writer."""
        ...


# Type aliases for writer and registry factory functions
WriterFactory = Callable[..., WriterProtocol]
StreamingWriterFactory = Callable[..., StreamingWriterProtocol]
