"""IO type interfaces for Transmog.

This module defines interfaces for IO operations to break circular dependencies.
"""

from typing import (
    Any,
    BinaryIO,
    Callable,
    Literal,
    Optional,
    Protocol,
    TextIO,
    Union,
)

from .base import JsonDict


class WriterProtocol(Protocol):
    """Protocol for data writers."""

    def write(
        self, data: Any, destination: Union[str, BinaryIO], **options: Any
    ) -> Any:
        """Write data to the destination."""
        ...


class StreamingWriterProtocol(Protocol):
    """Protocol for streaming writers."""

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        **options: Any,
    ) -> None:
        """Initialize the streaming writer.

        Args:
            destination: Output destination (file path or file-like object)
            entity_name: Name of the entity being processed
            **options: Format-specific options
        """
        ...

    def write_main_records(self, records: list[JsonDict]) -> None:
        """Write main table records."""
        ...

    def initialize_child_table(self, table_name: str) -> None:
        """Initialize a child table."""
        ...

    def write_child_records(self, table_name: str, records: list[JsonDict]) -> None:
        """Write child table records."""
        ...

    def finalize(self) -> None:
        """Finalize the output."""
        ...

    def close(self) -> None:
        """Clean up resources and complete writing.

        This is called as part of resource cleanup, typically in finally blocks.
        """
        ...

    def __enter__(self) -> "StreamingWriterProtocol":
        """Enter the context manager."""
        ...

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Exit the context manager."""
        ...

    def initialize_main_table(self) -> None:
        """Initialize the main table for streaming."""
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
