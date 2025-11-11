"""Writer interfaces for Transmog."""

import re
from abc import ABC, abstractmethod
from typing import Any, BinaryIO, Literal, Optional, TextIO, Union

from transmog.types import JsonDict


def sanitize_filename(name: str) -> str:
    """Sanitize a string for use as a filename."""
    sanitized = re.sub(r"[^\w\-_.]", "_", name)
    sanitized = re.sub(r"_{2,}", "_", sanitized)
    return sanitized.strip("_")


class DataWriter(ABC):
    """Abstract base class for data writers."""

    @abstractmethod
    def write(
        self,
        data: list[JsonDict],
        destination: Union[str, BinaryIO, TextIO],
        **options: Any,
    ) -> Union[str, BinaryIO, TextIO]:
        """Write data to the specified destination."""
        pass


class StreamingWriter(ABC):
    """Abstract base class for streaming writers."""

    def __init__(
        self,
        destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        entity_name: str = "entity",
        **options: Any,
    ):
        """Initialize the streaming writer."""
        self.destination = destination
        self.entity_name = entity_name
        self.options = options

    @abstractmethod
    def write_main_records(self, records: list[JsonDict]) -> None:
        """Write a batch of main records."""
        pass

    @abstractmethod
    def write_child_records(self, table_name: str, records: list[JsonDict]) -> None:
        """Write a batch of child records."""
        pass

    @abstractmethod
    def finalize(self) -> None:
        """Finalize the output."""
        pass

    def close(self) -> None:
        """Clean up resources."""
        if not getattr(self, "_finalized", False):
            self.finalize()
            self._finalized = True

    def __enter__(self) -> "StreamingWriter":
        """Support for context manager protocol."""
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> Literal[False]:
        """Finalize when exiting context."""
        self.close()
        return False
