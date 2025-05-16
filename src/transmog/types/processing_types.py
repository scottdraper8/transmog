"""Processing type interfaces for Transmog.

This module defines interfaces for processing strategies to break circular dependencies.
"""

from collections.abc import Iterator
from typing import Any, Literal, Protocol, Union

from .base import JsonDict
from .result_types import ResultInterface

# Type for flatten mode
FlattenMode = Literal["standard", "streaming"]


class ProcessingStrategyProtocol(Protocol):
    """Protocol for processing strategies."""

    def execute(self, entity_name: str, **kwargs: Any) -> ResultInterface:
        """Execute the processing strategy."""
        ...

    def process_records(
        self,
        records: Union[list[JsonDict], Iterator[JsonDict]],
        entity_name: str,
        **kwargs: Any,
    ) -> ResultInterface:
        """Process a collection of records."""
        ...

    def process_file(
        self, file_path: str, entity_name: str, **kwargs: Any
    ) -> ResultInterface:
        """Process records from a file."""
        ...
