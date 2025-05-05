"""
Processing type interfaces for Transmog.

This module defines interfaces for processing strategies to break circular dependencies.
"""

from typing import Protocol, Dict, List, Any, Optional, Union, Iterator

from .base import JsonDict
from .result_types import ResultInterface


class ProcessingStrategyProtocol(Protocol):
    """Protocol for processing strategies."""

    def execute(self, entity_name: str, **kwargs) -> ResultInterface:
        """Execute the processing strategy."""
        ...

    def process_records(
        self,
        records: Union[List[JsonDict], Iterator[JsonDict]],
        entity_name: str,
        **kwargs,
    ) -> ResultInterface:
        """Process a collection of records."""
        ...

    def process_file(
        self, file_path: str, entity_name: str, **kwargs
    ) -> ResultInterface:
        """Process records from a file."""
        ...
