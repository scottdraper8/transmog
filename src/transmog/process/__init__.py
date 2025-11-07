"""Process module for Transmog.

This module contains the main processor and processing strategies.
"""

import os
from collections.abc import Iterator
from typing import (
    Any,
    Optional,
    Protocol,
    TypeVar,
    Union,
    cast,
)

from ..config import TransmogConfig

# Cache management functions
from ..core.flattener import clear_caches, refresh_cache_config
from ..error import (
    ConfigurationError,
    error_context,
)

# Processing result handling
from .result import ProcessingResult

# Processing strategies
from .strategies import (
    ChunkedStrategy,
    FileStrategy,
    InMemoryStrategy,
    ProcessingStrategy,
)

# Type for data records
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


# Define protocols for data iteration
class DataIterator(Protocol[T_co]):
    """Protocol for data iterators."""

    def __iter__(self) -> Iterator[T_co]:
        """Return iterator."""
        ...

    def __next__(self) -> T_co:
        """Return next item."""
        ...


class Processor:
    """Processor for flattening nested structures."""

    def __init__(self, config: Optional[TransmogConfig] = None):
        """Initialize the processor."""
        self.config = config or TransmogConfig()
        self._configure_cache()

    @error_context("Failed to process data", log_exceptions=True)  # type: ignore[misc]
    def process(
        self,
        data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """Process data with the configured settings."""
        # Create result instance to pass to strategies
        result = ProcessingResult(
            main_table=[],
            child_tables={},
            entity_name=entity_name,
        )

        # Create appropriate strategy based on input type
        strategy: ProcessingStrategy
        if isinstance(data, (dict, list)):
            if isinstance(data, dict):
                data = [data]
            strategy = InMemoryStrategy(self.config)
        elif isinstance(data, (str, bytes)):
            if isinstance(data, str) and os.path.exists(data):
                strategy = FileStrategy(self.config)
            else:
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                strategy = ChunkedStrategy(self.config)
        else:
            raise ConfigurationError(f"Unsupported data type: {type(data)}")

        # Process using selected strategy
        processed_result = strategy.process(
            data,
            entity_name=entity_name,
            extract_time=extract_time,
            result=result,
        )

        return cast(ProcessingResult, processed_result)

    def clear_cache(self) -> "Processor":
        """Clear the processor's caches."""
        clear_caches()
        return self

    def _configure_cache(self) -> None:
        """Configure cache settings."""
        refresh_cache_config()
