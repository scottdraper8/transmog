"""Process module for Transmog.

This module contains the main processor and processing strategies.
"""

import os
from collections.abc import Iterator
from typing import (
    Any,
    BinaryIO,
    Callable,
    Optional,
    Protocol,
    TypeVar,
    Union,
    cast,
)

from ..config import ProcessingMode, TransmogConfig

# Cache management functions
from ..core.flattener import clear_caches, refresh_cache_config
from ..core.metadata import (
    get_current_timestamp,
)
from ..error import (
    LENIENT,
    STRICT,
    ConfigurationError,
    PartialProcessingRecovery,
    ProcessingError,
    RecoveryStrategy,
    SkipAndLogRecovery,
    StrictRecovery,
    error_context,
    logger,
    with_recovery,
)

# Data iteration utilities
from .data_iterators import (
    get_csv_file_iterator,
    get_data_iterator,
    get_json_data_iterator,
    get_json_file_iterator,
    get_jsonl_data_iterator,
    get_jsonl_file_iterator,
)

# File processing utilities
from .file_handling import (
    detect_input_format,
    process_chunked,
    process_csv,
    process_file,
    process_file_to_format,
)

# Processing result handling
from .result import ConversionMode, ProcessingResult

# Processing strategies
from .strategies import (
    BatchStrategy,
    ChunkedStrategy,
    CSVStrategy,
    FileStrategy,
    InMemoryStrategy,
    ProcessingStrategy,
)

# Streaming processing utilities
from .streaming import (
    stream_process,
    stream_process_csv,
    stream_process_file,
    stream_process_file_with_format,
)

# General utilities
from .utils import get_batch_size, get_common_config_params, handle_file_error

# Type for data records
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


# Define protocols for data iteration
class DataIterator(Protocol[T_co]):
    """Protocol for data iterators."""

    def __iter__(self) -> Iterator[T_co]: ...
    def __next__(self) -> T_co: ...


# Define return type variable for the decorator's generic type
R = TypeVar("R")


class Processor:
    """Internal processor for flattening nested structures.

    This class is internal only. Use tm.flatten() instead.
    """

    def __init__(self, config: Optional[TransmogConfig] = None):
        """Initialize the processor with the given configuration."""
        self.config = config or TransmogConfig.default()
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
            # In-memory data
            strategy = InMemoryStrategy(self.config)
        elif isinstance(data, (str, bytes)):
            # String or bytes data, check if it's a file path
            if isinstance(data, str) and os.path.exists(data):
                # File path
                file_ext = os.path.splitext(data)[1].lower()
                if file_ext == ".csv":
                    strategy = CSVStrategy(self.config)
                else:
                    strategy = FileStrategy(self.config)
            else:
                # JSON string/bytes or JSONL
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
        """Configure the processor's cache settings."""
        # Apply cache configuration from the processor's config to global settings
        from .. import config

        # Update global settings with cache configuration
        if hasattr(self.config, "cache_config"):
            cache_config = self.config.cache_config

            # Directly update the settings object
            config.settings.update(
                cache_enabled=cache_config.enabled,
                cache_maxsize=cache_config.maxsize,
                clear_cache_after_batch=cache_config.clear_after_batch,
            )

            # Refresh the cache configuration to apply settings
            refresh_cache_config()


# The Processor class is internal only - use tm.flatten() instead
# No __all__ export to keep it internal
