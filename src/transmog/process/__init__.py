"""Process module for Transmog.

This module contains the main processor and processing strategies.
"""

import os
from collections.abc import Iterator
from io import StringIO
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
from .strategy import (
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
    """Main processor for flattening nested JSON structures.

    The Processor handles the transformation of complex nested JSON data into
    flattened tables with parent-child relationships preserved.
    """

    def __init__(self, config: Optional[TransmogConfig] = None):
        """Initialize the processor with the given configuration.

        Args:
            config: Optional configuration object. If None, uses default configuration.
        """
        self.config = config or TransmogConfig.default()

        # Apply cache configuration
        self._configure_cache()

    @classmethod
    def default(cls) -> "Processor":
        """Create a processor with default configuration."""
        return cls()

    @classmethod
    def memory_optimized(cls) -> "Processor":
        """Create a processor with memory-optimized configuration."""
        return cls(TransmogConfig.memory_optimized())

    @classmethod
    def performance_optimized(cls) -> "Processor":
        """Create a processor with performance-optimized configuration."""
        return cls(TransmogConfig.performance_optimized())

    @classmethod
    def with_deterministic_ids(cls, source_field: str) -> "Processor":
        """Create a processor with deterministic ID generation.

        Args:
            source_field: Field name to use for deterministic ID generation

        Returns:
            Processor configured with deterministic ID generation
        """
        return cls(TransmogConfig.with_deterministic_ids(source_field))

    @classmethod
    def with_custom_id_generation(
        cls, strategy: Callable[[dict[str, Any]], str]
    ) -> "Processor":
        """Create a processor with custom ID generation."""
        return cls(TransmogConfig.with_custom_id_generation(strategy))

    @classmethod
    def with_partial_recovery(cls) -> "Processor":
        """Create a processor with partial recovery strategy.

        This enables maximizing data yield from problematic sources by recovering
        partial data from records with errors, particularly useful for:
        - Data migration from legacy systems
        - Processing API responses with inconsistent structures
        - Recovering data from malformed/corrupted files

        Returns:
            Processor configured with partial recovery strategy
        """
        return cls(
            TransmogConfig.default()
            .with_error_handling(recovery_strategy="partial", allow_malformed_data=True)
            .with_processing(
                # Enable string casting to handle numeric type issues
                cast_to_string=True
            )
        )

    def with_config(self, config: TransmogConfig) -> "Processor":
        """Create a new processor with the given configuration."""
        processor = Processor(config)
        return processor

    def with_naming(self, **kwargs: Any) -> "Processor":
        """Update naming configuration.

        Args:
            **kwargs: Naming configuration parameters

        Returns:
            Updated Processor instance
        """
        return self.with_config(self.config.with_naming(**kwargs))

    def with_processing(self, **kwargs: Any) -> "Processor":
        """Update processing configuration.

        Args:
            **kwargs: Processing configuration parameters

        Returns:
            Updated Processor instance
        """
        return self.with_config(self.config.with_processing(**kwargs))

    def with_metadata(self, **kwargs: Any) -> "Processor":
        """Update metadata configuration.

        Args:
            **kwargs: Metadata configuration parameters

        Returns:
            Updated Processor instance
        """
        return self.with_config(self.config.with_metadata(**kwargs))

    def with_error_handling(self, **kwargs: Any) -> "Processor":
        """Update error handling configuration.

        Args:
            **kwargs: Error handling configuration parameters

        Returns:
            Updated Processor instance
        """
        return self.with_config(self.config.with_error_handling(**kwargs))

    @error_context("Failed to process data", log_exceptions=True)
    def process(
        self,
        data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """Process data with the current configuration.

        Args:
            data: Input data to process
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp

        Returns:
            ProcessingResult containing processed data
        """
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

    @error_context("Failed to process batch", log_exceptions=True)
    def process_batch(
        self,
        batch_data: list[dict[str, Any]],
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """Process a batch of records.

        Args:
            batch_data: List of records to process
            entity_name: Name of the entity
            extract_time: Extraction timestamp

        Returns:
            ProcessingResult with flattened records
        """
        result = self._process_batch(batch_data, entity_name, extract_time)

        # Clear cache after batch processing if configured
        if getattr(self.config.cache_config, "clear_after_batch", False):
            self.clear_cache()

        return cast(ProcessingResult, result)

    def process_file(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """Process a file.

        Args:
            file_path: Path to the file to process
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp

        Returns:
            ProcessingResult containing processed data
        """
        return process_file(self, file_path, entity_name, extract_time)

    def process_file_to_format(
        self,
        file_path: str,
        entity_name: str,
        output_format: str,
        output_path: Optional[str] = None,
        extract_time: Optional[Any] = None,
        **format_options: Any,
    ) -> ProcessingResult:
        """Process a file and save to a specified format.

        Args:
            file_path: Path to the file to process
            entity_name: Name of the entity
            output_format: Output format (json, csv, parquet)
            output_path: Path to save output
            extract_time: Optional extraction timestamp
            **format_options: Format-specific options

        Returns:
            ProcessingResult containing processed data
        """
        return process_file_to_format(
            self,
            file_path,
            entity_name,
            output_format,
            output_path,
            extract_time,
            **format_options,
        )

    def process_csv(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
        delimiter: Optional[str] = None,
        has_header: bool = True,
        null_values: Optional[list[str]] = None,
        sanitize_column_names: bool = True,
        infer_types: bool = True,
        skip_rows: int = 0,
        quote_char: Optional[str] = None,
        encoding: str = "utf-8",
        chunk_size: Optional[int] = None,
    ) -> ProcessingResult:
        """Process a CSV file.

        Args:
            file_path: Path to the CSV file
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            delimiter: CSV delimiter character
            has_header: Whether the CSV has a header row
            null_values: List of strings to treat as null values
            sanitize_column_names: Whether to sanitize column names
            infer_types: Whether to infer data types
            skip_rows: Number of rows to skip at the beginning
            quote_char: Quote character for CSV fields
            encoding: File encoding
            chunk_size: Size of chunks to process

        Returns:
            ProcessingResult containing processed data
        """
        return process_csv(
            self,
            file_path,
            entity_name,
            extract_time,
            delimiter,
            has_header,
            null_values,
            sanitize_column_names,
            infer_types,
            skip_rows,
            quote_char,
            encoding,
            chunk_size,
        )

    @error_context("Failed to process chunked data", log_exceptions=True)
    def process_chunked(
        self,
        data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
        entity_name: str,
        extract_time: Optional[Any] = None,
        chunk_size: Optional[int] = None,
        input_format: str = "auto",
        **format_options: Any,
    ) -> ProcessingResult:
        """Process data in chunks for memory efficiency.

        Args:
            data: Input data to process
            entity_name: Name of the entity
            extract_time: Optional extraction timestamp
            chunk_size: Size of chunks to process
            input_format: Format of input data
            **format_options: Format-specific options

        Returns:
            ProcessingResult containing processed data
        """
        return process_chunked(
            self,
            data,
            entity_name,
            extract_time,
            chunk_size,
            input_format,
            **format_options,
        )

    def stream_process(
        self,
        data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
        entity_name: str,
        output_format: str,
        output_destination: Union[str, BinaryIO, StringIO],
        extract_time: Optional[Any] = None,
        **format_options: Any,
    ) -> None:
        """Stream process data directly to output format.

        Args:
            data: Input data to process
            entity_name: Name of the entity
            output_format: Output format
            output_destination: Output destination
            extract_time: Optional extraction timestamp
            **format_options: Format-specific options
        """
        return stream_process(
            self,
            data,
            entity_name,
            output_format,
            output_destination,
            extract_time,
            **format_options,
        )

    def stream_process_file(
        self,
        file_path: str,
        entity_name: str,
        output_format: str,
        output_destination: Optional[Union[str, BinaryIO]] = None,
        extract_time: Optional[Any] = None,
        **format_options: Any,
    ) -> None:
        """Stream process a file and write directly to the output destination.

        Args:
            file_path: Path to the input file
            entity_name: Name of the entity being processed
            output_format: Output format (json, csv, parquet, etc.)
            output_destination: Path or file-like object to write to
            extract_time: Optional extraction timestamp
            **format_options: Format-specific options
        """
        return stream_process_file(
            self,
            file_path,
            entity_name,
            output_format,
            output_destination,
            extract_time,
            **format_options,
        )

    def stream_process_csv(
        self,
        file_path: str,
        entity_name: str,
        output_format: str,
        output_destination: Union[str, BinaryIO, StringIO],
        extract_time: Optional[Any] = None,
        delimiter: Optional[str] = None,
        has_header: bool = True,
        null_values: Optional[list[str]] = None,
        sanitize_column_names: bool = True,
        infer_types: bool = True,
        skip_rows: int = 0,
        quote_char: Optional[str] = None,
        encoding: str = "utf-8",
        **format_options: Any,
    ) -> None:
        """Stream process a CSV file and write directly to the output destination.

        Args:
            file_path: Path to the CSV file
            entity_name: Name of the entity being processed
            output_format: Output format (json, csv, parquet, etc.)
            output_destination: Path or file-like object to write to
            extract_time: Optional extraction timestamp
            delimiter: CSV delimiter character
            has_header: Whether the CSV has a header row
            null_values: List of strings to treat as null values
            sanitize_column_names: Whether to sanitize column names
            infer_types: Whether to infer data types
            skip_rows: Number of rows to skip at the beginning
            quote_char: Quote character for CSV fields
            encoding: File encoding
            **format_options: Format-specific options
        """
        return stream_process_csv(
            self,
            file_path,
            entity_name,
            output_format,
            output_destination,
            extract_time,
            delimiter,
            has_header,
            null_values,
            sanitize_column_names,
            infer_types,
            skip_rows,
            quote_char,
            encoding,
            **format_options,
        )

    def stream_process_file_with_format(
        self,
        file_path: str,
        entity_name: str,
        output_format: str,
        format_type: str = "auto",
        output_destination: Optional[Union[str, BinaryIO]] = None,
        extract_time: Optional[Any] = None,
        **format_options: Any,
    ) -> None:
        """Stream process a file with known format.

        Args:
            file_path: Path to the file
            entity_name: Name of the entity
            output_format: Output format
            format_type: Input file format
            output_destination: Output destination
            extract_time: Optional extraction timestamp
            **format_options: Format-specific options
        """
        return stream_process_file_with_format(
            self,
            file_path,
            entity_name,
            output_format,
            format_type,
            output_destination,
            extract_time,
            **format_options,
        )

    def _determine_processing_mode(
        self, data: Union[dict[str, Any], list[dict[str, Any]], str, bytes]
    ) -> ProcessingMode:
        """Determine the best processing mode based on data characteristics.

        Args:
            data: Input data to analyze

        Returns:
            Recommended ProcessingMode
        """
        # For iterators, always use low-memory mode
        if (
            hasattr(data, "__iter__")
            and hasattr(data, "__next__")
            and not isinstance(data, (list, dict, str, bytes))
        ):
            return ProcessingMode.LOW_MEMORY

        # For strings/bytes, estimate size
        if isinstance(data, (str, bytes)):
            data_size = len(data)

            # If it's a file path, try to get file size
            if isinstance(data, str) and os.path.exists(data):
                try:
                    data_size = os.path.getsize(data)
                except OSError:
                    pass

            # Use memory threshold from settings or default
            memory_threshold = 100 * 1024 * 1024  # 100MB default

            if data_size > memory_threshold:
                return ProcessingMode.LOW_MEMORY

        # For lists, check length and item size
        if isinstance(data, list):
            # Sample a few items to estimate average size
            item_count = len(data)
            if item_count > 10000:  # Large number of items
                return ProcessingMode.LOW_MEMORY

            # Sample up to 10 items for size estimation
            sample_size = min(10, item_count)
            if sample_size > 0:
                try:
                    import json

                    # Estimate size by serializing to JSON
                    # (more accurate than sys.getsizeof)
                    sample_items = data[:sample_size]
                    average_size = (
                        len(json.dumps(sample_items)) / len(sample_items)
                        if sample_items
                        else 0
                    )
                    estimated_total_size = average_size * item_count

                    # Use memory threshold from settings or default
                    memory_threshold = 100 * 1024 * 1024  # 100MB default

                    if estimated_total_size > memory_threshold:
                        return ProcessingMode.LOW_MEMORY
                except Exception as e:
                    # If estimation fails, log and default to standard mode
                    logger.debug(f"Error estimating memory requirements: {e}")

        # Default to the configured processing mode
        return self.config.processing.processing_mode

    def _process_data(
        self,
        data: Union[list[dict[str, Any]], Iterator[dict[str, Any]]],
        entity_name: str,
        extract_time: Optional[Any] = None,
        memory_mode: ProcessingMode = ProcessingMode.STANDARD,
    ) -> ProcessingResult:
        """Process data with the specified memory mode.

        Args:
            data: Input data to process
            entity_name: Entity name
            extract_time: Optional extraction timestamp
            memory_mode: Memory optimization mode

        Returns:
            ProcessingResult
        """
        # Set a default extract time if none provided
        if extract_time is None:
            extract_time = get_current_timestamp()

        # Get parameters from config
        params = get_common_config_params(self.config)

        # Add recovery strategy to params
        if self.config.error_handling.recovery_strategy:
            from ..error.recovery import (
                DEFAULT,
                LENIENT,
                STRICT,
                PartialProcessingRecovery,
                SkipAndLogRecovery,
                StrictRecovery,
            )

            error_config = self.config.error_handling

            # Create the recovery strategy instance
            recovery_strategy: Optional[
                Union[StrictRecovery, SkipAndLogRecovery, PartialProcessingRecovery]
            ] = None

            if error_config.recovery_strategy == "strict":
                recovery_strategy = STRICT
            elif error_config.recovery_strategy == "skip":
                recovery_strategy = DEFAULT
            elif error_config.recovery_strategy == "partial":
                recovery_strategy = LENIENT
            elif isinstance(
                error_config.recovery_strategy,
                (StrictRecovery, SkipAndLogRecovery, PartialProcessingRecovery),
            ):
                recovery_strategy = error_config.recovery_strategy

            # Add to params
            if recovery_strategy:
                params["recovery_strategy"] = recovery_strategy

        # Choose the appropriate strategy for this data
        processing_result: ProcessingResult

        if memory_mode == ProcessingMode.STANDARD:
            # For standard mode, process in memory with the InMemoryStrategy
            processing_strategy = InMemoryStrategy(self.config)
            result = processing_strategy.process(
                data,
                entity_name=entity_name,
                extract_time=extract_time,
                config=self.config,
                params=params,
            )
            processing_result = cast(ProcessingResult, result)

        elif memory_mode == ProcessingMode.LOW_MEMORY:
            # For memory-optimized mode, use the BatchStrategy with a smaller batch size
            batch_strategy = BatchStrategy(self.config)
            result = batch_strategy.process(
                data if isinstance(data, list) else list(data),
                entity_name=entity_name,
                extract_time=extract_time,
                config=self.config,
                params=params,
            )
            processing_result = cast(ProcessingResult, result)

        elif memory_mode == ProcessingMode.HIGH_PERFORMANCE:
            # For large datasets, use chunked processing with larger batches
            if isinstance(data, list) and len(data) > self.config.processing.batch_size:
                batch_strategy = BatchStrategy(self.config)
                result = batch_strategy.process(
                    data,
                    entity_name=entity_name,
                    extract_time=extract_time,
                    config=self.config,
                    params=params,
                )
                processing_result = cast(ProcessingResult, result)
            else:
                # Default to in-memory strategy
                processing_strategy = InMemoryStrategy(self.config)
                result = processing_strategy.process(
                    data,
                    entity_name=entity_name,
                    extract_time=extract_time,
                    config=self.config,
                    params=params,
                )
                processing_result = cast(ProcessingResult, result)
        else:
            # Default to InMemoryStrategy for other modes
            processing_strategy = InMemoryStrategy(self.config)
            result = processing_strategy.process(
                data,
                entity_name=entity_name,
                extract_time=extract_time,
                config=self.config,
                params=params,
            )
            processing_result = cast(ProcessingResult, result)

        return processing_result

    def process_to_format(
        self,
        data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
        entity_name: str,
        output_format: str,
        output_path: Optional[str] = None,
        extract_time: Optional[Any] = None,
        auto_detect_mode: bool = True,
        **format_options: Any,
    ) -> ProcessingResult:
        """Process data and write directly to the specified output format.

        This is a convenience method that combines processing and writing in one step.

        Args:
            data: Input data to process
            entity_name: Name of the entity
            output_format: Output format ("json", "csv", "parquet", etc)
            output_path: Path to write output files
            extract_time: Optional extraction timestamp
            auto_detect_mode: Whether to auto-detect processing mode
            **format_options: Format-specific options

        Returns:
            ProcessingResult object (also writes to output_path if specified)
        """
        # Auto-detect processing mode if requested
        if auto_detect_mode:
            memory_mode = self._determine_processing_mode(data)
        else:
            memory_mode = self.config.processing.processing_mode

        # Process the data
        result: ProcessingResult
        if isinstance(data, (str, bytes)) and os.path.isfile(data):
            # For file paths, use file processing methods
            processed_result = process_file(self, data, entity_name, extract_time)
            result = cast(ProcessingResult, processed_result)
        else:
            # Process in memory with appropriate mode
            data_iterator = get_data_iterator(self, data)
            processed_result = self._process_data(
                list(data_iterator), entity_name, extract_time, memory_mode
            )
            result = cast(ProcessingResult, processed_result)

        # Write to output format if path is specified
        if output_path:
            # Create output directory if it doesn't exist
            os.makedirs(output_path, exist_ok=True)

            # Write to the specified format
            result.write(output_format, output_path, **format_options)

        return result

    def clear_cache(self) -> "Processor":
        """Clear the processor's caches.

        Returns:
            The processor instance for method chaining
        """
        clear_caches()
        return self

    def _process_batch(
        self,
        batch_data: list[dict[str, Any]],
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """Internal method to process a batch of records.

        Args:
            batch_data: List of records to process
            entity_name: Name of the entity
            extract_time: Extraction timestamp

        Returns:
            ProcessingResult with flattened records
        """
        # Create batch strategy and process
        strategy = BatchStrategy(self.config)
        result = strategy.process(
            batch_data, entity_name=entity_name, extract_time=extract_time
        )
        return cast(ProcessingResult, result)

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

    def _determine_processing_strategy(
        self,
        data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
        entity_name: str,
        mode: ProcessingMode,
    ) -> ProcessingStrategy:
        """Determine the best processing strategy based on data and mode.

        Args:
            data: Input data
            entity_name: Name of the entity
            mode: Processing mode

        Returns:
            Appropriate processing strategy for the data
        """
        # Determine strategy based on mode
        if mode == ProcessingMode.LOW_MEMORY:
            # For large datasets, use chunked processing
            if isinstance(data, list) and len(data) > self.config.processing.batch_size:
                return BatchStrategy(self.config)

            # Default to in-memory strategy
            return InMemoryStrategy(self.config)

        elif mode == ProcessingMode.HIGH_PERFORMANCE:
            # For large datasets, use chunked processing with larger batches
            if isinstance(data, list) and len(data) > self.config.processing.batch_size:
                return BatchStrategy(self.config)

            # Default to in-memory strategy
            return InMemoryStrategy(self.config)
        else:
            # Standard processing - choose strategy based on data type
            if isinstance(data, dict):
                return InMemoryStrategy(self.config)
            elif isinstance(data, list):
                return InMemoryStrategy(self.config)
            elif isinstance(data, str) and os.path.exists(data):
                file_ext = os.path.splitext(data)[1].lower()
                if file_ext == ".csv":
                    return CSVStrategy(self.config)
                else:
                    return FileStrategy(self.config)
            else:
                # Default to in-memory strategy
                return InMemoryStrategy(self.config)


# Public API
__all__ = [
    # Main classes
    "Processor",
    "ProcessingResult",
    "ConversionMode",
    # Strategy pattern classes
    "ProcessingStrategy",
    "InMemoryStrategy",
    "FileStrategy",
    "BatchStrategy",
    "ChunkedStrategy",
    "CSVStrategy",
    # File handling
    "process_file",
    "process_file_to_format",
    "process_csv",
    "process_chunked",
    "detect_input_format",
    "handle_file_error",
    # Streaming
    "stream_process",
    "stream_process_file",
    "stream_process_csv",
    "stream_process_file_with_format",
    # Data iterators
    "get_data_iterator",
    "get_json_file_iterator",
    "get_json_data_iterator",
    "get_jsonl_file_iterator",
    "get_jsonl_data_iterator",
    "get_csv_file_iterator",
    # Utilities
    "get_common_config_params",
    "get_batch_size",
    # Error handling
    "STRICT",
    "LENIENT",
    "PartialProcessingRecovery",
    "ProcessingError",
    "RecoveryStrategy",
    "SkipAndLogRecovery",
    "StrictRecovery",
    "error_context",
    "logger",
    "with_recovery",
]
