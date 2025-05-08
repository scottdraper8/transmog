"""
Process module for Transmog package.

This module provides the main processor functionality and result handling.
"""

import os
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    Callable,
    Iterator,
    TypeVar,
    Protocol,
    BinaryIO,
)
from io import StringIO

from ..error import (
    ConfigurationError,
    ProcessingError,
    error_context,
    logger,
    RecoveryStrategy,
    StrictRecovery,
    SkipAndLogRecovery,
    PartialProcessingRecovery,
    with_recovery,
    STRICT,
    LENIENT,
)
from ..config import (
    TransmogConfig,
    ProcessingMode,
)
from ..core.metadata import (
    get_current_timestamp,
)

# Import processing result handling
from .result import ProcessingResult, ConversionMode

# Import processing strategies
from .strategy import (
    ProcessingStrategy,
    InMemoryStrategy,
    FileStrategy,
    BatchStrategy,
    ChunkedStrategy,
    CSVStrategy,
)

# Import file handling functions
from .file_handling import (
    process_file,
    process_file_to_format,
    process_csv,
    process_chunked,
    detect_input_format,
    handle_file_error,
)

# Import streaming functions
from .streaming import (
    stream_process,
    stream_process_file,
    stream_process_csv,
    stream_process_file_with_format,
)

# Import data iterators
from .data_iterators import (
    get_data_iterator,
    get_json_file_iterator,
    get_json_data_iterator,
    get_jsonl_file_iterator,
    get_jsonl_data_iterator,
    get_csv_file_iterator,
)

# Import utilities
from .utils import get_common_config_params, get_batch_size

# Type for data records
T = TypeVar("T")


# Define protocols for data iteration
class DataIterator(Protocol[T]):
    """Protocol for data iterators."""

    def __iter__(self) -> Iterator[T]: ...
    def __next__(self) -> T: ...


class Processor:
    """
    Main processor for flattening nested JSON structures.

    The Processor handles the transformation of complex nested JSON data into
    flattened tables with parent-child relationships preserved.
    """

    def __init__(self, config: Optional[TransmogConfig] = None):
        """
        Initialize the processor with the given configuration.

        Args:
            config: Optional configuration object. If None, uses default configuration.
        """
        self.config = config or TransmogConfig.default()

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
        """
        Create a processor with deterministic ID generation.

        Args:
            source_field: Field name to use for deterministic ID generation

        Returns:
            Processor configured with deterministic ID generation
        """
        return cls(TransmogConfig.with_deterministic_ids(source_field))

    @classmethod
    def with_custom_id_generation(
        cls, strategy: Callable[[Dict[str, Any]], str]
    ) -> "Processor":
        """Create a processor with custom ID generation."""
        return cls(TransmogConfig.with_custom_id_generation(strategy))

    @classmethod
    def with_partial_recovery(cls) -> "Processor":
        """
        Create a processor with partial recovery strategy.

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
            .with_error_handling(recovery_strategy=LENIENT, allow_malformed_data=True)
            .with_processing(
                cast_to_string=True  # Enable string casting to handle numeric type issues
            )
        )

    def with_config(self, config: TransmogConfig) -> "Processor":
        """Create a new processor with the given configuration."""
        return Processor(config)

    def with_naming(self, **kwargs) -> "Processor":
        """Create a new processor with updated naming settings."""
        return self.with_config(self.config.with_naming(**kwargs))

    def with_processing(self, **kwargs) -> "Processor":
        """Create a new processor with updated processing settings."""
        return self.with_config(self.config.with_processing(**kwargs))

    def with_metadata(self, **kwargs) -> "Processor":
        """Create a new processor with updated metadata settings."""
        return self.with_config(self.config.with_metadata(**kwargs))

    def with_error_handling(self, **kwargs) -> "Processor":
        """Create a new processor with updated error handling settings."""
        return self.with_config(self.config.with_error_handling(**kwargs))

    @error_context("Failed to process data", log_exceptions=True)
    def process(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """
        Process data with the current configuration.

        Args:
            data: Input data to process
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp

        Returns:
            ProcessingResult containing processed data
        """
        # Create appropriate strategy based on input type
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
        return strategy.process(
            data,
            entity_name=entity_name,
            extract_time=extract_time,
        )

    @error_context("Failed to process batch", log_exceptions=True)
    def process_batch(
        self,
        batch_data: List[Dict[str, Any]],
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """
        Process a batch of records.

        This is a simplified version of process() specifically optimized
        for batch processing of records.

        Args:
            batch_data: List of records to process
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp

        Returns:
            ProcessingResult containing processed data
        """
        # Create batch strategy and process
        strategy = BatchStrategy(self.config)
        return strategy.process(
            batch_data, entity_name=entity_name, extract_time=extract_time
        )

    def process_file(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """
        Process a file.

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
        **format_options,
    ) -> ProcessingResult:
        """
        Process a file and write directly to the specified output format.

        Args:
            file_path: Path to the input file
            entity_name: Name of the entity
            output_format: Output format ("json", "csv", "parquet", etc)
            output_path: Path to write output files
            extract_time: Optional extraction timestamp
            **format_options: Format-specific options

        Returns:
            ProcessingResult object (also writes to output_path if specified)
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
        null_values: Optional[List[str]] = None,
        sanitize_column_names: bool = True,
        infer_types: bool = True,
        skip_rows: int = 0,
        quote_char: Optional[str] = None,
        encoding: str = "utf-8",
        chunk_size: Optional[int] = None,
    ) -> ProcessingResult:
        """
        Process a CSV file.

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

    def process_chunked(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
        entity_name: str,
        extract_time: Optional[Any] = None,
        chunk_size: Optional[int] = None,
        input_format: str = "auto",
        **format_options,
    ) -> ProcessingResult:
        """
        Process data in chunks for memory efficiency.

        Args:
            data: Input data (dict, list, string, bytes, or file path)
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            chunk_size: Size of chunks to process
            input_format: Format of the input data
            **format_options: Additional format options

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
        data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
        entity_name: str,
        output_format: str,
        output_destination: Union[str, BinaryIO, StringIO],
        extract_time: Optional[Any] = None,
        **format_options,
    ) -> None:
        """
        Stream process data and write directly to the output destination.

        Args:
            data: Input data to process
            entity_name: Name of the entity being processed
            output_format: Output format (json, csv, parquet, etc.)
            output_destination: Path or file-like object to write to
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
        output_destination: Union[str, BinaryIO, StringIO],
        extract_time: Optional[Any] = None,
        **format_options,
    ) -> None:
        """
        Stream process a file and write directly to the output destination.

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
        null_values: Optional[List[str]] = None,
        sanitize_column_names: bool = True,
        infer_types: bool = True,
        skip_rows: int = 0,
        quote_char: Optional[str] = None,
        encoding: str = "utf-8",
        **format_options,
    ) -> None:
        """
        Stream process a CSV file and write directly to the output destination.

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
        output_destination: Union[str, BinaryIO, StringIO],
        format_type: str = "auto",
        extract_time: Optional[Any] = None,
        **format_options,
    ) -> None:
        """
        Stream process a file with specified format and write to the output destination.

        Args:
            file_path: Path to the input file
            entity_name: Name of the entity being processed
            output_format: Output format (json, csv, parquet, etc.)
            output_destination: Path or file-like object to write to
            format_type: Input format type (auto, json, jsonl, csv)
            extract_time: Optional extraction timestamp
            **format_options: Format-specific options
        """
        return stream_process_file_with_format(
            self,
            file_path,
            entity_name,
            output_format,
            output_destination,
            format_type,
            extract_time,
            **format_options,
        )

    def _determine_processing_mode(
        self, data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes]
    ) -> ProcessingMode:
        """
        Determine the best processing mode based on data characteristics.

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
                except (OSError, IOError):
                    pass

            # Use memory threshold from settings or default
            memory_threshold = (
                self.config.processing.memory_threshold or 100 * 1024 * 1024
            )  # 100MB default

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
                    import sys
                    import json

                    # Estimate size by serializing to JSON (more accurate than sys.getsizeof)
                    sample_items = data[:sample_size]
                    average_size = (
                        sum(len(json.dumps(item)) for item in sample_items)
                        / sample_size
                    )
                    estimated_total_size = average_size * item_count

                    # Use memory threshold from settings or default
                    memory_threshold = (
                        self.config.processing.memory_threshold or 100 * 1024 * 1024
                    )  # 100MB default

                    if estimated_total_size > memory_threshold:
                        return ProcessingMode.LOW_MEMORY
                except Exception:
                    # If estimation fails, default to standard mode
                    pass

        # Default to the configured processing mode
        return self.config.processing.processing_mode

    def _process_data(
        self,
        data: Union[List[Dict[str, Any]], Iterator[Dict[str, Any]]],
        entity_name: str,
        extract_time: Optional[Any] = None,
        memory_mode: ProcessingMode = ProcessingMode.STANDARD,
    ) -> ProcessingResult:
        """
        Process data with the specified memory mode.

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
                STRICT,
                DEFAULT,
                LENIENT,
                StrictRecovery,
                SkipAndLogRecovery,
                PartialProcessingRecovery,
            )

            # Create the recovery strategy instance
            strategy_name = self.config.error_handling.recovery_strategy
            recovery_strategy = None

            if strategy_name == "strict":
                recovery_strategy = STRICT
            elif strategy_name == "skip":
                recovery_strategy = DEFAULT
            elif strategy_name == "partial":
                recovery_strategy = LENIENT
            elif isinstance(
                strategy_name,
                (StrictRecovery, SkipAndLogRecovery, PartialProcessingRecovery),
            ):
                recovery_strategy = strategy_name

            # Add to params
            if recovery_strategy:
                params["recovery_strategy"] = recovery_strategy

        # Choose the appropriate strategy for this data
        if memory_mode == ProcessingMode.STANDARD:
            # For standard mode, process in memory with the InMemoryStrategy
            processing_strategy = InMemoryStrategy()
        elif memory_mode == ProcessingMode.MEMORY_OPTIMIZED:
            # For memory-optimized mode, use the BatchStrategy with a smaller batch size
            batch_size = get_batch_size(
                self.config.processing.batch_size,
                memory_mode,
                data_len=getattr(data, "__len__", lambda: None)(),
            )
            processing_strategy = BatchStrategy(batch_size=batch_size)
        elif memory_mode == ProcessingMode.PERFORMANCE_OPTIMIZED:
            # For performance-optimized mode, use a larger batch size
            batch_size = get_batch_size(
                self.config.processing.batch_size,
                memory_mode,
                data_len=getattr(data, "__len__", lambda: None)(),
            )
            processing_strategy = BatchStrategy(batch_size=batch_size)
        else:
            # Default to InMemoryStrategy for other modes
            processing_strategy = InMemoryStrategy()

        # Process the data with the chosen strategy
        return processing_strategy.process(
            data,
            entity_name=entity_name,
            extract_time=extract_time,
            config=self.config,
            params=params,
        )

    def process_to_format(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
        entity_name: str,
        output_format: str,
        output_path: Optional[str] = None,
        extract_time: Optional[Any] = None,
        auto_detect_mode: bool = True,
        **format_options,
    ) -> ProcessingResult:
        """
        Process data and write directly to the specified output format.

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
        if isinstance(data, (str, bytes)) and os.path.isfile(data):
            # For file paths, use file processing methods
            result = process_file(self, data, entity_name, extract_time)
        else:
            # Process in memory with appropriate mode
            data_iterator = get_data_iterator(self, data)
            result = self._process_data(
                list(data_iterator), entity_name, extract_time, memory_mode
            )

        # Write to output format if path is specified
        if output_path:
            # Create output directory if it doesn't exist
            os.makedirs(output_path, exist_ok=True)

            # Write to the specified format
            result.write(output_format, output_path, **format_options)

        return result


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
]
