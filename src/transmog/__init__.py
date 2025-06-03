"""Transmog - Flatten, transform, and organize complex data.

Provides tools to transform deeply nested JSON structures into flattened tables.
"""

__version__ = "1.0.6"

# Configuration functionality
from transmog.config import (
    ErrorHandlingConfig,
    MetadataConfig,
    NamingConfig,
    ProcessingConfig,
    ProcessingMode,
    TransmogConfig,
    configure,
    extensions,
    load_config,
    load_profile,
    settings,
)
from transmog.core.extractor import extract_arrays, stream_extract_arrays

# Core functionality
from transmog.core.flattener import flatten_json
from transmog.core.hierarchy import (
    process_record_batch,
    process_records_in_single_pass,
    process_structure,
    stream_process_records,
)
from transmog.core.metadata import (
    annotate_with_metadata,
    create_batch_metadata,
    generate_extract_id,
    get_current_timestamp,
)

# Error handling and exceptions
from transmog.error import (
    DEFAULT,
    LENIENT,
    STRICT,
    ConfigurationError,
    FileError,
    MissingDependencyError,
    OutputError,
    ParsingError,
    PartialProcessingRecovery,
    ProcessingError,
    # Recovery strategies
    RecoveryStrategy,
    SkipAndLogRecovery,
    StrictRecovery,
    # Exceptions
    TransmogError,
    ValidationError,
    # Error handling utilities
    error_context,
    setup_logging,
    with_recovery,
)

# IO utilities
from transmog.io import (
    DataWriter,
    FormatRegistry,
    # Streaming writer interface
    StreamingWriter,
    create_streaming_writer,
    create_writer,
    detect_format,
    get_supported_streaming_formats,
    initialize_io_features,
    is_streaming_format_available,
)

# Specific writer implementations
from transmog.io.writers.parquet import ParquetStreamingWriter

# Naming utilities
from transmog.naming.conventions import (
    get_standard_field_name,
    get_table_name,
    handle_deeply_nested_path,
    sanitize_name,
)

# High-level processor class for one-step processing
from transmog.process import (
    BatchStrategy,
    ChunkedStrategy,
    CSVStrategy,
    FileStrategy,
    InMemoryStrategy,
    ProcessingResult,
    ProcessingStrategy,
    Processor,
)

# Alias the DependencyManager for backwards compatibility
from .dependencies import DependencyManager, DependencyManager as IoDependencyManager
from .features import Features

# Initialize IO features
initialize_io_features()

# Public API
__all__ = [
    # Main classes
    "Processor",
    "ProcessingResult",
    # Strategy pattern classes
    "ProcessingStrategy",
    "InMemoryStrategy",
    "FileStrategy",
    "BatchStrategy",
    "ChunkedStrategy",
    "CSVStrategy",
    # Configuration
    "TransmogConfig",
    "ProcessingMode",
    "NamingConfig",
    "ProcessingConfig",
    "MetadataConfig",
    "ErrorHandlingConfig",
    # Configuration utilities
    "settings",
    "extensions",
    "load_profile",
    "load_config",
    "configure",
    # Format utilities
    "FormatRegistry",
    "detect_format",
    "create_writer",
    "DataWriter",
    # Streaming features
    "StreamingWriter",
    "create_streaming_writer",
    "get_supported_streaming_formats",
    "is_streaming_format_available",
    # Specific writer implementations
    "ParquetStreamingWriter",
    # Features and dependencies
    "Features",
    "DependencyManager",
    "IoDependencyManager",
    # Exceptions
    "TransmogError",
    "ProcessingError",
    "ValidationError",
    "ParsingError",
    "FileError",
    "MissingDependencyError",
    "ConfigurationError",
    "OutputError",
    # Error handling
    "error_context",
    "setup_logging",
    # Recovery strategies
    "RecoveryStrategy",
    "StrictRecovery",
    "SkipAndLogRecovery",
    "PartialProcessingRecovery",
    "with_recovery",
    "STRICT",
    "DEFAULT",
    "LENIENT",
    # Core functionality
    "flatten_json",
    "process_record_batch",
    "process_records_in_single_pass",
    "process_structure",
    "stream_process_records",
    "extract_arrays",
    "stream_extract_arrays",
    # Metadata
    "annotate_with_metadata",
    "create_batch_metadata",
    "generate_extract_id",
    "get_current_timestamp",
    # Naming utilities
    "get_standard_field_name",
    "get_table_name",
    "sanitize_name",
    "handle_deeply_nested_path",
]
