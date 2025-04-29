"""
Transmog: JSON to tabular normalization and flattening utility

A library for transforming nested JSON structures into flattened formats
with parent-child relationship preservation and metadata annotation.
"""

__version__ = "0.1.2.5"

# Import dependencies and features early to enable detection
from .dependencies import DependencyManager
from .features import Features

# Core functionality
from transmog.core.flattener import flatten_json
from transmog.core.extractor import extract_arrays
from transmog.core.hierarchy import (
    process_structure,
    process_record_batch,
    process_records_in_single_pass,
)
from transmog.core.metadata import (
    generate_extract_id,
    annotate_with_metadata,
    get_current_timestamp,
    create_batch_metadata,
)

# Naming utilities
from transmog.naming.conventions import (
    get_table_name,
    sanitize_name,
    get_standard_field_name,
)
from transmog.naming.abbreviator import (
    abbreviate_table_name,
    abbreviate_field_name,
    get_common_abbreviations,
    merge_abbreviation_dicts,
)

# High-level processor class for one-step processing
from transmog.process import Processor, ProcessingResult

# Configuration functionality
from transmog.config import (
    TransmogConfig,
    ProcessingMode,
    NamingConfig,
    ProcessingConfig,
    MetadataConfig,
    ErrorHandlingConfig,
    settings,
    extensions,
    load_profile,
    load_config,
    configure,
)

# Error handling and exceptions
from transmog.error import (
    # Exceptions
    TransmogError,
    ProcessingError,
    ValidationError,
    ParsingError,
    FileError,
    CircularReferenceError,
    MissingDependencyError,
    ConfigurationError,
    OutputError,
    # Error handling utilities
    error_context,
    setup_logging,
    # Recovery strategies
    RecoveryStrategy,
    StrictRecovery,
    SkipAndLogRecovery,
    PartialProcessingRecovery,
    with_recovery,
    STRICT,
    DEFAULT,
    LENIENT,
)

# IO utilities
from transmog.io import (
    initialize_io_features,
    get_available_reader_formats,
    get_available_writer_formats,
    has_reader_format,
    has_writer_format,
    detect_format,
    create_writer,
    DataWriter,
)

# Initialize IO features
initialize_io_features()

# Public API
__all__ = [
    # Main classes
    "Processor",
    "ProcessingResult",
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
    "get_available_reader_formats",
    "get_available_writer_formats",
    "has_reader_format",
    "has_writer_format",
    "detect_format",
    "create_writer",
    "DataWriter",
    # Features and dependencies
    "Features",
    "DependencyManager",
    # Exceptions
    "TransmogError",
    "ProcessingError",
    "ValidationError",
    "ParsingError",
    "FileError",
    "CircularReferenceError",
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
]
