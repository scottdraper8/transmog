"""
Error handling module for Transmog package.

This module provides exception types, error handling utilities, and recovery strategies
for handling errors throughout the Transmog package.
"""

# Import exceptions
from .exceptions import (
    TransmogError,
    ProcessingError,
    ValidationError,
    ParsingError,
    FileError,
    CircularReferenceError,
    MissingDependencyError,
    ConfigurationError,
    OutputError,
)

# Import error handling utilities
from .handling import (
    error_context,
    setup_logging,
    safe_json_loads,
    check_dependency,
    require_dependency,
    handle_circular_reference,
    validate_input,
    try_with_recovery,
    logger,
)

# Import recovery strategies
from .recovery import (
    RecoveryStrategy,
    StrictRecovery,
    SkipAndLogRecovery,
    PartialProcessingRecovery,
    with_recovery,
    STRICT,
    DEFAULT,
    LENIENT,
)

__all__ = [
    # Base exceptions
    "TransmogError",
    "ProcessingError",
    "ValidationError",
    "ParsingError",
    "FileError",
    "CircularReferenceError",
    "MissingDependencyError",
    "ConfigurationError",
    "OutputError",
    # Error handling utilities
    "error_context",
    "setup_logging",
    "safe_json_loads",
    "check_dependency",
    "require_dependency",
    "handle_circular_reference",
    "validate_input",
    "try_with_recovery",
    "logger",
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
