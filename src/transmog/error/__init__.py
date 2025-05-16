"""Error handling package for Transmog.

This package provides error handling, logging, and recovery
functionality for the Transmog package.
"""

from .exceptions import (
    ConfigurationError,
    FileError,
    MissingDependencyError,
    OutputError,
    ParsingError,
    ProcessingError,
    TransmogError,
    ValidationError,
)

# Error handling utilities
from .handling import (
    check_dependency,
    error_context,
    logger,
    require_dependency,
    safe_json_loads,
    setup_logging,
    try_with_recovery,
    validate_input,
)
from .recovery import (
    DEFAULT,
    LENIENT,
    STRICT,
    PartialProcessingRecovery,
    RecoveryStrategy,
    SkipAndLogRecovery,
    StrictRecovery,
    with_recovery,
)

# Public API
__all__ = [
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
    "safe_json_loads",
    "check_dependency",
    "require_dependency",
    "try_with_recovery",
    "validate_input",
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
