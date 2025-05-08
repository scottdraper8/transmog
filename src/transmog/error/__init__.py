"""
Error handling package for Transmog.

This package provides error handling, logging, and recovery
functionality for the Transmog package.
"""

from .exceptions import (
    TransmogError,
    ProcessingError,
    ValidationError,
    ParsingError,
    FileError,
    MissingDependencyError,
    ConfigurationError,
    OutputError,
)

from .handling import (
    error_context,
    setup_logging,
    safe_json_loads,
    check_dependency,
    require_dependency,
    try_with_recovery,
    validate_input,
)

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

# Import logger for convenience
from .handling import logger

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
