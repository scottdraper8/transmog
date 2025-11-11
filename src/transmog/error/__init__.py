"""Error handling package for Transmog."""

import logging

from .exceptions import (
    ConfigurationError,
    MissingDependencyError,
    OutputError,
    ProcessingError,
    TransmogError,
    ValidationError,
)

logger = logging.getLogger(__name__)

__all__ = [
    "TransmogError",
    "ProcessingError",
    "ValidationError",
    "MissingDependencyError",
    "ConfigurationError",
    "OutputError",
    "logger",
]
