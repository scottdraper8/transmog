"""Custom exceptions for the Transmog package."""


class TransmogError(Exception):
    """Base exception class for all Transmog errors."""

    pass


class ValidationError(TransmogError):
    """Exception raised when data validation or processing fails."""

    pass


class MissingDependencyError(TransmogError):
    """Exception raised when an optional dependency is missing."""

    pass


class ConfigurationError(TransmogError):
    """Exception raised when there's a configuration error."""

    pass


class OutputError(TransmogError):
    """Exception raised when there's an error writing output."""

    pass


__all__ = [
    "TransmogError",
    "ValidationError",
    "MissingDependencyError",
    "ConfigurationError",
    "OutputError",
]
