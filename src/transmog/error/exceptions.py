"""Custom exceptions for the Transmog package."""

from typing import Optional


class TransmogError(Exception):
    """Base exception class for all Transmog errors."""

    pass


class ProcessingError(TransmogError):
    """Exception raised when an error occurs during data processing."""

    pass


class ValidationError(TransmogError):
    """Exception raised when data validation fails."""

    pass


class MissingDependencyError(TransmogError):
    """Exception raised when an optional dependency is missing."""

    def __init__(self, message: str, *, package: Optional[str] = None) -> None:
        """Initialize MissingDependencyError.

        Args:
            message: Error message
            package: Optional package name that is missing
        """
        super().__init__(message)
        self.package = package


class ConfigurationError(TransmogError):
    """Exception raised when there's a configuration error."""

    pass


class OutputError(TransmogError):
    """Exception raised when there's an error writing output."""

    pass
