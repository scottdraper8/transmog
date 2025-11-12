"""Custom exceptions for the Transmog package."""


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

    def __init__(self, message: str, package: str | None = None) -> None:
        """Initialize the exception with an error message and optional package name.

        Args:
            message: Error message describing the missing dependency
            package: Name of the missing package
        """
        super().__init__(message)
        self.package = package


class ConfigurationError(TransmogError):
    """Exception raised when there's a configuration error."""

    pass


class OutputError(TransmogError):
    """Exception raised when there's an error writing output."""

    pass


__all__ = [
    "TransmogError",
    "ProcessingError",
    "ValidationError",
    "MissingDependencyError",
    "ConfigurationError",
    "OutputError",
]
