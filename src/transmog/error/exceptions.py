"""Custom exceptions for the Transmog package.

This module defines specific exception types to improve error handling
and provide more informative error messages for common failure scenarios.
"""

from typing import Any, Optional


class TransmogError(Exception):
    """Base exception class for all Transmog errors."""

    def __init__(self, message: str):
        """Initialize the exception with a message.

        Args:
            message: Error message
        """
        self.message = message
        self.recover_strategy = "strict"  # Default recovery strategy
        super().__init__(self.message)


class ProcessingError(TransmogError):
    """Exception raised when an error occurs during data processing.

    This exception is typically raised when there's an issue with the data
    processing pipeline, such as encountering malformed data or when a
    transformation fails.
    """

    def __init__(
        self,
        message: str,
        entity_name: Optional[str] = None,
        data: Optional[Any] = None,
    ):
        """Initialize the processing error.

        Args:
            message: Error message
            entity_name: Name of the entity being processed
            data: Problematic data
        """
        self.entity_name = entity_name
        self.data = data
        full_message = "Error processing data"
        if entity_name:
            full_message += f" for entity '{entity_name}'"
        full_message += f": {message}"
        super().__init__(full_message)


class ValidationError(TransmogError):
    """Exception raised when data validation fails.

    This exception is raised when input data fails validation checks,
    such as required fields missing, invalid data types, or values
    outside acceptable ranges.
    """

    def __init__(self, message: str, errors: Optional[dict[str, str]] = None):
        """Initialize validation error with message and errors dict.

        Args:
            message: Error message
            errors: Dictionary of field-specific errors
        """
        self.errors = errors or {}
        error_details = ""
        if errors:
            error_details = ". Details: " + ", ".join(
                f"{k}: {v}" for k, v in errors.items()
            )
        super().__init__(f"Validation error: {message}{error_details}")


class ParsingError(TransmogError):
    """Exception raised when parsing input data fails.

    This exception is raised when there's an issue parsing input data,
    such as invalid JSON, CSV formatting errors, or other syntax issues
    with the input data.
    """

    def __init__(
        self, message: str, source: Optional[str] = None, line: Optional[int] = None
    ):
        """Initialize parsing error with source information.

        Args:
            message: Error message
            source: Source of the error (e.g., file path)
            line: Line number where error occurred
        """
        self.source = source
        self.line = line
        location_info = ""
        if source:
            location_info += f" in {source}"
        if line:
            location_info += f" at line {line}"
        super().__init__(f"JSON parsing error{location_info}: {message}")


class FileError(TransmogError):
    """Exception raised when file operations fail.

    This exception is raised for file-related issues, such as when a file
    cannot be read, written, or when there are permission or access problems.
    It provides context about which file operation failed and where.
    """

    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        operation: Optional[str] = None,
    ):
        """Initialize file error with file details.

        Args:
            message: Error message
            file_path: Path to the problematic file
            operation: Operation that failed
        """
        self.file_path = file_path
        self.operation = operation
        op_info = f" during {operation}" if operation else ""
        path_info = f" for '{file_path}'" if file_path else ""
        super().__init__(f"File error{op_info}{path_info}: {message}")


class MissingDependencyError(TransmogError):
    """Exception raised when an optional dependency is missing."""

    def __init__(self, message: str, package: str, feature: Optional[str] = None):
        """Initialize dependency error.

        Args:
            message: Error message
            package: Missing package name
            feature: Feature that requires the package
        """
        self.package = package
        self.feature = feature
        feature_info = f" for {feature}" if feature else ""
        install_info = (
            f"\nPlease install {package} with: pip install transmog[{feature or 'all'}]"
        )
        super().__init__(f"Missing dependency{feature_info}: {message}.{install_info}")


class ConfigurationError(TransmogError):
    """Exception raised when there's a configuration error."""

    def __init__(
        self, message: str, param: Optional[str] = None, value: Optional[Any] = None
    ):
        """Initialize configuration error.

        Args:
            message: Error message
            param: Parameter that caused the error
            value: Invalid value
        """
        self.param = param
        self.value = value
        param_info = ""
        if param:
            param_info = f" (parameter: '{param}'"
            if value is not None:
                param_info += f", value: '{value}'"
            param_info += ")"
        super().__init__(f"Configuration error{param_info}: {message}")


class OutputError(TransmogError):
    """Exception raised when there's an error writing output."""

    def __init__(
        self,
        message: str,
        output_format: Optional[str] = None,
        path: Optional[str] = None,
        table: Optional[str] = None,
    ):
        """Initialize output error.

        Args:
            message: Error message
            output_format: Output format
            path: Output path
            table: Table name
        """
        self.format = output_format
        self.path = path
        self.table = table
        details = ""
        if output_format:
            details += f" in {output_format} format"
        if table:
            details += f" for table '{table}'"
        if path:
            details += f" to '{path}'"
        super().__init__(f"Output error{details}: {message}")
