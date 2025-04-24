"""
Custom exceptions for the Transmogrify package.

This module defines specific exception types to improve error handling
and provide more informative error messages for common failure scenarios.
"""

from typing import Any, Dict, List, Optional, Union


class TransmogrifyError(Exception):
    """Base exception class for all Transmogrify errors."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class ProcessingError(TransmogrifyError):
    """Exception raised when there's an error during data processing."""

    def __init__(
        self,
        message: str,
        entity_name: Optional[str] = None,
        data: Optional[Any] = None,
    ):
        self.entity_name = entity_name
        self.data = data
        full_message = f"Error processing data"
        if entity_name:
            full_message += f" for entity '{entity_name}'"
        full_message += f": {message}"
        super().__init__(full_message)


class ValidationError(TransmogrifyError):
    """Exception raised when input data validation fails."""

    def __init__(self, message: str, errors: Optional[Dict[str, str]] = None):
        self.errors = errors or {}
        error_details = ""
        if errors:
            error_details = ". Details: " + ", ".join(
                f"{k}: {v}" for k, v in errors.items()
            )
        super().__init__(f"Validation error: {message}{error_details}")


class ParsingError(TransmogrifyError):
    """Exception raised when JSON parsing fails."""

    def __init__(
        self, message: str, source: Optional[str] = None, line: Optional[int] = None
    ):
        self.source = source
        self.line = line
        location_info = ""
        if source:
            location_info += f" in {source}"
        if line:
            location_info += f" at line {line}"
        super().__init__(f"JSON parsing error{location_info}: {message}")


class FileError(TransmogrifyError):
    """Exception raised for file-related errors."""

    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        operation: Optional[str] = None,
    ):
        self.file_path = file_path
        self.operation = operation
        op_info = f" during {operation}" if operation else ""
        path_info = f" for '{file_path}'" if file_path else ""
        super().__init__(f"File error{op_info}{path_info}: {message}")


class CircularReferenceError(TransmogrifyError):
    """Exception raised when a circular reference is detected."""

    def __init__(self, message: str, path: Optional[List[str]] = None):
        self.path = path
        path_info = ""
        if path:
            path_info = f" Path: {' > '.join(path)}"
        super().__init__(f"Circular reference detected: {message}.{path_info}")


class MissingDependencyError(TransmogrifyError):
    """Exception raised when an optional dependency is missing."""

    def __init__(self, message: str, package: str, feature: Optional[str] = None):
        self.package = package
        self.feature = feature
        feature_info = f" for {feature}" if feature else ""
        install_info = f"\nPlease install {package} with: pip install transmogrify[{feature or 'all'}]"
        super().__init__(f"Missing dependency{feature_info}: {message}.{install_info}")


class ConfigurationError(TransmogrifyError):
    """Exception raised when there's a configuration error."""

    def __init__(
        self, message: str, param: Optional[str] = None, value: Optional[Any] = None
    ):
        self.param = param
        self.value = value
        param_info = ""
        if param:
            param_info = f" (parameter: '{param}'"
            if value is not None:
                param_info += f", value: '{value}'"
            param_info += ")"
        super().__init__(f"Configuration error{param_info}: {message}")


class OutputError(TransmogrifyError):
    """Exception raised when there's an error writing output."""

    def __init__(
        self,
        message: str,
        format: Optional[str] = None,
        path: Optional[str] = None,
        table: Optional[str] = None,
    ):
        self.format = format
        self.path = path
        self.table = table
        details = ""
        if format:
            details += f" in {format} format"
        if table:
            details += f" for table '{table}'"
        if path:
            details += f" to '{path}'"
        super().__init__(f"Output error{details}: {message}")
