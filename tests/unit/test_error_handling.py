"""
Unit tests for error handling functionality.
"""

import json
import os
import pytest
from unittest.mock import patch, MagicMock

from src.transmog.exceptions import (
    CircularReferenceError,
    ConfigurationError,
    FileError,
    MissingDependencyError,
    TransmogError,
    OutputError,
    ParsingError,
    ProcessingError,
    ValidationError,
)
from src.transmog.core.error_handling import (
    error_context,
    handle_circular_reference,
    recover_or_raise,
    safe_json_loads,
    validate_input,
)
from src.transmog.recovery import (
    RecoveryStrategy,
    StrictRecovery,
    SkipAndLogRecovery,
    PartialProcessingRecovery,
    with_recovery,
)


class TestExceptions:
    """Tests for custom exception classes."""

    def test_base_error(self):
        """Test the base TransmogError class."""
        error = TransmogError("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"

    def test_processing_error(self):
        """Test ProcessingError formatting."""
        # Basic error
        error1 = ProcessingError("Failed processing")
        assert "Error processing data: Failed processing" in str(error1)

        # With entity name
        error2 = ProcessingError("Failed processing", entity_name="customers")
        assert "for entity 'customers'" in str(error2)
        assert error2.entity_name == "customers"

        # With data
        test_data = {"id": 123}
        error3 = ProcessingError("Bad data", data=test_data)
        assert error3.data == test_data

    def test_validation_error(self):
        """Test ValidationError formatting with details."""
        # Basic error
        error1 = ValidationError("Invalid input")
        assert "Validation error: Invalid input" in str(error1)

        # With error details
        errors = {"id": "must be an integer", "name": "cannot be empty"}
        error2 = ValidationError("Multiple validation issues", errors=errors)
        assert "Details:" in str(error2)
        assert "id: must be an integer" in str(error2)
        assert "name: cannot be empty" in str(error2)
        assert error2.errors == errors

    def test_parsing_error(self):
        """Test ParsingError with source and line info."""
        # Basic error
        error1 = ParsingError("Malformed JSON")
        assert "JSON parsing error: Malformed JSON" in str(error1)

        # With source
        error2 = ParsingError("Missing brace", source="data.json")
        assert "in data.json" in str(error2)
        assert error2.source == "data.json"

        # With line number
        error3 = ParsingError("Unexpected character", source="data.json", line=42)
        assert "at line 42" in str(error3)
        assert error3.line == 42

    def test_file_error(self):
        """Test FileError with file path and operation."""
        # Basic error
        error1 = FileError("File not found")
        assert "File error: File not found" in str(error1)

        # With file path
        error2 = FileError("Permission denied", file_path="/tmp/data.json")
        assert "for '/tmp/data.json'" in str(error2)
        assert error2.file_path == "/tmp/data.json"

        # With operation
        error3 = FileError(
            "Failed to write", file_path="output.json", operation="write"
        )
        assert "during write" in str(error3)
        assert error3.operation == "write"

    def test_circular_reference_error(self):
        """Test CircularReferenceError with path info."""
        # Basic error
        error1 = CircularReferenceError("Circular reference detected")
        assert "Circular reference detected:" in str(error1)

        # With path
        path = ["root", "items", "0", "parent"]
        error2 = CircularReferenceError("Object references itself", path=path)
        assert "Path: root > items > 0 > parent" in str(error2)
        assert error2.path == path


class TestErrorHandlingUtilities:
    """Tests for error handling utilities."""

    def test_safe_json_loads_valid(self):
        """Test safe_json_loads with valid input."""
        # Valid JSON string
        result = safe_json_loads('{"id": 123, "name": "Test"}')
        assert result == {"id": 123, "name": "Test"}

        # Valid JSON bytes
        result = safe_json_loads(b'{"active": true}')
        assert result == {"active": True}

    def test_safe_json_loads_invalid(self):
        """Test error message with invalid JSON input."""
        with pytest.raises(ParsingError) as exc_info:
            safe_json_loads('{"id": 123, "name": "Test"')

        error = str(exc_info.value)
        assert "Invalid JSON data" in error
        assert '{"id": 123, "name": "Test"' in error

    def test_handle_circular_reference(self):
        """Test circular reference detection."""
        # Set up tracking
        visited = set()
        path = ["root", "items", "0"]

        # Should not raise for new object
        obj_id = 12345
        handle_circular_reference(obj_id, visited, path)
        assert obj_id in visited

        # Should raise for repeated object
        with pytest.raises(CircularReferenceError) as exc_info:
            handle_circular_reference(obj_id, visited, path)
        assert "Path: root > items > 0" in str(exc_info.value)

        # Test max depth checking
        with pytest.raises(CircularReferenceError) as exc_info:
            handle_circular_reference(67890, visited, path, max_depth=2)
        assert "Maximum nesting depth exceeded" in str(exc_info.value)

    def test_error_context_decorator(self):
        """Test the error_context decorator."""

        # Define test function with decorator
        @error_context("Failed during test operation")
        def test_func(succeed=True):
            if not succeed:
                raise ValueError("Test error")
            return "Success"

        # Test successful execution
        assert test_func(succeed=True) == "Success"

        # Test with exception
        with pytest.raises(ValueError) as exc_info:
            test_func(succeed=False)

        # Decorator shouldn't change the exception type by default
        assert type(exc_info.value) == ValueError

        # Test with custom exception wrapper
        @error_context("Custom wrapper test", wrap_as=lambda e: ProcessingError(str(e)))
        def wrapper_func(succeed=True):
            if not succeed:
                raise ValueError("Original error")
            return "Success"

        # Should wrap the ValueError as ProcessingError
        with pytest.raises(ProcessingError) as exc_info:
            wrapper_func(succeed=False)
        assert "Original error" in str(exc_info.value)

    def test_recover_or_raise(self):
        """Test the recover_or_raise function."""

        # Test function
        def test_func(x):
            if x <= 0:
                raise ValueError("Value must be positive")
            return x * 2

        # Recovery function
        def recovery_func(e):
            return 0  # Default recovery value

        # Test successful execution
        assert recover_or_raise(test_func, recovery_func, 5) == 10

        # Test with recovery
        assert recover_or_raise(test_func, recovery_func, -1) == 0

    def test_validate_input(self):
        """Test input validation."""

        # Valid case - correct type
        validate_input("test", str, "test_param")

        # None with allow_none=True
        validate_input(None, str, "nullable_param", allow_none=True)

        # None with allow_none=False
        with pytest.raises(ValidationError) as exc_info:
            validate_input(None, str, "required_param", allow_none=False)
        assert "cannot be None" in str(exc_info.value)

        # Wrong type
        with pytest.raises(ValidationError) as exc_info:
            validate_input(123, str, "string_param")
        assert "expected str, got int" in str(exc_info.value)

        # Multiple accepted types
        validate_input(123, (int, str), "multi_type_param")

        # Custom validation function
        def validate_positive(value):
            if value <= 0:
                return False, "must be positive"
            return True, None

        with pytest.raises(ValidationError) as exc_info:
            validate_input(-1, int, "positive_param", validation_func=validate_positive)
        assert "must be positive" in str(exc_info.value)

        # Custom validation success
        validate_input(10, int, "positive_param", validation_func=validate_positive)


class TestRecoveryStrategies:
    """Tests for recovery strategies."""

    def test_strict_recovery(self):
        """Test StrictRecovery strategy."""
        strategy = StrictRecovery()

        # All methods should re-raise their exceptions
        with pytest.raises(ParsingError):
            strategy.handle_parsing_error(ParsingError("Test error"))

        with pytest.raises(ProcessingError):
            strategy.handle_processing_error(ProcessingError("Test error"))

        with pytest.raises(CircularReferenceError):
            strategy.handle_circular_reference(
                CircularReferenceError("Test error"), path=["path", "to", "error"]
            )

    @patch("src.transmog.recovery.logger")
    def test_skip_and_log_recovery(self, mock_logger):
        """Test SkipAndLogRecovery strategy."""
        strategy = SkipAndLogRecovery()

        # Test handling parsing error
        result = strategy.handle_parsing_error(ParsingError("Bad JSON"))
        assert result is None
        mock_logger.log.assert_called()

        # Test handling processing error
        result = strategy.handle_processing_error(ProcessingError("Failed to process"))
        assert result == {}

        # Test handling circular reference
        result = strategy.handle_circular_reference(
            CircularReferenceError("Circular reference"), path=["root", "items", "0"]
        )
        assert result == {}

        # Test handling file error
        result = strategy.handle_file_error(FileError("File not found"))
        assert result is None

    @patch("src.transmog.recovery.logger")
    def test_partial_recovery(self, mock_logger):
        """Test PartialProcessingRecovery strategy."""
        strategy = PartialProcessingRecovery()

        # Test handling processing error with recoverable data
        test_data = {"id": 123, "name": "Test", "items": [{"invalid": "structure"}]}
        error = ProcessingError("Failed processing nested structure")
        error.data = test_data

        result = strategy.handle_processing_error(error)
        # Should extract top-level scalar fields
        assert "id" in result
        assert "name" in result
        assert "items" not in result  # Should skip the array

        # Test handling circular reference
        result = strategy.handle_circular_reference(
            CircularReferenceError("Circular reference"), path=["root", "items", "0"]
        )
        assert result["__circular_reference"] is True
        assert "root > items > 0" in result["__reference_path"]

    def test_with_recovery(self):
        """Test the with_recovery utility function."""

        def problematic_func(data):
            if "error_type" not in data:
                return data["value"] * 2

            if data["error_type"] == "parsing":
                raise ParsingError("Parse error")
            elif data["error_type"] == "processing":
                raise ProcessingError("Process error")
            elif data["error_type"] == "circular":
                e = CircularReferenceError("Circular error")
                e.path = ["a", "b"]
                raise e
            elif data["error_type"] == "file":
                raise FileError("File error")
            else:
                raise ValueError("Unknown error")

        # Test with StrictRecovery - should re-raise
        with pytest.raises(ParsingError):
            with_recovery(
                problematic_func,
                strategy=StrictRecovery(),
                data={"error_type": "parsing"},
            )

        # Test with SkipAndLogRecovery
        result = with_recovery(
            problematic_func,
            strategy=SkipAndLogRecovery(),
            data={"error_type": "processing"},
        )
        assert result == {}

        # Test with PartialProcessingRecovery and circular reference
        result = with_recovery(
            problematic_func,
            strategy=PartialProcessingRecovery(),
            data={"error_type": "circular"},
        )
        assert result["__circular_reference"] is True

        # Test with successful execution
        result = with_recovery(
            problematic_func, strategy=StrictRecovery(), data={"value": 5}
        )
        assert result == 10
