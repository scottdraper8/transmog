"""
Unit tests for error handling functionality.
"""

import json
import os
import pytest
from unittest.mock import patch, MagicMock

from transmog.error import (
    CircularReferenceError,
    ConfigurationError,
    FileError,
    MissingDependencyError,
    TransmogError,
    OutputError,
    ParsingError,
    ProcessingError,
    ValidationError,
    error_context,
    handle_circular_reference,
    try_with_recovery as recover_or_raise,
    safe_json_loads,
    validate_input,
    RecoveryStrategy,
    StrictRecovery,
    SkipAndLogRecovery,
    PartialProcessingRecovery,
    with_recovery,
    logger,
    STRICT,
    DEFAULT,
    LENIENT,
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
        assert "must be of type str, got int" in str(exc_info.value)

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
    """Test the recovery strategy classes."""

    def test_strict_recovery(self):
        """Test that StrictRecovery raises the original exception."""
        strategy = StrictRecovery()
        error = ValueError("test error")

        # Test handling of a generic error wrapped as ProcessingError
        with pytest.raises(ValueError) as exc_info:
            strategy.handle_processing_error(error)
        assert str(exc_info.value) == "test error"

        # Test parsing error handling
        parse_error = ParsingError("Invalid JSON")
        with pytest.raises(ParsingError) as exc_info:
            strategy.handle_parsing_error(parse_error)
        assert (
            exc_info.value is parse_error
        )  # Check identity instead of string comparison

        # Test file error handling
        file_error = FileError("File not found")
        with pytest.raises(FileError) as exc_info:
            strategy.handle_file_error(file_error)
        assert (
            exc_info.value is file_error
        )  # Check identity instead of string comparison

        # Test circular reference handling
        circular_error = CircularReferenceError("Circular reference")
        with pytest.raises(CircularReferenceError) as exc_info:
            strategy.handle_circular_reference(circular_error, ["path", "to", "error"])
        assert (
            exc_info.value is circular_error
        )  # Check identity instead of string comparison

    def test_skip_and_log_recovery(self):
        """Test that SkipAndLogRecovery logs and returns empty values."""
        strategy = SkipAndLogRecovery()
        mock_logger = MagicMock()

        with patch.object(logger, "log", mock_logger):
            # Test processing error handling
            error = ProcessingError("Process failed", entity_name="test_entity")
            result = strategy.handle_processing_error(error, entity_name="test_entity")

            # Just check that logger was called with the right level
            mock_logger.assert_called_once()
            assert mock_logger.call_args[0][0] == strategy.log_level
            # Check that the error message contains key parts
            log_msg = mock_logger.call_args[0][1]
            assert "Skipping record due to processing error" in log_msg
            assert "test_entity" in log_msg
            assert "Process failed" in log_msg

            # Verify correct return value
            assert result == {}

            # Test parsing error handling
            mock_logger.reset_mock()
            parse_error = ParsingError("Invalid JSON", source="test.json")
            result = strategy.handle_parsing_error(parse_error, source="test.json")

            # Verify logger was called
            mock_logger.assert_called_once()
            assert mock_logger.call_args[0][0] == strategy.log_level
            log_msg = mock_logger.call_args[0][1]
            assert "Skipping record due to parsing error" in log_msg
            assert "test.json" in log_msg
            assert "Invalid JSON" in log_msg

            # Verify correct return value
            assert result == {}

            # Test file error handling
            mock_logger.reset_mock()
            file_error = FileError("Cannot read file", file_path="/path/to/file.json")
            result = strategy.handle_file_error(
                file_error, file_path="/path/to/file.json"
            )

            # Verify logger was called
            mock_logger.assert_called_once()
            assert mock_logger.call_args[0][0] == strategy.log_level
            log_msg = mock_logger.call_args[0][1]
            assert "File operation failed" in log_msg
            assert "/path/to/file.json" in log_msg
            assert "Cannot read file" in log_msg

            # Verify correct return value
            assert result == []

    def test_partial_recovery(self):
        """Test that PartialProcessingRecovery returns partial results."""
        strategy = PartialProcessingRecovery()
        mock_logger = MagicMock()

        with patch.object(logger, "log", mock_logger):
            # Test processing error with data
            data = {"id": 123, "name": "Test"}
            error = ProcessingError("Process failed", data=data)
            result = strategy.handle_processing_error(error)

            # Verify logger was called
            mock_logger.assert_called_once()
            assert mock_logger.call_args[0][0] == strategy.log_level
            log_msg = mock_logger.call_args[0][1]
            assert "Attempting partial recovery from processing error" in log_msg
            assert "Process failed" in log_msg

            # Verify correct return value contains original data with error marker
            assert result == {"id": 123, "name": "Test", "_error": str(error)}

            # Test circular reference handling
            mock_logger.reset_mock()
            path = ["root", "items", "0", "parent"]
            circular_error = CircularReferenceError(
                "Circular reference detected", path=path
            )
            result = strategy.handle_circular_reference(circular_error, path)

            # Verify logger was called
            mock_logger.assert_called_once()
            assert mock_logger.call_args[0][0] == strategy.log_level
            log_msg = mock_logger.call_args[0][1]
            assert "Truncating circular reference at path" in log_msg
            assert "root > items > 0 > parent" in log_msg
            assert "Circular reference detected" in log_msg

            # Verify correct return value
            assert result["_circular_reference"] is True
            assert result["_path"] == "root > items > 0 > parent"
            assert "_error" in result

            # Test file error for a file that exists
            mock_logger.reset_mock()
            file_error = FileError("Format error")

            # Create a temporary file for testing
            with patch("os.path.exists", return_value=True):
                with patch("builtins.open", create=True) as mock_open:
                    mock_open.return_value.__enter__.return_value.read.return_value = (
                        "line1\nline2\nline3"
                    )
                    result = strategy.handle_file_error(
                        file_error, file_path="test.txt"
                    )

                    # Verify logger was called
                    mock_logger.assert_called_once()
                    assert mock_logger.call_args[0][0] == strategy.log_level
                    log_msg = mock_logger.call_args[0][1]
                    assert "Attempting partial recovery from file error" in log_msg
                    assert "test.txt" in log_msg
                    assert "Format error" in log_msg

                    # Verify correct return value is a list of lines with line numbers
                    assert len(result) == 3
                    assert result[0]["_line"] == 1
                    assert result[0]["_content"] == "line1"


class TestWithRecovery:
    """Test the with_recovery decorator."""

    def test_decorator_usage(self):
        """Test using with_recovery as a decorator."""

        # Set up test function with decorator
        @with_recovery(strategy=SkipAndLogRecovery())
        def process_data(data):
            if data < 0:
                raise ValidationError("Data must be positive")
            elif data == 0:
                raise ParsingError("Cannot process zero")
            return data * 2

        # Test successful execution
        assert process_data(5) == 10

        # Test recovery from validation error
        result = process_data(-1)
        assert result == {}  # SkipAndLogRecovery returns empty dict

        # Test recovery from parsing error
        result = process_data(0)
        assert result == {}  # SkipAndLogRecovery returns empty dict

    def test_function_wrapper_usage(self):
        """Test using with_recovery as a function wrapper."""

        # Set up test function
        def process_data(data):
            if isinstance(data, dict) and "id" not in data:
                raise ProcessingError("Missing ID field", data=data)
            elif isinstance(data, str) and not data:
                raise ParsingError("Empty string")
            return data

        # Test successful execution
        test_data = {"id": 123, "name": "Test"}
        assert with_recovery(process_data, strategy=STRICT, data=test_data) == test_data

        # Test with SkipAndLogRecovery
        bad_data = {"name": "Test", "value": 42}  # Missing ID
        result = with_recovery(process_data, strategy=DEFAULT, data=bad_data)
        assert result == {}  # SkipAndLogRecovery returns empty dict

        # Test with PartialProcessingRecovery
        result = with_recovery(process_data, strategy=LENIENT, data=bad_data)
        assert result["name"] == "Test"  # Original data is preserved
        assert result["value"] == 42  # Original data is preserved
        assert "_error" in result  # Error info is added

    def test_different_error_types(self):
        """Test recovery from different error types."""

        # Test function that raises different error types
        def complex_process(input_type, path=None):
            if input_type == "parsing":
                raise ParsingError("Parse failed", source="test.json")
            elif input_type == "circular":
                raise CircularReferenceError("Circle detected", path=path or ["a", "b"])
            elif input_type == "file":
                raise FileError("File error", file_path="test.txt")
            elif input_type == "generic":
                raise ValueError("Generic error")
            return "Success"

        # Test with PartialProcessingRecovery
        strategy = PartialProcessingRecovery()

        # Test parsing error recovery
        result = with_recovery(complex_process, strategy=strategy, input_type="parsing")
        assert isinstance(result, dict)

        # Test circular reference recovery
        result = with_recovery(
            complex_process, strategy=strategy, input_type="circular"
        )
        assert result["_circular_reference"] is True

        # Test file error recovery
        with patch("os.path.exists", return_value=False):
            result = with_recovery(
                complex_process, strategy=strategy, input_type="file"
            )
            assert result == []  # Empty list for file errors when file doesn't exist

        # Test generic error recovery (wrapped as ProcessingError)
        # When a generic error is wrapped as ProcessingError,
        # it will have no associated data, so it becomes an empty dict with an error marker
        result = with_recovery(complex_process, strategy=strategy, input_type="generic")
        # Since we don't have data to recover, we'll just verify we get a dict of some kind
        assert isinstance(result, dict)
