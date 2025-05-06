"""
Error handling interface tests.

This module defines abstract test classes for error handling components
that all implementations must satisfy.
"""

import pytest
from typing import Any, Dict, List, Callable, Optional, Type


class AbstractErrorTest:
    """
    Abstract test class for exception classes.

    All error classes must pass these tests to ensure consistent behavior.
    """

    @pytest.fixture
    def base_error_class(self):
        """
        Fixture to provide the base error class.

        Implementations must override this to provide the actual base error class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def processing_error_class(self):
        """
        Fixture to provide the processing error class.

        Implementations must override this to provide the actual processing error class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def validation_error_class(self):
        """
        Fixture to provide the validation error class.

        Implementations must override this to provide the actual validation error class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def parsing_error_class(self):
        """
        Fixture to provide the parsing error class.

        Implementations must override this to provide the actual parsing error class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def file_error_class(self):
        """
        Fixture to provide the file error class.

        Implementations must override this to provide the actual file error class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def circular_reference_error_class(self):
        """
        Fixture to provide the circular reference error class.

        Implementations must override this to provide the actual circular reference error class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    def test_base_error(self, base_error_class):
        """Test the base error class."""
        error = base_error_class("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"

    def test_processing_error(self, processing_error_class):
        """Test processing error formatting."""
        # Basic error
        error1 = processing_error_class("Failed processing")
        assert "Error processing data: Failed processing" in str(error1)

        # With entity name
        error2 = processing_error_class("Failed processing", entity_name="customers")
        assert "for entity 'customers'" in str(error2)
        assert error2.entity_name == "customers"

        # With data
        test_data = {"id": 123}
        error3 = processing_error_class("Bad data", data=test_data)
        assert error3.data == test_data

    def test_validation_error(self, validation_error_class):
        """Test validation error formatting with details."""
        # Basic error
        error1 = validation_error_class("Invalid input")
        assert "Validation error: Invalid input" in str(error1)

        # With error details
        errors = {"id": "must be an integer", "name": "cannot be empty"}
        error2 = validation_error_class("Multiple validation issues", errors=errors)
        assert "Details:" in str(error2)
        assert "id: must be an integer" in str(error2)
        assert "name: cannot be empty" in str(error2)
        assert error2.errors == errors

    def test_parsing_error(self, parsing_error_class):
        """Test parsing error with source and line info."""
        # Basic error
        error1 = parsing_error_class("Malformed JSON")
        assert "JSON parsing error: Malformed JSON" in str(error1)

        # With source
        error2 = parsing_error_class("Missing brace", source="data.json")
        assert "in data.json" in str(error2)
        assert error2.source == "data.json"

        # With line number
        error3 = parsing_error_class(
            "Unexpected character", source="data.json", line=42
        )
        assert "at line 42" in str(error3)
        assert error3.line == 42

    def test_file_error(self, file_error_class):
        """Test file error with file path and operation."""
        # Basic error
        error1 = file_error_class("File not found")
        assert "File error: File not found" in str(error1)

        # With file path
        error2 = file_error_class("Permission denied", file_path="/tmp/data.json")
        assert "for '/tmp/data.json'" in str(error2)
        assert error2.file_path == "/tmp/data.json"

        # With operation
        error3 = file_error_class(
            "Failed to write", file_path="output.json", operation="write"
        )
        assert "during write" in str(error3)
        assert error3.operation == "write"

    def test_circular_reference_error(self, circular_reference_error_class):
        """Test circular reference error with path info."""
        # Basic error
        error1 = circular_reference_error_class("Circular reference detected")
        assert "Circular reference detected:" in str(error1)

        # With path
        path = ["root", "items", "0", "parent"]
        error2 = circular_reference_error_class("Object references itself", path=path)
        assert "Path: root > items > 0 > parent" in str(error2)
        assert error2.path == path


class AbstractErrorHandlingUtilitiesTest:
    """
    Abstract test class for error handling utilities.

    All error handling utility implementations must pass these tests.
    """

    @pytest.fixture
    def error_context_decorator(self):
        """
        Fixture to provide the error_context decorator.

        Implementations must override this to provide the actual decorator.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def safe_json_loads_func(self):
        """
        Fixture to provide the safe_json_loads function.

        Implementations must override this to provide the actual function.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def handle_circular_reference_func(self):
        """
        Fixture to provide the handle_circular_reference function.

        Implementations must override this to provide the actual function.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def parsing_error_class(self):
        """
        Fixture to provide the parsing error class.

        Implementations must override this to provide the actual parsing error class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def circular_reference_error_class(self):
        """
        Fixture to provide the circular reference error class.

        Implementations must override this to provide the actual circular reference error class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def processing_error_class(self):
        """
        Fixture to provide the processing error class.

        Implementations must override this to provide the actual processing error class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    def test_safe_json_loads_valid(self, safe_json_loads_func):
        """Test safe_json_loads with valid input."""
        # Valid JSON string
        result = safe_json_loads_func('{"id": 123, "name": "Test"}')
        assert result == {"id": 123, "name": "Test"}

        # Valid JSON bytes
        result = safe_json_loads_func(b'{"active": true}')
        assert result == {"active": True}

    def test_safe_json_loads_invalid(self, safe_json_loads_func, parsing_error_class):
        """Test error message with invalid JSON input."""
        with pytest.raises(parsing_error_class) as exc_info:
            safe_json_loads_func('{"id": 123, "name": "Test"')

        error = str(exc_info.value)
        assert "Invalid JSON data" in error or "JSON parsing error" in error

    def test_handle_circular_reference(
        self, handle_circular_reference_func, circular_reference_error_class
    ):
        """Test circular reference detection."""
        # Set up tracking
        visited = set()
        path = ["root", "items", "0"]

        # Should not raise for new object
        obj_id = 12345
        handle_circular_reference_func(obj_id, visited, path)
        assert obj_id in visited

        # Should raise for repeated object
        with pytest.raises(circular_reference_error_class) as exc_info:
            handle_circular_reference_func(obj_id, visited, path)
        assert "Path: root > items > 0" in str(exc_info.value)

        # Test max depth checking
        with pytest.raises(circular_reference_error_class) as exc_info:
            handle_circular_reference_func(67890, visited, path, max_depth=2)
        assert "Maximum nesting depth exceeded" in str(exc_info.value)

    def test_error_context_decorator(
        self, error_context_decorator, processing_error_class
    ):
        """Test the error_context decorator."""

        # Define test function with decorator
        @error_context_decorator("Failed during test operation")
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
        @error_context_decorator(
            "Custom wrapper test", wrap_as=lambda e: processing_error_class(str(e))
        )
        def wrapper_func(succeed=True):
            if not succeed:
                raise ValueError("Original error")
            return "Success"

        # Should wrap the ValueError as ProcessingError
        with pytest.raises(processing_error_class) as exc_info:
            wrapper_func(succeed=False)
        assert "Original error" in str(exc_info.value)


class AbstractRecoveryStrategyTest:
    """
    Abstract test class for recovery strategies.

    All recovery strategy implementations must pass these tests.
    """

    @pytest.fixture
    def strict_recovery_class(self):
        """
        Fixture to provide the strict recovery strategy class.

        Implementations must override this to provide the actual class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def skip_and_log_recovery_class(self):
        """
        Fixture to provide the skip and log recovery strategy class.

        Implementations must override this to provide the actual class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def partial_processing_recovery_class(self):
        """
        Fixture to provide the partial processing recovery strategy class.

        Implementations must override this to provide the actual class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def with_recovery_decorator(self):
        """
        Fixture to provide the with_recovery decorator.

        Implementations must override this to provide the actual decorator.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def try_with_recovery_func(self):
        """
        Fixture to provide the try_with_recovery function.

        Implementations must override this to provide the actual function.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    def test_strict_recovery(self, strict_recovery_class):
        """Test strict recovery strategy."""
        strategy = strict_recovery_class()

        # Create a function that can succeed or fail
        def test_func(data):
            if data["should_fail"]:
                raise ValueError("Test error")
            return {"result": "success"}

        # Test with successful data
        result = strategy.recover_or_raise(
            test_func, {"should_fail": False}, ValueError, "entity"
        )
        assert result == {"result": "success"}

        # Test with failing data - should raise
        with pytest.raises(ValueError):
            strategy.recover_or_raise(
                test_func, {"should_fail": True}, ValueError, "entity"
            )

    def test_skip_and_log_recovery(self, skip_and_log_recovery_class):
        """Test skip and log recovery strategy."""
        strategy = skip_and_log_recovery_class()

        # List to collect processed results
        processed_results = []

        # Create a function that processes a batch and can fail on some items
        def process_batch(items):
            results = []
            for item in items:
                if item.get("should_fail"):
                    raise ValueError(f"Failed on item {item['id']}")
                results.append({"id": item["id"], "status": "success"})
            return results

        # Create a batch of items, some will fail
        batch = [
            {"id": 1, "should_fail": False},
            {"id": 2, "should_fail": True},  # Will fail
            {"id": 3, "should_fail": False},
        ]

        # Process each item individually with recovery
        for item in batch:
            try:
                result = strategy.recover_or_raise(
                    process_batch, [item], ValueError, "items"
                )
                processed_results.extend(result)
            except ValueError:
                # We shouldn't get here with skip_and_log
                assert False, "Skip and log strategy should not raise exceptions"

        # Check results - should have 2 successful items
        assert len(processed_results) == 2
        assert processed_results[0]["id"] == 1
        assert processed_results[1]["id"] == 3

    def test_partial_recovery(self, partial_processing_recovery_class):
        """Test partial processing recovery strategy."""
        strategy = partial_processing_recovery_class()

        # Function that raises different types of errors
        def complex_process(input_type, value=None):
            if input_type == "value_error":
                raise ValueError("Invalid value")
            elif input_type == "key_error":
                # This is a partial failure that can be recovered from
                data = {"a": 1}
                # This will raise a KeyError
                return {"result": data["missing_key"]}
            elif input_type == "index_error":
                # Another recoverable error
                data = [1, 2]
                # This will raise an IndexError
                return {"result": data[10]}
            elif input_type == "custom":
                # Return a partial result with an error indication
                return {"result": value, "error": "Partial failure"}
            else:
                return {"result": "success"}

        # Test with different error types

        # Value error should be handled by PartialProcessingRecovery
        result = strategy.recover_or_raise(
            complex_process, "value_error", ValueError, "entity"
        )
        assert result is not None
        assert isinstance(result, dict)
        assert "_partial_error" in str(result) or "error" in result

        # KeyError should be recovered with a partial result
        result = strategy.recover_or_raise(
            complex_process, "key_error", KeyError, "entity"
        )
        assert result is not None
        assert "_partial_error" in str(result) or "error" in result

        # IndexError should be recovered with a partial result
        result = strategy.recover_or_raise(
            complex_process, "index_error", IndexError, "entity"
        )
        assert result is not None
        assert "_partial_error" in str(result) or "error" in result

        # Custom partial result should be returned as is
        result = strategy.recover_or_raise(
            complex_process, "custom", Exception, "entity", value="test"
        )
        assert result["result"] == "test"
        assert "error" in result

    def test_with_recovery_decorator(
        self, with_recovery_decorator, skip_and_log_recovery_class
    ):
        """Test the with_recovery decorator."""

        # Create a function to decorate
        @with_recovery_decorator(strategy=skip_and_log_recovery_class())
        def process_data(data):
            if isinstance(data, list):
                results = []
                for item in data:
                    if item.get("should_fail"):
                        raise ValueError(f"Failed on item {item['id']}")
                    results.append({"id": item["id"], "status": "success"})
                return results
            else:
                if data.get("should_fail"):
                    raise ValueError("Failed processing")
                return {"id": data["id"], "status": "success"}

        # Test with a single successful item
        result = process_data({"id": 1, "should_fail": False})
        assert result["id"] == 1
        assert result["status"] == "success"

        # Test with a single failing item - should return a fallback value (empty list or None)
        result = process_data({"id": 2, "should_fail": True})
        # The implementation can return either None or an empty data structure
        assert result is None or result == [] or result == {}, (
            f"Expected None or empty structure but got {result}"
        )

        # Test with a batch of items
        batch = [
            {"id": 1, "should_fail": False},
            {"id": 2, "should_fail": True},  # Will fail
            {"id": 3, "should_fail": False},
        ]

        # Should process the items that don't fail
        results = process_data(batch)
        # The returned value can be None, an empty list, or a list with the successful items
        if results is not None and len(results) > 0:
            # Check we have successful results
            assert any(item["id"] == 1 for item in results)
            assert any(item["id"] == 3 for item in results)
            # Failing item should not be in results
            assert not any(item["id"] == 2 for item in results)

    def test_function_wrapper_usage(
        self, with_recovery_decorator, skip_and_log_recovery_class
    ):
        """Test using with_recovery as a function wrapper."""

        # Define a function
        def process_data(data):
            if data.get("should_fail"):
                raise ValueError("Failed processing")
            return {"id": data["id"], "status": "success"}

        # Create wrapped version
        wrapped_process = with_recovery_decorator(
            strategy=skip_and_log_recovery_class()
        )(process_data)

        # Test with successful data
        result = wrapped_process({"id": 1, "should_fail": False})
        assert result["id"] == 1
        assert result["status"] == "success"

        # Test with failing data - should return None instead of raising
        result = wrapped_process({"id": 2, "should_fail": True})
        # The implementation can return either None or an empty data structure
        assert result is None or result == [] or result == {}, (
            f"Expected None or empty structure but got {result}"
        )

    def test_try_with_recovery(
        self, try_with_recovery_func, strict_recovery_class, skip_and_log_recovery_class
    ):
        """Test the try_with_recovery function."""

        # Create test function
        def test_func(x):
            if x == 0:
                raise ValueError("Cannot divide by zero")
            return 10 / x

        # Create recovery function
        def recovery_func(e):
            return float("inf")  # Return infinity for division by zero

        # Test with recovery and no error
        result = try_with_recovery_func(test_func, recovery_func, func_args=(5,))
        assert result == 2.0

        # Test with recovery and recoverable error
        result = try_with_recovery_func(test_func, recovery_func, func_args=(0,))
        assert result == float("inf")

        # Test with recovery strategy
        strategy = skip_and_log_recovery_class()
        result = try_with_recovery_func(
            test_func, recovery_strategy=strategy, func_args=(0,)
        )
        # Result could be None or an empty container, depending on the implementation
        assert result is None or result == [] or result == {}
