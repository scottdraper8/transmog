"""
Error handling interface tests.

This module defines abstract test classes for error handling components
that all implementations must satisfy.
"""

import pytest


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
    def parsing_error_class(self):
        """
        Fixture to provide the parsing error class.

        Implementations must override this to provide the actual parsing error class.
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
        """Test safe_json_loads with valid JSON."""
        valid_json = '{"key": "value", "nested": {"num": 42}}'
        result = safe_json_loads_func(valid_json)
        assert result["key"] == "value"
        assert result["nested"]["num"] == 42

    def test_safe_json_loads_invalid(self, safe_json_loads_func, parsing_error_class):
        """Test safe_json_loads with invalid JSON."""
        invalid_json = '{"broken": "json"'  # Missing closing brace
        with pytest.raises(parsing_error_class):
            safe_json_loads_func(invalid_json)

    def test_error_context_decorator(
        self, error_context_decorator, processing_error_class
    ):
        """Test the error_context decorator."""

        # Define a function with the decorator
        @error_context_decorator("Failed during test operation")
        def test_func(succeed=True):
            if not succeed:
                raise ValueError("Internal error")
            return "success"

        # Test successful execution
        assert test_func(succeed=True) == "success"

        # Test failure - should wrap the original exception
        with pytest.raises(processing_error_class) as excinfo:
            test_func(succeed=False)

        # Check error message
        error_msg = str(excinfo.value)
        assert "Failed during test operation" in error_msg
        assert "Internal error" in error_msg

        # Test with custom wrapper
        @error_context_decorator(
            "Custom wrapper test", wrap_as=lambda e: processing_error_class(str(e))
        )
        def wrapper_func(succeed=True):
            if not succeed:
                raise ValueError("Custom internal error")
            return "custom success"

        # Test failure with custom wrapper
        with pytest.raises(processing_error_class) as excinfo:
            wrapper_func(succeed=False)

        # Check custom wrapped error
        error_msg = str(excinfo.value)
        assert "Custom internal error" in error_msg


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

        def test_func(data):
            if data["should_fail"]:
                raise ValueError("Test error")
            return {"result": "success"}

        # Test should pass with strategy instance but not be used
        assert test_func({"should_fail": False})["result"] == "success"

        # Strategy should re-raise exceptions
        with pytest.raises(ValueError):
            strategy.recover(ValueError("Test error"), None)

    def test_skip_and_log_recovery(self, skip_and_log_recovery_class):
        """Test skip and log recovery strategy."""
        strategy = skip_and_log_recovery_class()

        # Create a function that processes a batch and can fail on some items
        def process_batch(items):
            results = []
            for item in items:
                if item.get("should_fail"):
                    raise ValueError(f"Failed on item {item['id']}")
                results.append({"id": item["id"], "status": "success"})
            return results

        # Test regular function behavior
        assert len(process_batch([{"id": 1, "should_fail": False}])) == 1

        # Test recovery behavior - should return something (empty dict or empty list)
        result = strategy.recover(ValueError("Test error"), None)
        assert result is None or isinstance(result, (dict, list))

        # If batch level errors, should return a reasonable default
        batch_error = ValueError("Batch processing failed")
        batch_result = strategy.recover(batch_error, context={"batch_size": 5})

        # Result should be some kind of empty container or None
        assert batch_result is None or batch_result == [] or batch_result == {}

    def test_partial_recovery(self, partial_processing_recovery_class):
        """Test partial processing recovery strategy."""
        strategy = partial_processing_recovery_class()

        # Function that raises different types of errors
        def complex_process(input_type, value=None):
            if input_type == "value_error":
                raise ValueError("Invalid value")
            elif input_type == "key_error":
                data = {"a": 1}
                return {"result": data["missing_key"]}  # Raises KeyError
            elif input_type == "index_error":
                data = [1, 2]
                return {"result": data[10]}  # Raises IndexError
            elif input_type == "custom":
                return {"result": value, "error": "Partial failure"}
            else:
                return {"result": "success"}

        # Test normal execution
        assert complex_process("normal")["result"] == "success"

        # Test recovery for different error types
        for error_type, error_class in [
            ("value_error", ValueError),
            ("key_error", KeyError),
            ("index_error", IndexError),
        ]:
            try:
                complex_process(error_type)
                pytest.fail(f"Expected {error_class.__name__} but none was raised")
            except Exception as e:
                # Test recovery behavior
                result = strategy.recover(e, {"error_type": error_type})
                assert isinstance(result, dict), f"Expected dict for {error_type}"
                # Result might contain error info
                if "error" in result:
                    assert result["error"], "Error field should be non-empty"

    def test_with_recovery_decorator(
        self, with_recovery_decorator, skip_and_log_recovery_class
    ):
        """Test the with_recovery decorator."""

        @with_recovery_decorator(strategy=skip_and_log_recovery_class())
        def process_data(data):
            if data.get("should_fail"):
                raise ValueError("Data processing failed")
            return {"result": data["value"]}

        # Test with non-failing data
        result = process_data({"value": "test", "should_fail": False})
        assert result["result"] == "test"

        # Test with failing data - should be recovered
        result = process_data({"value": "test", "should_fail": True})
        # The result after recovery could be None, empty dict, or something else
        assert result is None or isinstance(result, dict)

    def test_function_wrapper_usage(
        self, with_recovery_decorator, skip_and_log_recovery_class
    ):
        """Test using with_recovery as a function wrapper."""

        def process_data(data):
            if data.get("should_fail"):
                raise ValueError("Data processing failed")
            return {"result": data["value"]}

        # Create wrapped function
        wrapped_func = with_recovery_decorator(strategy=skip_and_log_recovery_class())(
            process_data
        )

        # Test with non-failing data
        result = wrapped_func({"value": "test", "should_fail": False})
        assert isinstance(result, dict)
        assert result["result"] == "test"

        # Test with failing data - should be recovered
        result = wrapped_func({"value": "test", "should_fail": True})
        # The result after recovery could be None, empty dict, or something else
        assert result is None or isinstance(result, dict)

    def test_try_with_recovery(
        self, try_with_recovery_func, strict_recovery_class, skip_and_log_recovery_class
    ):
        """Test the try_with_recovery function."""

        def test_func(x):
            if x == 0:
                raise ValueError("Cannot divide by zero")
            return 10 / x

        def recovery_func(e):
            return "recovered result"

        # Test with strict recovery (should re-raise)
        strict_strategy = strict_recovery_class()
        with pytest.raises(ValueError):
            try_with_recovery_func(test_func, recovery_strategy=strict_strategy, x=0)

        # Test with skip and log recovery
        skip_strategy = skip_and_log_recovery_class()
        result = try_with_recovery_func(test_func, recovery_strategy=skip_strategy, x=0)
        # Result should be empty container or None
        assert result is None or result == {} or result == []

        # Test with recovery function
        result = try_with_recovery_func(test_func, recovery_func=recovery_func, x=0)
        assert result == "recovered result"
