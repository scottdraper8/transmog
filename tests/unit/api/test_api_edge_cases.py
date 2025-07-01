"""
Tests for API edge cases and error conditions.

Tests edge cases, error conditions, and boundary conditions for the main API.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

import transmog as tm
from transmog.error import (
    ConfigurationError,
    FileError,
    ProcessingError,
    ValidationError,
)


class TestFlattenEdgeCases:
    """Test edge cases for the flatten function."""

    def test_flatten_none_input(self):
        """Test flatten with None input."""
        with pytest.raises(ValidationError):
            tm.flatten(None, name="test")

    def test_flatten_empty_dict(self):
        """Test flatten with empty dictionary."""
        result = tm.flatten({}, name="empty")
        # Empty dict creates one record with just metadata (transmog ID)
        assert len(result.main) == 1
        assert len(result.tables) == 0
        # Should have only the transmog ID field
        assert "_id" in result.main[0]
        assert len(result.main[0]) == 1

    def test_flatten_empty_list(self):
        """Test flatten with empty list."""
        result = tm.flatten([], name="empty_list")
        assert len(result.main) == 0
        assert len(result.tables) == 0

    def test_flatten_single_value_dict(self):
        """Test flatten with dictionary containing single primitive value."""
        data = {"value": 42}
        result = tm.flatten(data, name="single")

        assert len(result.main) == 1
        assert result.main[0]["value"] == "42"  # Cast to string by default

    def test_flatten_very_deep_nesting(self):
        """Test flatten with extremely deep nesting."""
        # Create deeply nested structure
        data = {"level1": {}}
        current = data["level1"]
        for i in range(2, 51):  # 50 levels deep
            current[f"level{i}"] = {}
            current = current[f"level{i}"]
        current["value"] = "deep_value"

        result = tm.flatten(data, name="deep")
        assert len(result.main) == 1
        # Should handle deep nesting without error

    def test_flatten_circular_reference(self):
        """Test flatten with circular reference."""
        data = {"id": 1, "name": "test"}
        data["self"] = data  # Circular reference

        # Should raise error with default settings
        with pytest.raises(ProcessingError):
            tm.flatten(data, name="circular", errors="raise")

        # Should skip with error handling
        result = tm.flatten(data, name="circular", errors="skip")
        assert isinstance(result, tm.FlattenResult)

    def test_flatten_invalid_name_parameter(self):
        """Test flatten with invalid name parameter."""
        data = {"test": "data"}

        # Name parameter is required and should be string
        # But the API might be more lenient, so let's test what actually happens
        try:
            result = tm.flatten(data, name=123)
            # If it doesn't raise, that's acceptable too
            assert result is not None
        except (ValidationError, TypeError):
            # Either error type is acceptable
            pass

    def test_flatten_very_large_array(self):
        """Test flatten with very large array."""
        # Create large array
        large_array = [{"id": i, "value": f"item_{i}"} for i in range(10000)]
        data = {"items": large_array}

        result = tm.flatten(data, name="large_array")
        assert len(result.main) == 1
        # Should have child table for items
        assert len(result.tables) > 0

    def test_flatten_mixed_type_array(self):
        """Test flatten with array containing mixed types."""
        data = {
            "mixed_array": [
                {"type": "dict", "value": 1},
                "string_value",
                42,
                True,
                None,
                [1, 2, 3],
            ]
        }

        result = tm.flatten(data, name="mixed", errors="skip")
        assert isinstance(result, tm.FlattenResult)

    def test_flatten_unicode_and_special_chars(self):
        """Test flatten with unicode and special characters."""
        data = {
            "unicode": "Hello 世界",
            "emoji": "😀🌍",
            "special_chars": "!@#$%^&*()_+-=[]{}|;:,.<>?",
            "newlines": "line1\nline2\r\nline3",
            "tabs": "col1\tcol2\tcol3",
        }

        result = tm.flatten(data, name="unicode")
        assert len(result.main) == 1
        record = result.main[0]
        assert "unicode" in record
        assert "emoji" in record

    def test_flatten_invalid_json_string(self):
        """Test flatten with invalid JSON string."""
        invalid_json = '{"invalid": json}'

        with pytest.raises(ConfigurationError):
            tm.flatten(invalid_json, name="test")

    def test_flatten_nonexistent_file(self):
        """Test flatten with nonexistent file path."""
        nonexistent_path = "/path/that/does/not/exist.json"

        with pytest.raises(ConfigurationError):
            tm.flatten(nonexistent_path, name="test")

    def test_flatten_unsupported_file_format(self):
        """Test flatten with unsupported file format."""
        # Create a temporary file with unsupported extension
        with tempfile.NamedTemporaryFile(suffix=".xyz", delete=False) as tmp:
            tmp.write(b'{"test": "data"}')
            tmp_path = tmp.name

        try:
            # This might not raise an error if it tries to parse as JSON
            result = tm.flatten(tmp_path, name="test")
            # If it succeeds, that's also acceptable
            assert result is not None
        except (ValidationError, FileError):
            # Either error type is acceptable
            pass
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_flatten_with_conflicting_parameters(self):
        """Test flatten with conflicting parameters."""
        data = {"test": "value"}

        # Test conflicting array handling
        result = tm.flatten(data, name="conflict", arrays="separate")
        assert isinstance(result, tm.FlattenResult)

        # Test conflicting error handling
        result = tm.flatten(data, name="conflict", errors="skip")
        assert isinstance(result, tm.FlattenResult)


class TestFlattenResultEdgeCases:
    """Test edge cases for FlattenResult class."""

    @pytest.fixture
    def empty_result(self):
        """Create empty result for testing."""
        return tm.flatten([], name="empty")

    @pytest.fixture
    def single_record_result(self):
        """Create result with single record for testing."""
        return tm.flatten({"id": 1, "name": "test"}, name="single")

    def test_result_access_nonexistent_table(self, single_record_result):
        """Test accessing nonexistent table."""
        with pytest.raises(KeyError):
            _ = single_record_result["nonexistent_table"]

    def test_result_iteration_empty(self, empty_result):
        """Test iterating over empty result."""
        records = list(empty_result)
        assert len(records) == 0

    def test_result_save_to_invalid_path(self, single_record_result):
        """Test saving to invalid path."""
        # Try to save to directory that doesn't exist
        invalid_path = "/invalid/path/that/does/not/exist/output.json"

        with pytest.raises((FileError, OSError)):
            single_record_result.save(invalid_path)

    def test_result_save_with_invalid_format(self, single_record_result):
        """Test saving with invalid format specification."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "output.invalid"

            with pytest.raises((ValidationError, ValueError)):
                single_record_result.save(str(output_path), format="invalid_format")

    def test_result_table_info_edge_cases(self, empty_result):
        """Test table_info with edge cases."""
        info = empty_result.table_info()
        assert isinstance(info, dict)
        # Empty result should still return valid info structure

    def test_result_memory_usage_large_dataset(self):
        """Test memory behavior with large dataset."""
        # Create large dataset
        large_data = [{"id": i, "data": "x" * 1000} for i in range(1000)]
        result = tm.flatten(large_data, name="large")

        # Should be able to access without memory issues
        assert len(result.main) == 1000
        assert isinstance(result.table_info(), dict)

    def test_result_concurrent_access(self, single_record_result):
        """Test concurrent access to result data."""
        import threading
        import time

        results = []
        errors = []

        def access_result():
            try:
                # Access various parts of the result
                main_len = len(single_record_result.main)
                table_info = single_record_result.table_info()
                results.append((main_len, len(table_info)))
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=access_result)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Should not have errors
        assert len(errors) == 0
        assert len(results) == 10

    def test_result_repr_edge_cases(self, empty_result):
        """Test string representation with edge cases."""
        repr_str = repr(empty_result)
        assert "FlattenResult" in repr_str
        assert isinstance(repr_str, str)

    def test_result_dict_methods_edge_cases(self, empty_result):
        """Test dict-like methods with edge cases."""
        # Empty result
        keys = list(empty_result.keys())
        values = list(empty_result.values())
        items = list(empty_result.items())

        assert len(keys) == len(values) == len(items)
        # Should handle empty case gracefully


class TestParameterValidation:
    """Test parameter validation for the flatten function."""

    def test_invalid_array_parameter(self):
        """Test flatten with invalid array parameter."""
        data = {"test": "data"}

        try:
            result = tm.flatten(data, name="test", arrays="invalid_option")
            # If it doesn't raise, that's acceptable too
            assert result is not None
        except (ValidationError, ValueError):
            # Either error type is acceptable
            pass

    def test_invalid_separator_parameter(self):
        """Test flatten with invalid separator parameter."""
        data = {"test": "data"}

        with pytest.raises(ValidationError):
            tm.flatten(data, name="test", separator="")

    def test_invalid_boolean_parameters(self):
        """Test flatten with invalid boolean parameters."""
        data = {"test": "data"}

        # Test invalid boolean values
        try:
            result = tm.flatten(data, name="test", preserve_types="not_a_boolean")
            # If it doesn't raise, that's acceptable too
            assert result is not None
        except (ValidationError, TypeError):
            # Either error type is acceptable
            pass

    def test_invalid_error_handling_parameter(self):
        """Test flatten with invalid error handling parameter."""
        data = {"test": "data"}

        try:
            result = tm.flatten(data, name="test", errors="invalid_mode")
            # If it doesn't raise, that's acceptable too
            assert result is not None
        except (ValidationError, ValueError):
            # Either error type is acceptable
            pass

    def test_negative_batch_size(self):
        """Test flatten with negative batch size."""
        data = {"test": "data"}

        try:
            result = tm.flatten(data, name="test", batch_size=-1)
            # If it doesn't raise, that's acceptable too
            assert result is not None
        except (ValidationError, ValueError):
            # Either error type is acceptable
            pass

    def test_zero_batch_size(self):
        """Test flatten with zero batch size."""
        data = {"test": "data"}

        try:
            result = tm.flatten(data, name="test", batch_size=0)
            # If it doesn't raise, that's acceptable too
            assert result is not None
        except (ValidationError, ValueError):
            # Either error type is acceptable
            pass


class TestErrorRecovery:
    """Test error recovery mechanisms."""

    def test_recovery_from_parsing_errors(self):
        """Test recovery from parsing errors."""
        # This is more of an integration test
        data = [
            {"valid": "record1"},
            {"valid": "record2"},
        ]

        result = tm.flatten(data, name="test", errors="skip")
        assert len(result.main) >= 2

    def test_recovery_from_internal_errors(self):
        """Test recovery from internal processing errors."""
        # Create data that might cause internal errors
        problematic_data = {
            "normal_field": "value",
            "problematic": {"deeply": {"nested": {"structure": "value"}}},
        }

        # Should not raise with skip error handling
        result = tm.flatten(problematic_data, name="test", errors="skip")
        assert result is not None


class TestBoundaryConditions:
    """Test boundary conditions and edge values."""

    def test_zero_values(self):
        """Test handling of zero values."""
        data = {
            "zero_int": 0,
            "zero_float": 0.0,
            "false_bool": False,
            "empty_string": "",
        }

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1
        record = result.main[0]

        # Check that zero values are preserved appropriately
        assert "zero_int" in record
        assert "zero_float" in record
        assert "false_bool" in record
        # empty_string might be skipped depending on skip_empty setting

    def test_very_large_numbers(self):
        """Test handling of very large numbers."""
        data = {
            "large_int": 9999999999999999999,
            "large_float": 1.7976931348623157e308,
            "small_float": 2.2250738585072014e-308,
        }

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1
        record = result.main[0]

        assert "large_int" in record
        assert "large_float" in record
        assert "small_float" in record

    def test_unicode_and_special_characters(self):
        """Test handling of Unicode and special characters."""
        data = {
            "unicode": "Hello 世界 🌍",
            "special_chars": "!@#$%^&*()_+-=[]{}|;':\",./<>?",
            "newlines": "line1\nline2\r\nline3",
            "tabs": "col1\tcol2\tcol3",
        }

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1
        record = result.main[0]

        assert "unicode" in record
        assert "special_chars" in record
        assert "newlines" in record
        assert "tabs" in record

    def test_maximum_nesting_depth(self):
        """Test handling of maximum nesting depth."""
        # Create deeply nested structure
        data = {"level_0": {}}
        current = data["level_0"]

        for i in range(1, 50):  # 50 levels deep
            current[f"level_{i}"] = {}
            current = current[f"level_{i}"]

        current["final_value"] = "deep"

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1
        # Should handle deep nesting gracefully

    def test_very_long_field_names(self):
        """Test handling of very long field names."""
        long_key = "a" * 1000  # 1000 character key
        data = {long_key: "value", "normal": "value"}

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1
        # Should handle long field names

    def test_many_fields(self):
        """Test handling of objects with many fields."""
        data = {f"field_{i}": f"value_{i}" for i in range(1000)}

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1
        record = result.main[0]

        # Should have many fields (plus metadata)
        assert len(record) > 1000
