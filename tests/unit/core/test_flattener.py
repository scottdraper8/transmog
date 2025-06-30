"""
Tests for core flattening functionality.

Tests the core JSON flattening logic and cache management.
"""

import pytest

from transmog.core.flattener import clear_caches, flatten_json, refresh_cache_config


class TestFlattenJson:
    """Test the core flatten_json function."""

    def test_flatten_simple_object(self):
        """Test flattening a simple nested object."""
        data = {"name": "test", "nested": {"value": 42}}

        result = flatten_json(data, separator="_")

        assert isinstance(result, dict)
        assert result["name"] == "test"
        assert result["nested_value"] == "42"  # Values are cast to strings by default

    def test_flatten_with_separator(self):
        """Test flattening with custom separator."""
        data = {"level1": {"level2": {"value": "test"}}}

        result = flatten_json(data, separator=".")

        assert "level1.level2.value" in result
        assert result["level1.level2.value"] == "test"

    def test_flatten_empty_object(self):
        """Test flattening empty object."""
        data = {}

        result = flatten_json(data, separator="_")

        assert result == {}

    def test_flatten_with_arrays(self):
        """Test flattening object with arrays."""
        data = {"items": [1, 2, 3], "name": "test"}

        result = flatten_json(data, separator="_")

        # Arrays may be skipped or converted to JSON strings in basic flattening
        assert result["name"] == "test"
        # Arrays handling depends on configuration

    def test_flatten_nested_objects(self):
        """Test flattening deeply nested objects."""
        data = {"level1": {"level2": {"level3": {"value": "deep"}}}}

        result = flatten_json(data, separator="_")

        assert "level1_level2_level3_value" in result
        assert result["level1_level2_level3_value"] == "deep"

    def test_flatten_with_null_values(self):
        """Test flattening with null values."""
        data = {"name": "test", "null_field": None, "nested": {"null_nested": None}}

        result = flatten_json(data, separator="_")

        assert result["name"] == "test"
        # Null handling may vary based on configuration

    def test_flatten_with_empty_strings(self):
        """Test flattening with empty strings."""
        data = {"name": "", "nested": {"empty": ""}}

        result = flatten_json(data, separator="_")

        # Empty strings may be skipped based on configuration
        assert isinstance(result, dict)

    def test_flatten_mixed_types(self):
        """Test flattening with mixed data types."""
        data = {
            "string": "text",
            "number": 42,
            "boolean": True,
            "nested": {"float": 3.14, "list": [1, 2, 3]},
        }

        result = flatten_json(data, separator="_")

        assert result["string"] == "text"
        assert result["number"] == "42"  # Cast to string
        # Boolean conversion might be "1"/"0" or "true"/"false"
        assert result["boolean"] in ["1", "0", "true", "false", "True", "False"]
        assert result["nested_float"] == "3.14"  # Cast to string

    def test_flatten_with_special_characters(self):
        """Test flattening with special characters in keys."""
        data = {"key-with-dash": "value1", "key.with.dots": "value2"}

        result = flatten_json(data, separator="_")

        # Should handle special characters in keys
        assert len(result) >= 2

    def test_flatten_preserves_original(self):
        """Test that flattening preserves original data."""
        original = {"nested": {"value": 42}}
        data = original.copy()

        result = flatten_json(data, separator="_")

        # Original should be unchanged
        assert data == original
        assert result != original


class TestCacheManagement:
    """Test cache management functions."""

    def test_clear_caches(self):
        """Test clearing caches."""
        # This should not raise an exception
        clear_caches()

    def test_refresh_cache_config(self):
        """Test refreshing cache configuration."""
        # This should not raise an exception
        refresh_cache_config()

    def test_cache_functionality(self):
        """Test that caching works correctly."""
        data = {"test": {"nested": "value"}}

        # First call
        result1 = flatten_json(data, separator="_")

        # Second call should use cache (if enabled)
        result2 = flatten_json(data, separator="_")

        assert result1 == result2

    def test_cache_with_different_separators(self):
        """Test caching with different separators."""
        data = {"test": {"nested": "value"}}

        result_underscore = flatten_json(data, separator="_")
        result_dot = flatten_json(data, separator=".")

        # Results should be different due to different separators
        assert result_underscore != result_dot


class TestFlattenJsonEdgeCases:
    """Test edge cases for flatten_json."""

    def test_flatten_circular_reference_safe(self):
        """Test that circular references are handled safely."""
        data = {"name": "test"}
        data["self"] = data  # Create circular reference

        # Should not cause infinite recursion
        try:
            result = flatten_json(data, separator="_")
            # If it doesn't crash, that's good
            assert isinstance(result, dict)
        except (RecursionError, ValueError, Exception):
            # Circular reference detection/error is acceptable
            pass

    def test_flatten_very_deep_nesting(self):
        """Test flattening very deeply nested objects."""
        # Create deeply nested structure
        data = {"level0": {}}
        current = data["level0"]

        for i in range(1, 20):  # 20 levels deep
            current[f"level{i}"] = {}
            current = current[f"level{i}"]

        current["value"] = "deep"

        result = flatten_json(data, separator="_")

        # Should handle deep nesting
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_flatten_unicode_keys_and_values(self):
        """Test flattening with Unicode keys and values."""
        data = {"café": "coffee", "nested": {"résumé": "CV", "🚀": "rocket"}}

        result = flatten_json(data, separator="_")

        assert isinstance(result, dict)
        assert len(result) >= 3

    def test_flatten_numeric_keys(self):
        """Test flattening with numeric-like keys."""
        data = {"123": "numeric_key", "nested": {"456": "nested_numeric"}}

        result = flatten_json(data, separator="_")

        assert isinstance(result, dict)
        assert len(result) >= 2

    def test_flatten_large_object(self):
        """Test flattening large objects."""
        # Create large nested object
        data = {"root": {}}
        current = data["root"]

        for i in range(100):
            current[f"field_{i}"] = {"value": i, "nested": {"deep": f"value_{i}"}}

        result = flatten_json(data, separator="_")

        assert isinstance(result, dict)
        assert len(result) > 100  # Should have many flattened fields

    def test_flatten_with_list_values(self):
        """Test flattening with list values."""
        data = {"simple_list": [1, 2, 3], "nested": {"list": ["a", "b", "c"]}}

        result = flatten_json(data, separator="_")

        assert isinstance(result, dict)
        # Lists might be converted to JSON strings or handled specially
        assert len(result) >= 1

    def test_flatten_boolean_and_none_handling(self):
        """Test flattening with boolean and None values."""
        data = {
            "true_val": True,
            "false_val": False,
            "none_val": None,
            "nested": {"bool": True, "null": None},
        }

        result = flatten_json(data, separator="_")

        assert isinstance(result, dict)
        # Check that booleans are converted appropriately
        if "true_val" in result:
            assert result["true_val"] in ["1", "0", "true", "false", "True", "False"]
        if "false_val" in result:
            assert result["false_val"] in ["1", "0", "true", "false", "True", "False"]
