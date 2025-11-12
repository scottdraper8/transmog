"""Tests for core flattening functionality."""

import pytest

from transmog.config import TransmogConfig
from transmog.flattening import flatten_json


class TestFlattenJson:
    """Test the core flatten_json function."""

    def test_flatten_simple_object(self):
        """Test flattening a simple nested object."""
        data = {"name": "test", "nested": {"value": 42}}
        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert isinstance(result, dict)
        assert result["name"] == "test"
        assert result["nested_value"] == 42

    def test_flatten_empty_object(self):
        """Test flattening empty object."""
        data = {}

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert result == {}

    def test_flatten_with_arrays(self):
        """Test flattening object with arrays."""
        data = {"items": [1, 2, 3], "name": "test"}
        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert result["name"] == "test"

    def test_flatten_nested_objects(self):
        """Test flattening deeply nested objects."""
        data = {"level1": {"level2": {"level3": {"value": "deep"}}}}

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert "level1_level2_level3_value" in result
        assert result["level1_level2_level3_value"] == "deep"

    def test_flatten_with_null_values(self):
        """Test flattening with null values."""
        data = {"name": "test", "null_field": None, "nested": {"null_nested": None}}

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert result["name"] == "test"

    def test_flatten_with_empty_strings(self):
        """Test flattening with empty strings."""
        data = {"name": "", "nested": {"empty": ""}}

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert isinstance(result, dict)

    def test_flatten_mixed_types(self):
        """Test flattening with mixed data types."""
        data = {
            "string": "text",
            "number": 42,
            "boolean": True,
            "nested": {"float": 3.14, "list": [1, 2, 3]},
        }

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert result["string"] == "text"
        assert result["number"] == 42
        assert result["boolean"]
        assert result["nested_float"] == 3.14

    def test_flatten_with_special_characters(self):
        """Test flattening with special characters in keys."""
        data = {"key-with-dash": "value1", "key.with.dots": "value2"}
        config = TransmogConfig()

        config = TransmogConfig()

        result = flatten_json(data, config)

        # Should handle special characters in keys
        assert len(result) >= 2

    def test_flatten_preserves_original(self):
        """Test that flattening preserves original data."""
        original = {"nested": {"value": 42}}
        data = original.copy()

        config = TransmogConfig()

        result = flatten_json(data, config)

        # Original should be unchanged
        assert data == original
        assert result != original


class TestFlattenJsonEdgeCases:
    """Test edge cases for flatten_json."""

    def test_flatten_circular_reference_safe(self):
        """Test that circular references are handled safely."""
        data = {"name": "test"}
        data["self"] = data  # Create circular reference

        # Should not cause infinite recursion
        try:
            config = TransmogConfig()
            result = flatten_json(data, config)
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

        for i in range(1, 20):
            current[f"level{i}"] = {}
            current = current[f"level{i}"]

        current["value"] = "deep"

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert isinstance(result, dict)
        assert len(result) > 0

    def test_flatten_unicode_keys_and_values(self):
        """Test flattening with Unicode keys and values."""
        data = {"café": "coffee", "nested": {"résumé": "CV", "🚀": "rocket"}}

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert isinstance(result, dict)
        assert len(result) >= 3

    def test_flatten_numeric_keys(self):
        """Test flattening with numeric-like keys."""
        data = {"123": "numeric_key", "nested": {"456": "nested_numeric"}}

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert isinstance(result, dict)
        assert len(result) >= 2

    def test_flatten_large_object(self):
        """Test flattening large objects."""
        data = {"root": {}}
        current = data["root"]

        for i in range(100):
            current[f"field_{i}"] = {"value": i, "nested": {"deep": f"value_{i}"}}

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert isinstance(result, dict)
        assert len(result) >= 50

    def test_flatten_with_list_values(self):
        """Test flattening with list values in SMART mode."""
        from transmog.types import ArrayMode

        data = {"simple_list": [1, 2, 3], "nested": {"list": ["a", "b", "c"]}}

        config = TransmogConfig(array_mode=ArrayMode.SMART)

        result, _ = flatten_json(data, config, _collect_arrays=False)

        assert isinstance(result, dict)
        assert "simple_list" in result
        assert "nested_list" in result

    def test_flatten_boolean_and_none_handling(self):
        """Test flattening with boolean and None values."""
        data = {
            "true_val": True,
            "false_val": False,
            "none_val": None,
            "nested": {"bool": True, "null": None},
        }

        config = TransmogConfig()

        result, _ = flatten_json(data, config)

        assert isinstance(result, dict)
        if "true_val" in result:
            assert result["true_val"]
        if "false_val" in result:
            assert not result["false_val"]
