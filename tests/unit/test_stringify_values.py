"""Tests for stringify_values configuration option."""

import math

import pytest

import transmog as tm
from transmog.config import TransmogConfig
from transmog.types import ArrayMode


class TestStringifyPrimitives:
    """Test stringification of primitive values."""

    def test_numbers_stringified(self):
        """Test that numbers are converted to strings."""
        data = {"int_val": 42, "float_val": 3.14}
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        assert result.main[0]["int_val"] == "42"
        assert result.main[0]["float_val"] == "3.14"
        assert isinstance(result.main[0]["int_val"], str)
        assert isinstance(result.main[0]["float_val"], str)

    def test_booleans_stringified(self):
        """Test that booleans are converted to strings."""
        data = {"active": True, "deleted": False}
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        assert result.main[0]["active"] == "True"
        assert result.main[0]["deleted"] == "False"

    def test_strings_unchanged(self):
        """Test that strings remain unchanged."""
        data = {"name": "Alice", "status": "active"}
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        assert result.main[0]["name"] == "Alice"
        assert result.main[0]["status"] == "active"

    def test_nulls_not_stringified(self):
        """Test that null values remain as None, not 'None'."""
        data = {"value": None, "empty": ""}
        config = TransmogConfig(stringify_values=True, include_nulls=True)
        result = tm.flatten(data, config=config)

        # None should stay None, not become "None"
        assert result.main[0]["value"] is None
        # Empty string is null-like
        assert result.main[0]["empty"] is None


class TestStringifyNestedStructures:
    """Test stringification with nested objects."""

    def test_nested_objects_flattened_then_stringified(self):
        """Test that nested objects are flattened, then values stringified."""
        data = {"user": {"id": 123, "age": 30, "active": True}}
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        # Object is flattened first
        assert "user_id" in result.main[0]
        assert "user_age" in result.main[0]
        assert "user_active" in result.main[0]

        # Then values are stringified
        assert result.main[0]["user_id"] == "123"
        assert result.main[0]["user_age"] == "30"
        assert result.main[0]["user_active"] == "True"

    def test_deeply_nested_stringified(self):
        """Test stringification works at any depth."""
        data = {"level1": {"level2": {"level3": {"value": 999}}}}
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        assert result.main[0]["level1_level2_level3_value"] == "999"


class TestStringifyWithArrayModes:
    """Test stringification interaction with array modes."""

    def test_stringify_with_smart_mode_simple_arrays(self):
        """Test stringify with SMART mode and simple arrays."""
        data = {"tags": ["sale", "new"], "scores": [10, 20, 30]}
        config = TransmogConfig(stringify_values=True, array_mode=ArrayMode.SMART)
        result = tm.flatten(data, config=config)

        # Simple arrays preserved as arrays
        assert isinstance(result.main[0]["tags"], list)
        assert isinstance(result.main[0]["scores"], list)

        # String items unchanged
        assert result.main[0]["tags"] == ["sale", "new"]

        # Numeric items stringified
        assert result.main[0]["scores"] == ["10", "20", "30"]

    def test_stringify_with_smart_mode_complex_arrays(self):
        """Test stringify with SMART mode and complex arrays."""
        data = {
            "name": "Product",
            "reviews": [
                {"rating": 5, "helpful": True},
                {"rating": 4, "helpful": False},
            ],
        }
        config = TransmogConfig(stringify_values=True, array_mode=ArrayMode.SMART)
        result = tm.flatten(data, name="products", config=config)

        # Main table
        assert result.main[0]["name"] == "Product"

        # Child table values stringified
        assert result.tables["products_reviews"][0]["rating"] == "5"
        assert result.tables["products_reviews"][0]["helpful"] == "True"
        assert result.tables["products_reviews"][1]["rating"] == "4"
        assert result.tables["products_reviews"][1]["helpful"] == "False"

    def test_stringify_with_separate_mode(self):
        """Test stringify with SEPARATE mode."""
        data = {"items": [1, 2, 3]}
        config = TransmogConfig(stringify_values=True, array_mode=ArrayMode.SEPARATE)
        result = tm.flatten(data, name="data", config=config)

        # Array extracted to child table with stringified values
        assert result.tables["data_items"][0]["value"] == "1"
        assert result.tables["data_items"][1]["value"] == "2"
        assert result.tables["data_items"][2]["value"] == "3"

    def test_stringify_with_inline_mode(self):
        """Test stringify with INLINE mode."""
        data = {"tags": [1, 2, 3]}
        config = TransmogConfig(stringify_values=True, array_mode=ArrayMode.INLINE)
        result = tm.flatten(data, config=config)

        # Array already JSON-stringified by INLINE mode
        assert result.main[0]["tags"] == "[1, 2, 3]"

    def test_stringify_with_skip_mode(self):
        """Test stringify with SKIP mode."""
        data = {"value": 42, "tags": [1, 2, 3]}
        config = TransmogConfig(stringify_values=True, array_mode=ArrayMode.SKIP)
        result = tm.flatten(data, config=config)

        # Array skipped, value stringified
        assert "tags" not in result.main[0]
        assert result.main[0]["value"] == "42"


class TestStringifyWithOtherOptions:
    """Test stringify interaction with other config options."""

    def test_stringify_with_include_nulls(self):
        """Test stringify respects include_nulls setting."""
        data = {"value": 42, "null_field": None, "empty": ""}

        # Without include_nulls
        config = TransmogConfig(stringify_values=True, include_nulls=False)
        result = tm.flatten(data, config=config)
        assert "null_field" not in result.main[0]
        assert "empty" not in result.main[0]
        assert result.main[0]["value"] == "42"

        # With include_nulls
        config = TransmogConfig(stringify_values=True, include_nulls=True)
        result = tm.flatten(data, config=config)
        assert result.main[0]["null_field"] is None  # Not "None"
        assert result.main[0]["empty"] is None

    def test_stringify_disabled_by_default(self):
        """Test that stringify is disabled by default."""
        data = {"int_val": 42, "bool_val": True}
        config = TransmogConfig()  # Default: stringify_values=False
        result = tm.flatten(data, config=config)

        # Types preserved
        assert result.main[0]["int_val"] == 42
        assert result.main[0]["bool_val"] is True
        assert isinstance(result.main[0]["int_val"], int)
        assert isinstance(result.main[0]["bool_val"], bool)

    def test_stringify_with_metadata_fields(self):
        """Test that metadata fields (IDs, timestamps) remain strings."""
        data = {"value": 42}
        config = TransmogConfig(stringify_values=True, id_generation="random")
        result = tm.flatten(data, config=config)

        # Metadata already strings, should work fine
        assert isinstance(result.main[0]["_id"], str)
        assert isinstance(result.main[0]["_timestamp"], str)
        assert result.main[0]["value"] == "42"


class TestStringifyEdgeCases:
    """Test edge cases with stringify."""

    def test_stringify_with_special_floats(self):
        """Test stringify with NaN and Infinity."""
        data = {"nan_val": math.nan, "inf_val": math.inf, "normal": 42.0}
        config = TransmogConfig(stringify_values=True, include_nulls=True)
        result = tm.flatten(data, config=config)

        # NaN and Inf are null-like, so they become None (not stringified)
        assert result.main[0]["nan_val"] is None
        assert result.main[0]["inf_val"] is None
        assert result.main[0]["normal"] == "42.0"

    def test_stringify_mixed_array_types(self):
        """Test stringify with arrays containing mixed types."""
        data = {"mixed": [1, "text", True, 3.14]}
        config = TransmogConfig(stringify_values=True, array_mode=ArrayMode.SMART)
        result = tm.flatten(data, config=config)

        # All items stringified (strings unchanged)
        assert result.main[0]["mixed"] == ["1", "text", "True", "3.14"]

    def test_stringify_empty_data(self):
        """Test stringify with empty data."""
        data = {}
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        # Empty data produces no records (expected behavior)
        assert len(result.main) == 0

    def test_stringify_negative_numbers(self):
        """Test stringify with negative numbers."""
        data = {"negative_int": -42, "negative_float": -3.14}
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        assert result.main[0]["negative_int"] == "-42"
        assert result.main[0]["negative_float"] == "-3.14"

    def test_stringify_zero_values(self):
        """Test stringify with zero values."""
        data = {"zero_int": 0, "zero_float": 0.0}
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        assert result.main[0]["zero_int"] == "0"
        assert result.main[0]["zero_float"] == "0.0"

    def test_stringify_large_numbers(self):
        """Test stringify with large numbers."""
        data = {"large_int": 9999999999999999, "large_float": 1.7976931348623157e308}
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        assert result.main[0]["large_int"] == "9999999999999999"
        assert isinstance(result.main[0]["large_float"], str)


class TestStringifyMultipleRecords:
    """Test stringify with multiple records."""

    def test_stringify_batch_processing(self):
        """Test stringify works correctly with batch processing."""
        data = [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 300},
        ]
        config = TransmogConfig(stringify_values=True, batch_size=2)
        result = tm.flatten(data, config=config)

        assert len(result.main) == 3
        assert result.main[0]["id"] == "1"
        assert result.main[0]["value"] == "100"
        assert result.main[1]["id"] == "2"
        assert result.main[1]["value"] == "200"
        assert result.main[2]["id"] == "3"
        assert result.main[2]["value"] == "300"

    def test_stringify_with_varying_schemas(self):
        """Test stringify with records having different fields."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "age": 30},
            {"id": 3, "name": "Bob", "age": 25},
        ]
        config = TransmogConfig(stringify_values=True)
        result = tm.flatten(data, config=config)

        assert len(result.main) == 3
        assert result.main[0]["id"] == "1"
        assert result.main[0]["name"] == "Alice"
        assert result.main[1]["id"] == "2"
        assert result.main[1]["age"] == "30"
        assert result.main[2]["id"] == "3"
        assert result.main[2]["name"] == "Bob"
        assert result.main[2]["age"] == "25"
