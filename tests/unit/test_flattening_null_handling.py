"""
Tests for null and empty value handling.

Tests different null handling strategies (SKIP vs INCLUDE) and their impact on
flattened output.
"""

import pytest

import transmog as tm
from transmog.config import TransmogConfig


class TestNullHandlingSkip:
    """Test False  # include_nulls mode (default)."""

    def test_skip_null_values(self):
        """Test that null values are skipped by default."""
        data = {"id": 1, "name": "test", "value": None, "description": "valid"}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        assert "id" in main_record
        assert "name" in main_record
        assert "description" in main_record
        assert "value" not in main_record

    def test_skip_nested_null_values(self):
        """Test that nested null values are skipped."""
        data = {"id": 1, "nested": {"value": None, "valid": "data", "also_null": None}}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        assert "nested_valid" in main_record
        assert "nested_value" not in main_record
        assert "nested_also_null" not in main_record

    def test_skip_empty_strings(self):
        """Test that empty strings are skipped."""
        data = {"id": 1, "empty": "", "valid": "data"}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        assert "valid" in main_record
        assert "empty" not in main_record

    def test_skip_empty_dicts(self):
        """Test that empty dicts are skipped."""
        data = {"id": 1, "empty_dict": {}, "nested": {"value": "test"}}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        assert "nested_value" in main_record
        # Empty dict should not create any fields

    def test_skip_empty_lists(self):
        """Test that empty lists are skipped."""
        data = {"id": 1, "empty_list": [], "items": [1, 2, 3]}

        result = tm.flatten(data, name="test")

        main_record = result.main[0]
        assert "id" in main_record
        # Empty list should not create child tables


class TestNullHandlingInclude:
    """Test True  # include_nulls mode."""

    def test_include_null_values(self):
        """Test that null values are included when configured."""
        data = {"id": 1, "name": "test", "value": None, "description": "valid"}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        main_record = result.main[0]
        assert "id" in main_record
        assert "name" in main_record
        assert "description" in main_record
        assert "value" in main_record
        assert main_record["value"] is None or main_record["value"] == ""

    def test_include_nested_null_values(self):
        """Test that nested null values are included."""
        data = {"id": 1, "nested": {"value": None, "valid": "data"}}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        main_record = result.main[0]
        assert "nested_valid" in main_record
        assert "nested_value" in main_record

    def test_include_empty_strings(self):
        """Test that empty strings are included."""
        data = {"id": 1, "empty": "", "valid": "data"}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        main_record = result.main[0]
        assert "valid" in main_record
        assert "empty" in main_record
        # Empty strings are converted to None for consistent null representation
        assert main_record["empty"] is None

    def test_include_preserves_zero_values(self):
        """Test that zero values are preserved (not treated as null)."""
        data = {"id": 0, "count": 0, "value": 0.0, "flag": False}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        main_record = result.main[0]
        assert "id" in main_record
        assert main_record["id"] == 0 or main_record["id"] == "0"
        assert "count" in main_record
        assert "value" in main_record
        assert "flag" in main_record


class TestNullHandlingEdgeCases:
    """Test edge cases for null handling."""

    def test_all_null_record_skip(self):
        """Test record with all null values in SKIP mode."""
        data = {"field1": None, "field2": None, "field3": None}

        result = tm.flatten(data, name="test")

        # Record should exist with only metadata fields (no data fields)
        assert len(result.main) == 1
        main_record = result.main[0]
        # Should only have metadata fields, no data fields
        data_fields = [k for k in main_record.keys() if not k.startswith("_")]
        assert len(data_fields) == 0

    def test_all_null_record_include(self):
        """Test record with all null values in INCLUDE mode."""
        data = {"field1": None, "field2": None, "field3": None}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        # Record should exist with null fields included
        assert len(result.main) == 1
        main_record = result.main[0]
        # Should have the null fields
        assert "field1" in main_record
        assert "field2" in main_record
        assert "field3" in main_record
        assert main_record["field1"] is None
        assert main_record["field2"] is None
        assert main_record["field3"] is None

    def test_mixed_null_and_valid_data(self):
        """Test mix of null and valid data."""
        data = [
            {"id": 1, "name": "Alice", "age": None, "city": "NYC"},
            {"id": 2, "name": None, "age": 25, "city": "LA"},
            {"id": 3, "name": "Charlie", "age": 30, "city": None},
        ]

        # SKIP mode
        result_skip = tm.flatten(data, name="test")
        assert len(result_skip.main) == 3

        # INCLUDE mode
        config = TransmogConfig(include_nulls=True)
        result_include = tm.flatten(data, name="test", config=config)
        assert len(result_include.main) == 3

        # INCLUDE should have more fields
        skip_keys = set(result_skip.main[0].keys())
        include_keys = set(result_include.main[0].keys())
        # Include mode may have equal or more keys
        assert len(include_keys) >= len(skip_keys)

    def test_null_in_arrays(self):
        """Test null values within arrays."""
        data = {"id": 1, "items": [None, "valid", None, "also_valid", None]}

        # SKIP mode
        result_skip = tm.flatten(data, name="test")
        assert len(result_skip.main) >= 1

        # INCLUDE mode
        config = TransmogConfig(include_nulls=True)
        result_include = tm.flatten(data, name="test", config=config)
        assert len(result_include.main) >= 1

    def test_deeply_nested_nulls(self):
        """Test null values in deeply nested structures."""
        data = {
            "id": 1,
            "level1": {
                "value": None,
                "level2": {"value": None, "level3": {"value": None, "final": "value"}},
            },
        }

        # SKIP mode
        result_skip = tm.flatten(data, name="test")
        main_skip = result_skip.main[0]
        # Should have final value
        assert any("final" in key for key in main_skip.keys())

        # INCLUDE mode
        config = TransmogConfig(include_nulls=True)
        result_include = tm.flatten(data, name="test", config=config)
        main_include = result_include.main[0]
        # Should have all levels including null ones
        assert any("final" in key for key in main_include.keys())


class TestNullHandlingConsistency:
    """Test consistency of null handling across operations."""

    def test_consistent_behavior_across_batches(self):
        """Test null handling is consistent across batches."""
        batch1 = [{"id": 1, "value": None}, {"id": 2, "value": "test"}]

        batch2 = [{"id": 3, "value": None}, {"id": 4, "value": "data"}]

        config = TransmogConfig(include_nulls=False)
        result1 = tm.flatten(batch1, name="test", config=config)
        result2 = tm.flatten(batch2, name="test", config=config)

        # Field presence should be consistent
        record1_with_value = [r for r in result1.main if "value" in r]
        record2_with_value = [r for r in result2.main if "value" in r]

        assert len(record1_with_value) == 1
        assert len(record2_with_value) == 1

    def test_null_handling_deterministic(self):
        """Test that null handling produces deterministic results."""
        data = {"id": 1, "null1": None, "valid": "test", "null2": None}

        config = TransmogConfig(include_nulls=False)
        result1 = tm.flatten(data, name="test", config=config)
        result2 = tm.flatten(data, name="test", config=config)

        # Results should be identical
        assert set(result1.main[0].keys()) == set(result2.main[0].keys())


class TestNullHandlingWithArrayModes:
    """Test null handling interaction with different array modes."""

    def test_null_in_array_smart_mode(self):
        """Test null values in arrays with SMART mode."""
        from transmog.types import ArrayMode

        data = {"id": 1, "values": [1, None, 2, None, 3]}

        config = TransmogConfig(array_mode=ArrayMode.SMART, include_nulls=False)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        # Simple array should be preserved
        assert "values" in main

    def test_null_in_array_separate_mode(self):
        """Test null values in arrays with SEPARATE mode."""
        from transmog.types import ArrayMode

        data = {
            "id": 1,
            "items": [
                {"name": "Item1", "value": 10},
                {"name": None, "value": 20},
                {"name": "Item3", "value": None},
            ],
        }

        config = TransmogConfig(array_mode=ArrayMode.SEPARATE, include_nulls=False)
        result = tm.flatten(data, name="test", config=config)

        # Should have child table
        assert len(result.tables) > 0

    def test_null_in_array_inline_mode(self):
        """Test null values in arrays with INLINE mode."""
        from transmog.types import ArrayMode

        data = {"id": 1, "values": [1, None, 2]}

        config = TransmogConfig(array_mode=ArrayMode.INLINE)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        # Array should be JSON-serialized
        assert "values" in main

    def test_null_in_array_skip_mode(self):
        """Test null values in arrays with SKIP mode."""
        from transmog.types import ArrayMode

        data = {"id": 1, "values": [1, None, 2], "name": "test"}

        config = TransmogConfig(array_mode=ArrayMode.SKIP)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        # Array should be skipped entirely
        assert "values" not in main
        assert "name" in main


class TestNullVsMissingKey:
    """Test distinction between explicit None and missing keys."""

    def test_explicit_none_vs_missing_skip_mode(self):
        """Test that explicit None and missing key behave same in skip mode."""
        data_with_none = {"id": 1, "name": "Alice", "optional": None}
        data_missing_key = {"id": 2, "name": "Bob"}

        result1 = tm.flatten(data_with_none, name="test")
        result2 = tm.flatten(data_missing_key, name="test")

        # Both should not have 'optional' field
        assert "optional" not in result1.main[0]
        assert "optional" not in result2.main[0]

    def test_explicit_none_vs_missing_include_mode(self):
        """Test that explicit None is included but missing key is not."""
        data_with_none = {"id": 1, "name": "Alice", "optional": None}
        data_missing_key = {"id": 2, "name": "Bob"}

        config = TransmogConfig(include_nulls=True)
        result1 = tm.flatten(data_with_none, name="test", config=config)
        result2 = tm.flatten(data_missing_key, name="test", config=config)

        # Explicit None should be included
        assert "optional" in result1.main[0]
        assert result1.main[0]["optional"] is None

        # Missing key should not be present
        assert "optional" not in result2.main[0]

    def test_batch_with_mixed_none_and_missing(self):
        """Test batch where some records have None, others missing key."""
        data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": None},
            {"id": 3, "name": "Charlie"},  # Missing score
        ]

        # Skip mode
        result_skip = tm.flatten(data, name="test")
        assert "score" in result_skip.main[0]
        assert "score" not in result_skip.main[1]
        assert "score" not in result_skip.main[2]

        # Include mode
        config = TransmogConfig(include_nulls=True)
        result_include = tm.flatten(data, name="test", config=config)
        assert "score" in result_include.main[0]
        assert "score" in result_include.main[1]  # Explicit None included
        assert "score" not in result_include.main[2]  # Missing key still not present


class TestNullInNestedStructures:
    """Test null handling in various nested structure scenarios."""

    def test_null_object_value_in_nested(self):
        """Test None as entire nested object value."""
        data = {"id": 1, "nested": None, "valid": "data"}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "valid" in main
        # None nested object should not create any nested_ fields
        assert not any(key.startswith("nested") for key in main.keys())

    def test_null_at_multiple_nesting_levels(self):
        """Test nulls at different nesting levels."""
        data = {
            "id": 1,
            "level1_null": None,
            "level1": {
                "level2_null": None,
                "level2": {"level3_null": None, "level3_valid": "deep_value"},
            },
        }

        result = tm.flatten(data, name="test")

        main = result.main[0]
        # Only the valid deep value should be present
        assert any("level3_valid" in key for key in main.keys())
        assert not any("null" in key for key in main.keys())

    def test_null_sibling_to_valid_nested(self):
        """Test null value as sibling to valid nested object."""
        data = {
            "id": 1,
            "null_sibling": None,
            "valid_sibling": {"key": "value"},
        }

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "valid_sibling_key" in main
        assert "null_sibling" not in main
