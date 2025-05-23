"""
Tests for the flattener implementation.

This module tests the core flattener functionality using the interface-based approach.
"""

import pytest

# Import and inherit from the interface
from tests.interfaces.test_flattener_interface import AbstractFlattenerTest
from transmog.config import TransmogConfig
from transmog.core.flattener import flatten_json
from transmog.error import ProcessingError


class TestFlattener(AbstractFlattenerTest):
    """
    Tests for the flattener module.

    Inherits from AbstractFlattenerTest to ensure it follows the interface-based testing pattern.
    """

    def test_flattener_with_config(self, simple_data):
        """Test flattening with a TransmogConfig object."""
        # Create processor with explicit configuration
        proc_config = (
            TransmogConfig.default()
            .with_naming(separator="_", deeply_nested_threshold=5)
            .with_processing(cast_to_string=False)
        )

        # Use the TransmogConfig to get the parameters
        flattened = flatten_json(
            simple_data,
            separator=proc_config.naming.separator,
            cast_to_string=proc_config.processing.cast_to_string,
            deeply_nested_threshold=proc_config.naming.deeply_nested_threshold,
        )

        # Check basic fields are preserved
        assert flattened["id"] == 1
        assert flattened["name"] == "Test"

        # Check nested fields are flattened
        assert "address_street" in flattened
        assert "address_city" in flattened
        assert "address_state" in flattened

    def test_sanitize_field_names(self):
        """Test the sanitize_field_names option."""
        # Create data with special characters in field names
        data = {
            "field with spaces": "value1",
            "field-with-hyphens": "value2",
            "field.with.dots": "value3",
            "nested": {
                "field+with+plus+signs": "nested value",
            },
        }

        # In the current implementation, field names are not sanitized by default
        # They are only sanitized in paths when combining them
        flattened = flatten_json(data)

        # Verify the keys are preserved as-is for top-level fields
        assert "field with spaces" in flattened
        assert "field-with-hyphens" in flattened
        assert "field.with.dots" in flattened

        # Nested fields are joined with separator, but the field names themselves
        # are not sanitized in the simplified naming scheme
        assert "nested_field+with+plus+signs" in flattened

    def test_in_place_option(self, simple_data):
        """Test the in_place option."""
        # Make a copy of the data for testing
        data_copy = dict(simple_data)

        # Flatten with in_place=True
        flattened = flatten_json(data_copy, in_place=True)

        # The flattened result should be the same object as the input
        assert flattened is data_copy

        # Check flattened fields were added
        assert "address_street" in flattened
        assert "address_city" in flattened
        assert "address_state" in flattened

        # In the current implementation, in_place flattening adds the flattened
        # fields but doesn't remove the original nested structure
        assert "address" in flattened
        assert isinstance(flattened["address"], dict)

    def test_error_handling(self):
        """Test error handling with non-serializable objects."""

        # Create a non-JSON-serializable object
        class BadObject:
            def __eq__(self, other):
                return False

            def __repr__(self):
                return "<BadObject>"

        # Create data with the bad object
        data = {"bad": BadObject()}

        # Test with lenient error handling
        with pytest.raises(ProcessingError):
            flatten_json(data, error_handling="lenient")

        # Test with strict error handling
        with pytest.raises(ProcessingError):
            flatten_json(data, error_handling="strict")

    def test_max_depth(self, deep_data):
        """Test the max_depth option to limit recursion depth."""
        # Test with a higher depth than the fixture actually has
        higher_depth = 20
        flattened_full = flatten_json(deep_data, max_depth=higher_depth)

        # Should have processed at least some fields
        assert len(flattened_full) > 0

        # Get the maximum possible depth from the keys
        max_possible_depth = 0
        for key in flattened_full.keys():
            depth = key.count("_") + 1  # Count separators plus 1
            max_possible_depth = max(max_possible_depth, depth)

        # Now use a max_depth less than what we observed
        if max_possible_depth > 1:
            lower_depth = max_possible_depth - 1
            flattened_limited = flatten_json(deep_data, max_depth=lower_depth)

            # The limited version should have fewer fields
            assert len(flattened_limited) < len(flattened_full)

    def test_deeply_nested_threshold(self, deep_data):
        """Test the deeply_nested_threshold option for handling deeply nested paths."""
        # Create a deep path with standard threshold (default 4)
        standard_threshold = 4
        flattened_standard = flatten_json(
            deep_data, deeply_nested_threshold=standard_threshold
        )

        # Some paths should be simplified with "nested" indicator
        nested_paths = [key for key in flattened_standard.keys() if "nested" in key]
        assert len(nested_paths) > 0

        # Test with a much higher threshold where no simplification should occur
        high_threshold = 20
        flattened_high = flatten_json(deep_data, deeply_nested_threshold=high_threshold)

        # No paths should be simplified
        high_nested_paths = [key for key in flattened_high.keys() if "nested" in key]
        assert len(high_nested_paths) == 0

        # The high threshold version should have more complex keys
        assert sum(key.count("_") for key in flattened_high.keys()) > sum(
            key.count("_") for key in flattened_standard.keys()
        )

    def test_array_handling(self, array_data):
        """Test handling of arrays in the input data."""
        # Test with skip_arrays=True (default)
        flattened_skip = flatten_json(array_data, skip_arrays=True)

        # Arrays should be skipped as single entities, but array items may be flattened
        assert "items" not in flattened_skip
        # Basic fields should still be present
        assert "id" in flattened_skip

        # Test with skip_arrays=False but without visit_arrays
        flattened_include = flatten_json(
            array_data, skip_arrays=False, visit_arrays=False
        )

        # The array itself should be included when skip_arrays=False
        assert "items" in flattened_include
        # The array should be a string (JSON representation) or list
        assert isinstance(flattened_include["items"], (str, list))

        # Test with explicit visit_arrays=True and in_place=True
        flattened_visit = flatten_json(
            array_data.copy(),
            visit_arrays=True,
            skip_arrays=False,
            in_place=True,  # Even with in_place=True, arrays are processed
        )

        # With the current implementation, arrays are always processed
        # When visit_arrays=True, the array is stringified
        assert "items" in flattened_visit
        assert isinstance(flattened_visit["items"], str)

        # Test with explicit visit_arrays=True and in_place=False (default)
        flattened_visit_no_preserve = flatten_json(
            array_data.copy(),
            visit_arrays=True,
            skip_arrays=False,
            in_place=False,  # This is the default, removes original arrays
        )

        # With improved behavior, the original array is removed
        assert "items" not in flattened_visit_no_preserve
        # But we should have results from flattening the array items
        assert "id" in flattened_visit_no_preserve  # The root id is preserved

    def test_simplified_naming(self, array_data):
        """Test that array flattening uses the simplified naming scheme without indices."""
        # Create data with nested arrays
        data = {
            "id": 1,
            "items": [{"name": "item1", "value": 100}, {"name": "item2", "value": 200}],
        }

        # Flatten with the simplified naming scheme and in_place=True to keep original arrays
        flattened = flatten_json(
            data.copy(), visit_arrays=True, skip_arrays=False, in_place=True
        )

        # With in_place=True, the array is preserved but stringified
        assert "items" in flattened

        # The items should be flattened as a JSON string
        assert isinstance(flattened["items"], str)

        # Test with in_place=False (default behavior)
        flattened_no_preserve = flatten_json(
            data.copy(), visit_arrays=True, skip_arrays=False
        )

        # With the improved behavior, the original array is removed
        assert "items" not in flattened_no_preserve

        # We should not have index-based fields in either case
        assert not any(key.startswith("items[") for key in flattened.keys())
        assert not any(key.startswith("items[") for key in flattened_no_preserve.keys())

    def test_empty_objects_and_arrays_are_skipped(self):
        """Test that empty objects and arrays are skipped."""
        # Create test data with empty objects and arrays
        data = {
            "id": 1,
            "name": "Test",
            "empty_object": {},
            "empty_array": [],
            "nested": {
                "value": "nested_value",
                "empty_nested_object": {},
                "empty_nested_array": [],
            },
            "non_empty_object": {"key": "value"},
            "array_with_empty_objects": [{}, {}, {}],
            "non_empty_array": [1, 2, 3],
        }

        # Flatten the data
        flattened = flatten_json(data)

        # Empty objects and arrays should be skipped
        assert "empty_object" not in flattened
        assert "empty_array" not in flattened
        assert "nested_empty_nested_object" not in flattened
        assert "nested_empty_nested_array" not in flattened

        # Original keys should be removed after flattening nested objects
        assert "nested" not in flattened
        assert "nested_value" in flattened

        # Non-empty objects and arrays should be flattened/processed correctly
        assert "non_empty_object" not in flattened  # Original object removed
        assert "non_empty_object_key" in flattened  # But flattened key exists

        # An array containing only empty objects is effectively empty and should be skipped
        assert "array_with_empty_objects" not in flattened

        # Non-empty arrays should be preserved in string format
        # But only if we use in_place=True
        flattened_with_arrays = flatten_json(data.copy(), in_place=True)
        assert isinstance(flattened_with_arrays.get("non_empty_array", None), str)

    def test_original_nested_structures_removed(self):
        """Test that original nested structures are removed after flattening."""
        # Create deeply nested test data
        data = {
            "id": 1,
            "level1": {
                "value1": "foo",
                "level2": {"value2": "bar", "level3": {"value3": "baz"}},
            },
        }

        # Flatten the data
        flattened = flatten_json(data)

        # Original nested structure keys should be removed
        assert "level1" not in flattened
        assert "level1_level2" not in flattened
        assert "level1_level2_level3" not in flattened

        # But their values should be preserved with flattened paths
        assert "level1_value1" in flattened
        assert "level1_level2_value2" in flattened
        assert "level1_level2_level3_value3" in flattened
