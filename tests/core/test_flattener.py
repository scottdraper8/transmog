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

        # Test with explicit visit_arrays=True
        flattened_visit = flatten_json(array_data, visit_arrays=True, skip_arrays=False)

        # With visit_arrays=True in the simplified naming scheme,
        # the array itself is preserved as a JSON string
        assert "items" in flattened_visit
        assert isinstance(flattened_visit["items"], str)

    def test_simplified_naming(self, array_data):
        """Test that array flattening uses the simplified naming scheme without indices."""
        # Create data with nested arrays
        data = {
            "id": 1,
            "items": [{"name": "item1", "value": 100}, {"name": "item2", "value": 200}],
        }

        # Flatten with the simplified naming scheme
        flattened = flatten_json(data, visit_arrays=True, skip_arrays=False)

        # Check that we have flattened array items with simple names (no indices)
        assert "items" in flattened  # The array itself is preserved

        # The items should be flattened as a JSON string
        assert isinstance(flattened["items"], str)

        # We should not have index-based fields
        assert not any(key.startswith("items[") for key in flattened.keys())
