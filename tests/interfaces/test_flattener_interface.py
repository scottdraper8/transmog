"""
Tests for flattener interface conformance.

This module defines an abstract test class for testing flattener implementations.
"""

import pytest

from transmog.core.flattener import flatten_json
from transmog.error import ProcessingError


class TestFlattenerInterface:
    """Test that flattener implements the required interface."""

    def test_flattener_interface(self):
        """Test that flattener implements the required interface."""
        # Check if the flatten_json function exists
        assert callable(flatten_json), "flatten_json must be callable"

        # Test basic functionality
        data = {"id": 1, "name": "Test"}
        result = flatten_json(data)

        assert isinstance(result, dict), "flatten_json must return a dictionary"
        assert "id" in result, "flatten_json must preserve top-level fields"
        assert "name" in result, "flatten_json must preserve top-level fields"


class AbstractFlattenerTest:
    """
    Abstract base class for flattener tests.

    This class defines a standardized set of tests that should apply to all flattener implementations.
    Subclasses must implement appropriate fixtures if needed.
    """

    @pytest.fixture
    def simple_data(self):
        """Create a simple data structure."""
        return {
            "id": 1,
            "name": "Test",
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "state": "CA",
            },
        }

    @pytest.fixture
    def nested_data(self):
        """Create a nested data structure."""
        return {
            "id": 2,
            "details": {
                "info": {
                    "value": "test",
                },
            },
        }

    @pytest.fixture
    def array_data(self):
        """Create data with arrays."""
        return {
            "id": 3,
            "items": [
                {"id": 1, "value": "one"},
                {"id": 2, "value": "two"},
            ],
        }

    @pytest.fixture
    def type_data(self):
        """Create data with various types."""
        return {
            "string": "text",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "none": None,
            "nested": {
                "value": 123,
            },
        }

    @pytest.fixture
    def empty_data(self):
        """Create data with empty values."""
        return {
            "value1": "",
            "value2": "non-empty",
            "nested": {
                "empty": "",
                "nonempty": "value",
            },
        }

    @pytest.fixture
    def null_data(self):
        """Create data with null values."""
        return {
            "value1": None,
            "value2": "non-null",
            "nested": {
                "null": None,
                "nonnull": "value",
            },
        }

    @pytest.fixture
    def deep_data(self):
        """Create a deeply nested structure."""
        data = {"level0": {}}
        current = data["level0"]

        # Create 10 levels of nesting
        for i in range(1, 10):
            current[f"level{i}"] = {}
            current = current[f"level{i}"]

        # Add a value at the deepest level
        current["value"] = "deep"

        return data

    def test_basic_flattening(self, simple_data):
        """Test basic flattening of a nested dictionary."""
        flattened = flatten_json(simple_data)

        # Since cast_to_string=True is the default, all values are strings
        assert flattened["id"] == "1"
        assert flattened["name"] == "Test"

        # Check nested fields are flattened
        assert "address_street" in flattened
        assert "address_city" in flattened
        assert "address_state" in flattened

        # Check values of flattened fields
        assert flattened["address_street"] == "123 Main St"
        assert flattened["address_city"] == "Anytown"
        assert flattened["address_state"] == "CA"

        # Test with explicit cast_to_string=False
        flattened_raw = flatten_json(simple_data, cast_to_string=False)

        # Values should keep their original types
        assert flattened_raw["id"] == 1
        assert flattened_raw["name"] == "Test"

    def test_separator_option(self, simple_data):
        """Test using different separators for flattened field names."""
        separators = ["_", ".", "-", "/"]

        for sep in separators:
            flattened = flatten_json(simple_data, separator=sep)

            # Check the field name uses the separator
            expected_field = f"address{sep}street"
            assert expected_field in flattened
            assert flattened[expected_field] == "123 Main St"

    def test_cast_to_string(self, type_data):
        """Test the cast_to_string option."""
        # When skip_null=True (default), null values are omitted
        # Let's create data without null values
        data = {
            "string": "text",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "nested": {
                "value": 123,
            },
        }

        # Test with default behavior (should be cast_to_string=True)
        flattened_default = flatten_json(data)

        # By default, all values should be strings
        assert flattened_default["string"] == "text"
        assert flattened_default["integer"] == "42"
        assert flattened_default["float"] == "3.14"
        assert flattened_default["boolean"] == "true"
        assert flattened_default["nested_value"] == "123"

        # Test with cast_to_string=False
        flattened_raw = flatten_json(data, cast_to_string=False)

        # Values should keep their original types
        assert flattened_raw["string"] == "text"
        assert flattened_raw["integer"] == 42
        assert flattened_raw["float"] == 3.14
        assert flattened_raw["boolean"] is True
        assert flattened_raw["nested_value"] == 123

    def test_include_empty(self, empty_data):
        """Test the include_empty option."""
        # Test with default behavior (should be include_empty=False)
        flattened_default = flatten_json(empty_data)

        # Empty values should be skipped by default
        assert "value1" not in flattened_default
        assert "value2" in flattened_default
        assert "nested_empty" not in flattened_default
        assert "nested_nonempty" in flattened_default

        # Test with include_empty=True
        flattened_include = flatten_json(empty_data, include_empty=True)

        # Empty values should be included
        assert "value1" in flattened_include
        assert "value2" in flattened_include
        assert "nested_empty" in flattened_include
        assert "nested_nonempty" in flattened_include

        # Empty values should be empty strings
        assert flattened_include["value1"] == ""
        assert flattened_include["nested_empty"] == ""

    def test_skip_null(self, null_data):
        """Test the skip_null option."""
        # Test with default behavior (should be skip_null=True)
        flattened_default = flatten_json(null_data)

        # Null values should be skipped by default
        assert "value1" not in flattened_default
        assert "value2" in flattened_default
        assert "nested_null" not in flattened_default
        assert "nested_nonnull" in flattened_default

        # Test with skip_null=False - null values should be converted to empty strings
        flattened_include = flatten_json(null_data, skip_null=False)

        # Null values should be included as empty strings when skip_null=False
        assert "value1" in flattened_include
        assert "value2" in flattened_include
        assert "nested_null" in flattened_include
        assert "nested_nonnull" in flattened_include

        # Null values should be converted to empty strings
        assert flattened_include["value1"] == ""
        assert flattened_include["nested_null"] == ""

    def test_deep_nesting(self, deep_data):
        """Test handling of deeply nested structures."""
        flattened = flatten_json(deep_data)

        # Check that we get a flattened result with expected field
        fields = list(flattened.keys())
        assert len(fields) > 0

        # Find the deepest field which should have multiple underscores
        # With the default settings, we should have some "nested" indicators for deeply nested paths
        nested_fields = [field for field in fields if "nested" in field]
        assert len(nested_fields) > 0, f"No nested indicators found in {fields}"

        # Test with higher deeply_nested_threshold to get fully expanded paths
        flattened_expanded = flatten_json(deep_data, deeply_nested_threshold=20)

        # These fields should have full paths without "nested" indicators
        expanded_fields = list(flattened_expanded.keys())
        nested_in_expanded = [field for field in expanded_fields if "nested" in field]
        assert len(nested_in_expanded) == 0, (
            f"Found nested indicators in expanded fields: {nested_in_expanded}"
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
        # In the current implementation, this still skips the arrays but preserves the 'items' key
        flattened_include = flatten_json(
            array_data, skip_arrays=False, visit_arrays=False
        )

        # The array itself should be included when skip_arrays=False
        if "items" in flattened_include:
            # If item is preserved as-is, it will be a string or array
            assert isinstance(flattened_include["items"], (str, list))
        else:
            # In the current implementation, it might be flattened with indexed keys
            has_item_fields = any("items" in key for key in flattened_include.keys())
            assert has_item_fields, (
                f"Expected items fields but got: {flattened_include.keys()}"
            )

    def test_visit_arrays(self, array_data):
        """Test the visit_arrays option."""
        # Test with visit_arrays=False
        flattened_no_visit = flatten_json(array_data, visit_arrays=False)

        # Arrays as objects should not be visited, but may be included based on skip_arrays
        assert not any(
            key != "items" and "items" in key for key in flattened_no_visit.keys()
        )

        # Test with visit_arrays=True (default)
        flattened_visit = flatten_json(array_data, visit_arrays=True)

        # With visit_arrays=True in the simplified naming scheme,
        # the array itself is preserved as a JSON string
        if "items" in flattened_visit:
            assert isinstance(flattened_visit["items"], str)

    def test_max_depth(self, deep_data):
        """Test the max_depth option to limit recursion depth."""
        # Create a fixture to get the maximum depth
        # Using a higher max_depth than in the fixture
        higher_depth = 20
        flattened_deep = flatten_json(deep_data, max_depth=higher_depth)

        # Should have processed all fields
        deepest_field = max(flattened_deep.keys(), key=lambda x: x.count("_"))
        max_underscores = deepest_field.count("_")

        # Use a lower depth limit
        if max_underscores > 1:
            lower_depth = 2  # Arbitrary lower depth

            flattened_limited = flatten_json(deep_data, max_depth=lower_depth)

            # The limited version should have fewer fields
            assert len(flattened_limited) < len(flattened_deep)

            # Find the maximum depth in the limited result
            if flattened_limited:
                limited_max = max(
                    field.count("_") for field in flattened_limited.keys()
                )
                # Should be less than the max from the higher depth result
                assert limited_max <= lower_depth

    def test_deeply_nested_paths(self, deep_data):
        """Test handling of deeply nested paths with different thresholds."""
        # Test with default threshold (4)
        flattened_default = flatten_json(deep_data)

        # Some deeply nested paths should be simplified with "nested" indicators
        nested_fields = [
            field for field in flattened_default.keys() if "nested" in field
        ]
        assert len(nested_fields) > 0, (
            f"Expected simplified paths with nested indicators in {flattened_default.keys()}"
        )

        # Test with lower threshold that simplifies more paths
        flattened_low = flatten_json(deep_data, deeply_nested_threshold=2)

        # More paths should be simplified
        low_nested_fields = [
            field for field in flattened_low.keys() if "nested" in field
        ]
        assert len(low_nested_fields) >= len(nested_fields), (
            "Expected more simplified paths with lower threshold"
        )

        # Test with higher threshold that doesn't simplify paths
        flattened_high = flatten_json(deep_data, deeply_nested_threshold=20)

        # No paths should be simplified
        high_nested_fields = [
            field for field in flattened_high.keys() if "nested" in field
        ]
        assert len(high_nested_fields) == 0, (
            f"Found simplified paths with high threshold: {high_nested_fields}"
        )

    def test_error_handling(self):
        """Test error handling in case of issues."""

        # Define a custom class not directly serializable
        class CustomObject:
            def __repr__(self):
                return "<CustomObject>"

        # Create test data with problematic value
        data = {"problem": CustomObject()}

        # It should raise an error
        with pytest.raises(ProcessingError):
            flatten_json(data)
