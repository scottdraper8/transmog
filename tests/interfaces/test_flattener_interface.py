"""
Tests for flattener interface conformance.

This module defines an abstract test class for testing flattener implementations.
"""

import pytest
from typing import Any, Dict, List, Set, Optional

from transmog.core.flattener import flatten_json
from transmog.error import CircularReferenceError, ProcessingError


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

    @pytest.fixture
    def circular_data(self):
        """Create data with circular references."""
        data = {"id": 4, "name": "Circular"}
        data["self"] = data
        return data

    @pytest.fixture
    def field_abbrev_data(self):
        """Create data with long field names for abbreviation testing."""
        return {
            "very_long_field_name": "value",
            "nested": {
                "another_very_long_field_name": "nested value",
            },
        }

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
        assert isinstance(flattened_default["string"], str)
        assert isinstance(flattened_default["integer"], str)
        assert isinstance(flattened_default["float"], str)
        assert isinstance(flattened_default["boolean"], str)
        assert isinstance(flattened_default["nested_value"], str)

        # Check values are properly converted
        assert flattened_default["integer"] == "42"
        assert flattened_default["float"] == "3.14"
        assert flattened_default["boolean"] == "true"

        # Test with null values and skip_null=False
        data_with_null = {
            "string": "text",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "none": None,
            "nested": {
                "value": 123,
            },
        }

        # Test with explicit cast_to_string=True, skip_null=False
        flattened_string = flatten_json(
            data_with_null, cast_to_string=True, skip_null=False
        )

        # None should be converted to empty string
        assert "none" in flattened_string
        assert flattened_string["none"] == ""

        # Test with explicit cast_to_string=False
        flattened_raw = flatten_json(data, cast_to_string=False)

        # Values should keep their original types
        assert isinstance(flattened_raw["string"], str)
        assert isinstance(flattened_raw["integer"], int)
        assert isinstance(flattened_raw["float"], float)
        assert isinstance(flattened_raw["boolean"], bool)
        # Confirm values
        assert flattened_raw["integer"] == 42
        assert flattened_raw["float"] == 3.14
        assert flattened_raw["boolean"] is True

    def test_include_empty(self, empty_data):
        """Test the include_empty option."""
        # Create test data with no underscores for clearer testing
        data = {
            "value1": "",
            "value2": "non-empty",
            "nested": {
                "empty": "",
                "nonempty": "value",
            },
        }

        # With include_empty=True
        flattened_with_empty = flatten_json(data, include_empty=True)

        # Empty values should be included
        assert "value1" in flattened_with_empty
        assert flattened_with_empty["value1"] == ""
        assert "nested_empty" in flattened_with_empty
        assert flattened_with_empty["nested_empty"] == ""

        # Non-empty values should be included
        assert "value2" in flattened_with_empty
        assert "nested_nonempty" in flattened_with_empty

        # With include_empty=False (default)
        flattened_without_empty = flatten_json(data, include_empty=False)

        # Empty values should be excluded
        assert "value1" not in flattened_without_empty
        assert "nested_empty" not in flattened_without_empty

        # Non-empty values should still be present
        assert "value2" in flattened_without_empty
        assert "nested_nonempty" in flattened_without_empty

    def test_skip_null(self, null_data):
        """Test the skip_null option."""
        # Create test data with no underscores for clearer testing
        data = {
            "value1": None,
            "value2": "non-null",
            "nested": {
                "null": None,
                "nonnull": "value",
            },
        }

        # With skip_null=True (default)
        flattened_skip_null = flatten_json(data, skip_null=True)

        # Null values should be skipped
        assert "value1" not in flattened_skip_null
        assert "nested_null" not in flattened_skip_null

        # Non-null values should be present
        assert "value2" in flattened_skip_null
        assert "nested_nonnull" in flattened_skip_null

        # With skip_null=False, cast_to_string=True (default)
        flattened_with_null_as_string = flatten_json(
            data, skip_null=False, cast_to_string=True
        )

        # Null values should be included as empty strings
        assert "value1" in flattened_with_null_as_string
        assert flattened_with_null_as_string["value1"] == ""
        assert "nested_null" in flattened_with_null_as_string
        assert flattened_with_null_as_string["nested_null"] == ""

        # With skip_null=False, cast_to_string=False
        flattened_with_null_raw = flatten_json(
            data, skip_null=False, cast_to_string=False
        )

        # Null values should be included as empty strings (since None can't be represented in many output formats)
        assert "value1" in flattened_with_null_raw
        assert flattened_with_null_raw["value1"] == ""
        assert "nested_null" in flattened_with_null_raw
        assert flattened_with_null_raw["nested_null"] == ""

    def test_deep_nesting(self, deep_data):
        """Test flattening deeply nested structures."""
        # Flatten with default settings and no abbreviation
        flattened = flatten_json(deep_data, abbreviate_field_names=False)

        # Check that a deeply nested value exists (exact field name may vary)
        nested_value_keys = [k for k in flattened.keys() if k.endswith("_value")]
        assert len(nested_value_keys) > 0, "No deeply nested value found"

        # The value should be present and correct
        assert flattened[nested_value_keys[0]] == "deep"

    def test_circular_reference_detection(self, circular_data):
        """Test detection of circular references."""
        # The implementation may raise either a CircularReferenceError directly
        # or wrap it in a ProcessingError
        try:
            flatten_json(circular_data)
            pytest.fail(
                "Expected CircularReferenceError or ProcessingError but no exception was raised"
            )
        except (CircularReferenceError, ProcessingError) as e:
            # If it's a ProcessingError, check if it contains information about circular references
            if isinstance(e, ProcessingError):
                error_msg = str(e).lower()
                assert "circular" in error_msg or "recursion" in error_msg, (
                    f"ProcessingError does not indicate circular reference: {error_msg}"
                )
            # Otherwise, it should be a CircularReferenceError
            else:
                assert isinstance(e, CircularReferenceError), (
                    f"Expected CircularReferenceError but got {type(e).__name__}"
                )

    def test_array_handling(self, array_data):
        """Test handling of arrays in the input data."""
        # Test with skip_arrays=True (default)
        flattened_skip = flatten_json(array_data, skip_arrays=True)

        # Arrays should be skipped - no array fields should be present
        assert "items" not in flattened_skip
        assert not any(key.startswith("items_") for key in flattened_skip.keys())

        # Basic fields should still be present
        assert "id" in flattened_skip

        # Test with skip_arrays=False
        flattened_include = flatten_json(array_data, skip_arrays=False)

        # Arrays should be included, but not flattened (unless visit_arrays=True)
        assert "items" in flattened_include
        assert isinstance(flattened_include["items"], (str, list)), (
            f"Expected array or string, got {type(flattened_include['items'])}"
        )

    def test_visit_arrays(self, array_data):
        """Test the visit_arrays option for flattening arrays."""
        # With visit_arrays=True
        flattened_visit = flatten_json(array_data, visit_arrays=True, skip_arrays=False)

        # Should have flattened array items
        array_item_keys = [
            key for key in flattened_visit.keys() if key.startswith("items_")
        ]
        assert len(array_item_keys) > 0, "Expected flattened array items"

        # Check that array item values are accessible
        for key in array_item_keys:
            assert flattened_visit[key] is not None

        # With visit_arrays=False
        flattened_no_visit = flatten_json(
            array_data, visit_arrays=False, skip_arrays=False
        )

        # Should not have flattened array items
        assert not any(key.startswith("items_") for key in flattened_no_visit.keys())
        # But the array itself should be present
        assert "items" in flattened_no_visit

    def test_max_depth(self, deep_data):
        """Test the max_depth option to limit recursion depth."""
        # Test with limited depth (5 levels)
        max_depth = 5
        flattened_limited = flatten_json(
            deep_data, max_depth=max_depth, abbreviate_field_names=False
        )

        # Create a deep path that should NOT be in the result
        # This path would only exist if we went beyond max_depth
        deep_values = [k for k in flattened_limited.keys() if k.endswith("_value")]

        # The deep value should not be present
        assert len(deep_values) == 0, (
            f"Found deep value keys beyond max_depth: {deep_values}"
        )

        # Check that we have some fields at the max depth
        level5_path = "level0_level1_level2_level3_level4"
        level5_fields = [
            k for k in flattened_limited.keys() if k.startswith(level5_path)
        ]

        # Should have at least one field at max depth
        assert len(level5_fields) > 0, (
            f"No fields found at max depth (level {max_depth})"
        )

    def test_abbreviate_field_names(self, field_abbrev_data):
        """Test field name abbreviation."""
        # Create test data without underscores to test clearly
        clean_data = {
            "verylongfieldname": "value",
            "nested": {"anotherverylongfieldname": "nested value"},
        }

        # With abbreviation enabled with max length constraint
        flattened_abbrev = flatten_json(
            clean_data, abbreviate_field_names=True, max_field_component_length=5
        )

        # Root name should be preserved by default
        assert "verylongfieldname" in flattened_abbrev, (
            f"Root name not preserved, got {list(flattened_abbrev.keys())}"
        )

        # Nested field exists with some abbreviation
        nested_keys = [k for k in flattened_abbrev.keys() if k.startswith("nested_")]
        assert len(nested_keys) > 0, "No nested keys found in result"

        # With abbreviation disabled
        flattened_full = flatten_json(clean_data, abbreviate_field_names=False)

        # Field names should be preserved at full length
        assert "verylongfieldname" in flattened_full
        assert "nested_anotherverylongfieldname" in flattened_full

    def test_preserve_components(self):
        """Test the preserve_root_component and preserve_leaf_component options."""
        # Data with nested fields - no underscores in names to avoid confusion
        data = {
            "rootcomp": {
                "middlecomp": {
                    "leafcomp": "value",
                },
            },
        }

        max_len = 3  # Max component length for clear truncation

        # Test 1: Default behavior (preserve both root and leaf)
        flattened_default = flatten_json(
            data,
            abbreviate_field_names=True,
            max_field_component_length=max_len,
        )

        # Check result keys
        keys = list(flattened_default.keys())
        assert len(keys) == 1, f"Expected 1 key, got {len(keys)}: {keys}"
        key = keys[0]

        # By default, first (root) and last (leaf) components are preserved
        assert key.startswith("rootcomp_"), (
            f"Expected key to start with 'rootcomp_', got {key}"
        )
        assert key.endswith("_leafcomp"), (
            f"Expected key to end with '_leafcomp', got {key}"
        )

        # Middle component should be abbreviated
        middle_part = key[len("rootcomp_") : -len("_leafcomp")]
        assert len(middle_part) <= max_len, (
            f"Middle part '{middle_part}' exceeds max length {max_len}"
        )

        # Test 2: Preserve root only
        flattened_root_only = flatten_json(
            data,
            abbreviate_field_names=True,
            max_field_component_length=max_len,
            preserve_root_component=True,
            preserve_leaf_component=False,
        )

        keys = list(flattened_root_only.keys())
        key = keys[0]

        # Root should be preserved
        assert key.startswith("rootcomp_"), (
            f"Expected key to start with 'rootcomp_', got {key}"
        )

        # Leaf should be truncated
        assert not key.endswith("_leafcomp"), (
            f"Key should not end with '_leafcomp', got {key}"
        )
        # Last part should be truncated to max length
        parts = key.split("_")
        assert len(parts[-1]) <= max_len, (
            f"Last part '{parts[-1]}' should be truncated to {max_len} chars"
        )

        # Test 3: Preserve leaf only
        flattened_leaf_only = flatten_json(
            data,
            abbreviate_field_names=True,
            max_field_component_length=max_len,
            preserve_root_component=False,
            preserve_leaf_component=True,
        )

        keys = list(flattened_leaf_only.keys())
        key = keys[0]

        # Root should be truncated
        assert not key.startswith("rootcomp_"), (
            f"Key should not start with 'rootcomp_', got {key}"
        )
        first_part = key.split("_")[0]
        assert len(first_part) <= max_len, (
            f"First part '{first_part}' should be truncated to {max_len} chars"
        )

        # Leaf should be preserved
        assert key.endswith("_leafcomp"), (
            f"Expected key to end with '_leafcomp', got {key}"
        )

        # Test 4: No preservation (truncate everything)
        flattened_no_preserve = flatten_json(
            data,
            abbreviate_field_names=True,
            max_field_component_length=max_len,
            preserve_root_component=False,
            preserve_leaf_component=False,
        )

        keys = list(flattened_no_preserve.keys())
        key_parts = keys[0].split("_")

        # All components should be truncated
        for part in key_parts:
            assert len(part) <= max_len, (
                f"Part '{part}' should be truncated to {max_len} chars"
            )

    def test_custom_abbreviations(self):
        """Test using custom abbreviations."""
        # Data with fields to abbreviate (no underscores for cleaner testing)
        data = {
            "customer": {
                "firstname": "John",
                "lastname": "Doe",
            },
            "transaction": {
                "amount": 100,
            },
        }

        # Custom abbreviations
        custom_abbrev = {
            "customer": "cust",
            "firstname": "fname",
            "lastname": "lname",
            "transaction": "tx",
            "amount": "amt",
        }

        # Test with abbreviate_field_names=True and custom abbreviations
        flattened = flatten_json(
            data, abbreviate_field_names=True, custom_abbreviations=custom_abbrev
        )

        # By default, root component is preserved
        assert any(k.startswith("customer_") for k in flattened.keys())
        assert any(k.startswith("transaction_") for k in flattened.keys())

        # Test with preserve_root_component=False to apply abbreviations to root
        flattened_abbrev_root = flatten_json(
            data,
            abbreviate_field_names=True,
            custom_abbreviations=custom_abbrev,
            preserve_root_component=False,
        )

        # Root components should be abbreviated
        assert any(k.startswith("cust_") for k in flattened_abbrev_root.keys())
        assert any(k.startswith("tx_") for k in flattened_abbrev_root.keys())

        # Test with both root and leaf component abbreviation
        flattened_abbrev_all = flatten_json(
            data,
            abbreviate_field_names=True,
            custom_abbreviations=custom_abbrev,
            preserve_root_component=False,
            preserve_leaf_component=False,
        )

        # Check both root and leaf components are abbreviated
        assert any("cust_fname" == k for k in flattened_abbrev_all.keys())
        assert any("cust_lname" == k for k in flattened_abbrev_all.keys())
        assert any("tx_amt" == k for k in flattened_abbrev_all.keys())
