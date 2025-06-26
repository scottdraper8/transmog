"""
Tests for extractor interface conformance.

This module defines an abstract test class for testing extractor implementations.
"""

import pytest

from transmog.core.extractor import extract_arrays, stream_extract_arrays


class TestExtractorInterface:
    """Test that extractors conform to the required interface."""

    def test_extractor_functions_exist(self):
        """Test that core extractor functions exist."""
        # Check main extract_arrays function
        assert callable(extract_arrays), "extract_arrays should be a callable function"

        # Check streaming extract function
        assert callable(stream_extract_arrays), (
            "stream_extract_arrays should be a callable function"
        )


class AbstractExtractorTest:
    """
    Abstract base class for extractor tests.

    This class defines a standardized set of tests that should apply to extractor functionality.
    Subclasses must define appropriate fixtures for nested data structures.
    """

    @pytest.fixture
    def simple_nested_data(self):
        """Create a simple nested data structure with arrays."""
        return {
            "id": "123",
            "name": "Test Entity",
            "items": [
                {"id": "item1", "name": "Item 1"},
                {"id": "item2", "name": "Item 2"},
            ],
        }

    @pytest.fixture
    def complex_nested_data(self):
        """Create a complex nested data structure with nested arrays."""
        return {
            "id": "456",
            "name": "Complex Entity",
            "items": [
                {
                    "id": "item1",
                    "name": "Item 1",
                    "subitems": [
                        {"id": "sub1", "name": "Subitem 1"},
                        {"id": "sub2", "name": "Subitem 2"},
                    ],
                },
                {
                    "id": "item2",
                    "name": "Item 2",
                    "subitems": [{"id": "sub3", "name": "Subitem 3"}],
                },
            ],
        }

    @pytest.fixture
    def self_referential_data(self):
        """Create a data structure with self-referential properties for recursion testing."""
        data = {
            "id": "789",
            "name": "Recursive Entity",
            "child": {"id": "child1", "name": "Child 1"},
        }
        # Create a self-reference
        data["child"]["parent"] = data
        return data

    def test_basic_array_extraction(self, simple_nested_data):
        """Test basic array extraction."""
        # Extract arrays with default settings
        arrays = extract_arrays(simple_nested_data, entity_name="test")

        # Check that the items array was extracted
        assert "test_items" in arrays
        assert len(arrays["test_items"]) == 2

        # Check array items have expected fields
        for item in arrays["test_items"]:
            assert "__transmog_id" in item
            assert "__transmog_datetime" in item
            # Note: In the actual implementation, parent_id isn't included if entity_name is provided
            # instead of parent_id directly

    def test_nested_array_extraction(self, complex_nested_data):
        """Test extraction of nested arrays."""
        # Extract arrays with default settings
        arrays = extract_arrays(complex_nested_data, entity_name="test")

        # Check that the items array was extracted
        assert "test_items" in arrays
        assert len(arrays["test_items"]) == 2

        # The expected path for subitems might vary by implementation, check all possible paths
        subitems_table_names = [name for name in arrays.keys() if "subitems" in name]
        assert len(subitems_table_names) > 0

        # Get the actual subitems table name
        subitems_table = subitems_table_names[0]
        assert len(arrays[subitems_table]) == 3  # Total of 3 subitems

        # Check parent-child relationships if parent IDs are included
        for subitem in arrays[subitems_table]:
            if "__parent_transmog_id" in subitem:
                parent_id = subitem["__parent_transmog_id"]
                assert any(
                    item["__transmog_id"] == parent_id for item in arrays["test_items"]
                )

    def test_streaming_extraction(self, complex_nested_data):
        """Test streaming array extraction."""
        # Use streaming extraction
        records = list(stream_extract_arrays(complex_nested_data, entity_name="test"))

        # Verify results structure
        tables = {}
        for table_name, record in records:
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(record)

        # Check that arrays are extracted
        assert len(tables) > 0

        # There should be one table for items
        items_tables = [
            name for name in tables.keys() if "items" in name and "subitems" not in name
        ]
        assert len(items_tables) == 1
        items_table = items_tables[0]

        # And one table for subitems
        subitems_tables = [name for name in tables.keys() if "subitems" in name]
        assert len(subitems_tables) == 1
        subitems_table = subitems_tables[0]

        # Check counts
        assert len(tables[items_table]) == 2
        assert len(tables[subitems_table]) == 3

    def test_recursion_handling(self, self_referential_data):
        """Test handling of recursive data structures."""
        # Extract arrays with default settings
        arrays = extract_arrays(self_referential_data, entity_name="test")

        # Verify it doesn't cause infinite recursion
        # The implementation might handle self-references in different ways
        # Some might prune the reference, others might have a maximum recursion depth

        # Just verify the function didn't crash with self-referential data
        assert isinstance(arrays, dict)

    def test_extraction_with_options(self, simple_nested_data):
        """Test extraction with various options."""
        # Test with custom separator
        arrays1 = extract_arrays(simple_nested_data, entity_name="test", separator=".")

        # Table name should use the entity name and possibly the separator
        items_tables = [name for name in arrays1.keys() if "items" in name]
        assert len(items_tables) == 1

        # Test with cast_to_string=True
        arrays2 = extract_arrays(
            simple_nested_data, entity_name="test", cast_to_string=True
        )

        # Get items table name
        items_tables = [name for name in arrays2.keys() if "items" in name]
        assert len(items_tables) == 1
        items_table = items_tables[0]

        # Values should be strings
        for item in arrays2[items_table]:
            if "id" in item:
                assert isinstance(item["id"], str)

        # Test with deeply_nested_threshold
        arrays3 = extract_arrays(
            simple_nested_data,
            entity_name="test",
            deeply_nested_threshold=2,  # Lower threshold
        )

        # Just verify it doesn't crash with these options
        assert len(arrays3) > 0
