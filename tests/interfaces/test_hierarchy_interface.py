"""
Tests for hierarchy interface conformance.

This module defines an abstract test class for testing hierarchy functionality.
"""

import pytest

from transmog.core.hierarchy import (
    process_record_batch,
    process_structure,
    stream_process_records,
)


class TestHierarchyInterface:
    """Test that hierarchy functions conform to the required interface."""

    def test_hierarchy_functions_exist(self):
        """Test that core hierarchy functions exist."""
        # Check main hierarchy functions
        assert callable(process_structure), (
            "process_structure should be a callable function"
        )
        assert callable(process_record_batch), (
            "process_record_batch should be a callable function"
        )
        assert callable(stream_process_records), (
            "stream_process_records should be a callable function"
        )


class AbstractHierarchyTest:
    """
    Abstract base class for hierarchy function tests.

    This class defines a standardized set of tests that should apply to hierarchy functionality.
    """

    @pytest.fixture
    def simple_table_structure(self):
        """Create a simple data structure for testing hierarchy functions."""
        return {
            "id": "1",
            "name": "Main 1",
            "items": [
                {"id": "101", "name": "Item 1"},
                {"id": "102", "name": "Item 2"},
            ],
        }

    @pytest.fixture
    def complex_table_structure(self):
        """Create a complex data structure with multiple levels for testing hierarchy functions."""
        return {
            "id": "1",
            "name": "Main 1",
            "items": [
                {
                    "id": "101",
                    "name": "Item 1",
                    "subitems": [
                        {"id": "201", "name": "Subitem 1"},
                        {"id": "202", "name": "Subitem 2"},
                    ],
                },
                {
                    "id": "102",
                    "name": "Item 2",
                    "subitems": [{"id": "203", "name": "Subitem 3"}],
                },
            ],
            "tags": [{"id": "301", "name": "Tag 1"}],
        }

    @pytest.fixture
    def deeply_nested_data(self):
        """Create a data structure with deep nesting for max depth testing."""
        result = {"id": "789", "name": "Deeply Nested Structure"}

        # Create a deeply nested structure (10 levels deep)
        current = result
        for i in range(10):
            current["level"] = {"id": f"level-{i}", "name": f"Level {i}"}
            current = current["level"]

        return result

    def test_process_structure(self, simple_table_structure):
        """Test processing a simple structure."""
        # Process structure
        main_record, arrays = process_structure(
            simple_table_structure, entity_name="test"
        )

        # Verify main record is properly processed
        assert "__extract_id" in main_record

        # Verify arrays are extracted
        assert "test_items" in arrays
        assert len(arrays["test_items"]) == 2

        # Check array items have required fields
        for item in arrays["test_items"]:
            assert "__extract_id" in item
            assert "__parent_extract_id" in item
            # Original fields may or may not be included depending on implementation

    def test_process_complex_structure(self, complex_table_structure):
        """Test processing a complex structure with nested arrays."""
        # Process structure
        main_record, arrays = process_structure(
            complex_table_structure, entity_name="test"
        )

        # Verify main table properties
        assert "__extract_id" in main_record

        # Verify arrays are extracted
        assert "test_items" in arrays

        # Get the subitems table name
        subitems_table_names = [name for name in arrays.keys() if "subitems" in name]
        assert len(subitems_table_names) > 0
        subitems_table = subitems_table_names[0]

        assert "test_tags" in arrays

        # Check counts
        assert len(arrays["test_items"]) == 2
        assert len(arrays[subitems_table]) == 3
        assert len(arrays["test_tags"]) == 1

        # Check parent relationships
        main_id = main_record["__extract_id"]

        # Items should have main record as parent
        for item in arrays["test_items"]:
            assert item["__parent_extract_id"] == main_id

        # Tags should have main record as parent
        for tag in arrays["test_tags"]:
            assert tag["__parent_extract_id"] == main_id

        # Subitems should have items as parents
        for subitem in arrays[subitems_table]:
            parent_id = subitem["__parent_extract_id"]
            assert any(
                item["__extract_id"] == parent_id for item in arrays["test_items"]
            )

    def test_process_record_batch(self, simple_table_structure):
        """Test processing a batch of records."""
        # Create a batch of records
        batch = [simple_table_structure for _ in range(3)]

        # Process batch
        main_records, arrays = process_record_batch(batch, entity_name="test")

        # Verify main records
        assert len(main_records) == 3
        for record in main_records:
            assert "__extract_id" in record

        # Verify arrays
        assert "test_items" in arrays
        assert len(arrays["test_items"]) == 6  # 2 items per record * 3 records

    def test_stream_process_records(self, complex_table_structure):
        """Test streaming processing of records."""
        # Create a batch of records
        batch = [complex_table_structure for _ in range(2)]

        # Process records in streaming mode
        main_records, child_tables_generator = stream_process_records(
            batch, entity_name="test"
        )

        # Verify main records
        assert len(main_records) == 2
        for record in main_records:
            assert "__extract_id" in record

        # Convert generator to tables
        child_tables = {}
        for table_name, records in child_tables_generator:
            if table_name not in child_tables:
                child_tables[table_name] = []
            child_tables[table_name].extend(records)

        # Verify child tables
        assert "test_items" in child_tables

        # Get the subitems table name
        subitems_table_names = [
            name for name in child_tables.keys() if "subitems" in name
        ]
        assert len(subitems_table_names) > 0
        subitems_table = subitems_table_names[0]

        assert "test_tags" in child_tables

        # Check counts
        assert len(child_tables["test_items"]) == 4  # 2 items per record * 2 records
        assert (
            len(child_tables[subitems_table]) == 6
        )  # 3 subitems per record * 2 records
        assert len(child_tables["test_tags"]) == 2  # 1 tag per record * 2 records

    def test_max_depth_handling(self, deeply_nested_data):
        """Test handling of deeply nested structures."""
        # Process with default settings
        main_record, arrays = process_structure(deeply_nested_data, entity_name="test")

        # Verify it processes without errors
        assert "__extract_id" in main_record

        # Test with limited max_depth
        main_record_limited, arrays_limited = process_structure(
            deeply_nested_data,
            entity_name="test",
            max_depth=3,  # Limit recursion depth
        )

        # It should still complete without errors
        assert "__extract_id" in main_record_limited
