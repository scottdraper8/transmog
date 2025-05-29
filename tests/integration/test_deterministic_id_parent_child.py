"""
Integration tests for deterministic ID generation with parent-child relationships.

These tests verify that deterministic IDs work correctly across multiple
processing runs and maintain consistent parent-child relationships.
"""

import copy
from typing import Any

import pytest

from transmog import Processor
from transmog.core.metadata import generate_deterministic_id


class TestDeterministicIdParentChild:
    """Integration tests for deterministic ID generation with parent-child relationships."""

    @pytest.fixture
    def nested_parent_child_data(self) -> dict[str, Any]:
        """Return nested data with parent-child relationships for testing."""
        return {
            "id": "PARENT001",
            "name": "Parent Record",
            "metadata": {
                "created_at": "2023-01-01",
                "status": "active",
            },
            "children": [
                {"id": "CHILD001", "name": "Child 1", "value": 100},
                {
                    "id": "CHILD002",
                    "name": "Child 2",
                    "value": 200,
                    "grandchildren": [
                        {"id": "GRANDCHILD001", "name": "Grandchild 1"},
                        {"id": "GRANDCHILD002", "name": "Grandchild 2"},
                    ],
                },
            ],
        }

    def test_consistent_parent_child_ids(self, nested_parent_child_data, tmp_path):
        """Test that parent-child relationships are maintained with deterministic IDs."""
        # Create a processor with deterministic IDs
        processor = Processor.with_deterministic_ids("id")

        # Process the data multiple times
        results = []
        for _ in range(2):
            # Create a deep copy to ensure we're not modifying the original
            data_copy = copy.deepcopy(nested_parent_child_data)
            result = processor.process(data_copy, entity_name="family")
            results.append(result)

        # Get the tables from both runs
        main_tables = [result.get_main_table() for result in results]
        children_tables = [
            result.get_child_table("family_children") for result in results
        ]
        grandchildren_tables = [
            result.get_child_table("family_children_grandchildren")
            for result in results
        ]

        # Verify deterministic IDs are consistent across runs
        assert main_tables[0][0]["__extract_id"] == main_tables[1][0]["__extract_id"]

        # Sort children by id to ensure consistent ordering
        children_tables = [
            sorted(table, key=lambda x: x["id"]) for table in children_tables
        ]

        # Verify all children have consistent IDs
        for i in range(len(children_tables[0])):
            assert (
                children_tables[0][i]["__extract_id"]
                == children_tables[1][i]["__extract_id"]
            )

        # Sort grandchildren by id
        grandchildren_tables = [
            sorted(table, key=lambda x: x["id"]) for table in grandchildren_tables
        ]

        # Verify all grandchildren have consistent IDs
        for i in range(len(grandchildren_tables[0])):
            assert (
                grandchildren_tables[0][i]["__extract_id"]
                == grandchildren_tables[1][i]["__extract_id"]
            )

        # Verify parent-child relationships are maintained
        for run_idx in range(2):
            parent_id = main_tables[run_idx][0]["__extract_id"]

            # All children should reference the parent
            for child in children_tables[run_idx]:
                assert child["__parent_extract_id"] == parent_id

            # Find Child 2 to check grandchildren relationships
            child2 = next(c for c in children_tables[run_idx] if c["id"] == "CHILD002")
            child2_id = child2["__extract_id"]

            # All grandchildren should reference Child 2
            for grandchild in grandchildren_tables[run_idx]:
                assert grandchild["__parent_extract_id"] == child2_id

    def test_complex_deterministic_id_consistency(self, tmp_path):
        """Test deterministic ID consistency with complex nested structures."""
        # Create complex nested data
        data = {
            "id": "COMPLEX001",
            "details": {"code": "ABC123", "description": "Complex test record"},
            "items": [
                {
                    "id": "ITEM001",
                    "name": "Item 1",
                    "subitems": [
                        {"id": "SUBITEM001", "value": 10},
                        {"id": "SUBITEM002", "value": 20},
                    ],
                },
                {
                    "id": "ITEM002",
                    "name": "Item 2",
                    "subitems": [
                        {"id": "SUBITEM003", "value": 30},
                        {"id": "SUBITEM004", "value": 40},
                    ],
                },
            ],
        }

        # Create processor with deterministic IDs
        processor = Processor.with_deterministic_ids("id")

        # Process the data twice
        results = []
        for _ in range(2):
            data_copy = copy.deepcopy(data)
            result = processor.process(data_copy, entity_name="complex")
            results.append(result)

        # Check consistency across runs
        # Get table names from both runs
        run1_tables = results[0].get_table_names()
        run2_tables = results[1].get_table_names()

        # Verify table names are the same
        assert set(run1_tables) == set(run2_tables)

        # Helper function to sort and compare tables
        def compare_tables(table1, table2, key_field="id"):
            sorted1 = sorted(table1, key=lambda x: x.get(key_field, ""))
            sorted2 = sorted(table2, key=lambda x: x.get(key_field, ""))

            assert len(sorted1) == len(sorted2)

            for i in range(len(sorted1)):
                assert sorted1[i]["__extract_id"] == sorted2[i]["__extract_id"]

                # If this record has a parent, check parent reference consistency
                if "__parent_extract_id" in sorted1[i]:
                    assert (
                        sorted1[i]["__parent_extract_id"]
                        == sorted2[i]["__parent_extract_id"]
                    )

        # Compare main tables
        compare_tables(results[0].get_main_table(), results[1].get_main_table())

        # Compare each child table
        for table_name in run1_tables:
            if table_name == "main":
                continue

            child_table1 = results[0].get_child_table(table_name)
            child_table2 = results[1].get_child_table(table_name)
            compare_tables(child_table1, child_table2)
