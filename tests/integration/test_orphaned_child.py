"""
Integration tests for orphaned child records handling.

These tests verify that the library correctly handles orphaned child records
when processing data with parent-child relationships.
"""

import copy
import os
from typing import Any

import pytest

from transmog import Processor


class TestOrphanedChild:
    """Integration tests for orphaned child records handling."""

    @pytest.fixture
    def orphaned_child_data(self) -> dict[str, Any]:
        """Return data with orphaned child records for testing."""
        return {
            "id": "PARENT001",
            "name": "Parent Record",
            "children": [
                {"id": "CHILD001", "name": "Child 1", "value": 100},
                {"id": "CHILD002", "name": "Child 2", "value": 200},
            ],
        }

    def test_array_field_handling(self, orphaned_child_data, tmp_path):
        """Test that array fields are properly handled during processing."""
        # Create a processor
        processor = Processor.default()

        # Process the data multiple times using deep copies
        results = []
        for _ in range(2):
            # Create a deep copy to ensure we're not modifying the original
            data_copy = copy.deepcopy(orphaned_child_data)
            result = processor.process(data_copy, entity_name="parent")
            results.append(result)

        # Get the tables from both runs
        main_tables = [result.get_main_table() for result in results]
        children_tables = [
            result.get_child_table("parent_children") for result in results
        ]

        # Verify that both runs have the same number of records
        assert len(main_tables[0]) == len(main_tables[1])
        assert len(children_tables[0]) == len(children_tables[1])

        # Verify children reference correct parent in both runs
        for run_idx in range(2):
            parent_id = main_tables[run_idx][0]["__extract_id"]

            for child in children_tables[run_idx]:
                assert child["__parent_extract_id"] == parent_id

    def test_parent_removal_effect(self, orphaned_child_data, tmp_path):
        """Test behavior when parent records are removed but children remain."""
        # Set up output directories
        output_dir = os.path.join(tmp_path, "orphaned_output")
        os.makedirs(output_dir, exist_ok=True)

        # Create a processor
        processor = Processor.default()

        # First, process the complete data
        complete_data = copy.deepcopy(orphaned_child_data)
        complete_result = processor.process(complete_data, entity_name="parent")

        # Now process modified data with parent but no children
        parent_only_data = copy.deepcopy(orphaned_child_data)
        parent_only_data.pop("children")
        parent_result = processor.process(parent_only_data, entity_name="parent")

        # Now process modified data with children but empty parent
        # This simulates a case where parent data is missing or null
        children_only_data = {
            "id": "PARENT001",  # Keep ID for linking
            "name": None,  # Missing data
            "children": copy.deepcopy(orphaned_child_data["children"]),
        }
        children_result = processor.process(children_only_data, entity_name="parent")

        # Verify parent records exist in all results
        assert len(complete_result.get_main_table()) == 1
        assert len(parent_result.get_main_table()) == 1
        assert len(children_result.get_main_table()) == 1

        # Parent-only result should have no children
        assert "parent_children" not in parent_result.get_table_names()

        # Children-only and complete results should have the same number of children
        complete_children = complete_result.get_child_table("parent_children")
        children_only_children = children_result.get_child_table("parent_children")
        assert len(complete_children) == len(children_only_children)

        # Sort children by ID for comparison
        complete_children_sorted = sorted(complete_children, key=lambda x: x["id"])
        children_only_sorted = sorted(children_only_children, key=lambda x: x["id"])

        # Verify children have the same data in both results
        for i in range(len(complete_children_sorted)):
            assert complete_children_sorted[i]["id"] == children_only_sorted[i]["id"]
            assert (
                complete_children_sorted[i]["name"] == children_only_sorted[i]["name"]
            )
            assert (
                complete_children_sorted[i]["value"] == children_only_sorted[i]["value"]
            )

    def test_multiple_runs_data_integrity(self, orphaned_child_data, tmp_path):
        """Test data integrity across multiple processing runs."""
        # Create a processor
        processor = Processor.default()

        # Process the data multiple times
        for run_idx in range(3):
            # Create a deep copy to ensure we're not modifying the original
            data_copy = copy.deepcopy(orphaned_child_data)

            # Process the data
            result = processor.process(data_copy, entity_name="parent")

            # Verify the original data is still intact after processing
            assert "children" in data_copy
            assert len(data_copy["children"]) == 2
            assert data_copy["children"][0]["id"] == "CHILD001"
            assert data_copy["children"][1]["id"] == "CHILD002"

            # Write to output directory for this run
            run_dir = os.path.join(tmp_path, f"run{run_idx + 1}")
            os.makedirs(run_dir, exist_ok=True)

            # Write tables to JSON
            if hasattr(result, "write_all_json"):
                result.write_all_json(base_path=run_dir)

            # Verify the result tables
            main_table = result.get_main_table()
            children_table = result.get_child_table("parent_children")

            assert len(main_table) == 1
            assert len(children_table) == 2

            # Verify parent-child relationships
            parent_id = main_table[0]["__extract_id"]
            for child in children_table:
                assert child["__parent_extract_id"] == parent_id
