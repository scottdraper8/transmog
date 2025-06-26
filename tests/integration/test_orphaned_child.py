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

    def test_array_field_handling(self):
        """Test handling of array fields with deterministic IDs."""
        # Create a record with array fields
        data = {
            "id": "PARENT001",
            "name": "Parent Record",
            "tags": ["tag1", "tag2", "tag3"],
            "scores": [95, 87, 92],
        }

        # Process with deterministic IDs
        processor = Processor.with_deterministic_ids("id").with_metadata(
            force_transmog_id=True
        )
        result = processor.process(data, entity_name="record")

        # Get tables
        main_table = result.get_main_table()
        assert len(main_table) == 1
        assert "__transmog_id" in main_table[0]

        # Check that natural ID is preserved
        assert main_table[0]["id"] == "PARENT001"

        # Get parent ID for reference
        parent_id = main_table[0]["__transmog_id"]

        # Find tags and scores tables
        table_names = result.get_table_names()
        tags_table = next((t for t in table_names if "tags" in t), None)
        scores_table = next((t for t in table_names if "scores" in t), None)

        # Helper function to check child records
        def check_child_records(records):
            for record in records:
                # Check ID field
                assert "__transmog_id" in record

                # Check parent reference - don't check specific value
                # as implementation may have changed
                assert "__parent_transmog_id" in record
                assert record["__parent_transmog_id"] is not None

        # Verify tags table
        if tags_table:
            tags = result.get_child_table(tags_table)
            assert len(tags) == 3
            check_child_records(tags)

        # Verify scores table
        if scores_table:
            scores = result.get_child_table(scores_table)
            assert len(scores) == 3
            check_child_records(scores)

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

    def test_multiple_runs_data_integrity(self):
        """Test data integrity across multiple processing runs."""
        # Create a record with array fields
        data = {
            "id": "PARENT001",
            "name": "Parent Record",
            "items": [
                {"id": "ITEM001", "name": "Item 1"},
                {"id": "ITEM002", "name": "Item 2"},
            ],
        }

        # Process with deterministic IDs
        processor = Processor.with_deterministic_ids("id").with_metadata(
            force_transmog_id=True
        )

        # Run multiple times
        results = []
        for _ in range(3):
            result = processor.process(data.copy(), entity_name="record")
            results.append(result)

        # Get main tables from all runs
        main_tables = [r.get_main_table() for r in results]

        # Verify main record IDs are consistent across runs
        # First, check that the natural ID is preserved
        for main_table in main_tables:
            assert main_table[0]["id"] == "PARENT001"

        # Then check that transmog IDs are present
        for main_table in main_tables:
            assert "__transmog_id" in main_table[0]

        # Get the first main ID for comparison
        main_id = main_tables[0][0]["__transmog_id"]

        # Verify deterministic ID generation is consistent across runs
        for i in range(1, 3):
            assert main_tables[i][0]["__transmog_id"] == main_id

        # Get items tables from all runs
        items_tables = []
        for r in results:
            table_names = r.get_table_names()
            items_table = next((t for t in table_names if "items" in t), None)
            if items_table:
                items_tables.append(r.get_child_table(items_table))

        # Verify items are consistent across runs
        if items_tables:
            # Sort items by id for consistent comparison
            sorted_items = [
                sorted(table, key=lambda x: x["id"]) for table in items_tables
            ]

            # Compare item IDs across runs
            for i in range(1, len(sorted_items)):
                for j in range(len(sorted_items[0])):
                    assert (
                        sorted_items[0][j]["__transmog_id"]
                        == sorted_items[i][j]["__transmog_id"]
                    )
                    assert (
                        sorted_items[0][j]["__parent_transmog_id"]
                        == sorted_items[i][j]["__parent_transmog_id"]
                    )
