"""
Unit tests for the core streaming functions in Transmog.

These tests focus on the low-level streaming functions that power the processor's streaming capabilities:
- stream_process_records
- stream_process_structure
- flatten_json with mode="streaming"
- stream_extract_arrays
"""

import pytest
from typing import Dict, List, Any, Generator, Tuple

from transmog.core.hierarchy import (
    stream_process_records,
    stream_process_structure,
)
from transmog.core.flattener import flatten_json
from transmog.core.extractor import stream_extract_arrays


class TestCoreStreaming:
    """Test the core streaming functions in Transmog."""

    def test_stream_flatten_json(self):
        """Test flatten_json function in streaming mode."""
        # Create test data
        data = {
            "id": 123,
            "name": "Test",
            "address": {"street": "123 Main St", "city": "Anytown", "zip": "12345"},
        }

        # Call the function with streaming mode and abbreviation disabled
        result = flatten_json(
            data,
            separator="_",
            cast_to_string=False,
            abbreviate_field_names=False,
            mode="streaming",
        )

        # Verify flattened structure
        assert "id" in result
        assert "name" in result
        assert "address_street" in result
        assert "address_city" in result
        assert "address_zip" in result

        # Verify values
        assert result["id"] == 123
        assert result["name"] == "Test"
        assert result["address_street"] == "123 Main St"
        assert result["address_city"] == "Anytown"
        assert result["address_zip"] == "12345"

    def test_stream_flatten_json_with_options(self):
        """Test flatten_json in streaming mode with different options."""
        # Create test data with null and empty values
        data = {
            "id": 123,
            "name": "",  # empty string
            "description": None,  # null value
            "metadata": {
                "created": "2023-01-01",
                "modified": None,  # nested null
                "tags": [],  # empty array
            },
        }

        # Test with skip_null=True (default), abbreviation disabled
        result1 = flatten_json(
            data,
            separator="_",
            cast_to_string=False,
            skip_null=True,
            abbreviate_field_names=False,
            mode="streaming",
        )

        # Null values should be skipped
        assert "description" not in result1
        assert "metadata_modified" not in result1

        # Non-null values should be included
        assert "id" in result1
        # Empty strings are skipped by default
        assert "name" not in result1

        # Test with skip_null=False and cast_to_string=True, abbreviation disabled
        # When skip_null=False and cast_to_string=True, null values are converted to empty strings
        result2 = flatten_json(
            data,
            separator="_",
            cast_to_string=True,
            skip_null=False,
            abbreviate_field_names=False,
            mode="streaming",
        )

        # Now null values should be included but converted to empty strings
        assert "description" in result2
        assert result2["description"] == ""
        assert "metadata_modified" in result2
        assert result2["metadata_modified"] == ""

        # Test with include_empty=True to include empty strings
        result3 = flatten_json(
            data,
            separator="_",
            cast_to_string=False,
            include_empty=True,
            abbreviate_field_names=False,
            mode="streaming",
        )

        # Now empty strings should be included
        assert "name" in result3
        assert result3["name"] == ""

        # Test with custom separator
        result4 = flatten_json(
            data,
            separator=".",
            cast_to_string=False,
            skip_null=True,
            abbreviate_field_names=False,
            mode="streaming",
        )

        # Verify separator was used
        assert "metadata.created" in result4
        assert result4["metadata.created"] == "2023-01-01"
        # Empty arrays are included (not skipped) by default
        assert "metadata.tags" in result4
        assert result4["metadata.tags"] == []

        # Test with cast_to_string=True
        result5 = flatten_json(
            data,
            separator="_",
            cast_to_string=True,
            abbreviate_field_names=False,
            mode="streaming",
        )

        # Values should be converted to strings
        assert isinstance(result5["id"], str)
        assert result5["id"] == "123"

    def test_stream_extract_arrays(self):
        """Test stream_extract_arrays function."""
        # Create test data with nested arrays
        data = {
            "id": "parent",
            "items": [{"id": "item1", "value": 10}, {"id": "item2", "value": 20}],
            "metadata": {
                "tags": [
                    {"name": "tag1", "priority": 1},
                    {"name": "tag2", "priority": 2},
                ]
            },
        }

        # Call the streaming function
        parent_id = "parent-123"
        arrays_gen = stream_extract_arrays(
            data, parent_id=parent_id, separator="_", cast_to_string=False
        )

        # Convert generator to list for testing
        arrays = list(arrays_gen)

        # In the current implementation, each array item is yielded individually
        # So we expect 4 items total (2 items + 2 tags)
        assert len(arrays) == 4

        # Group records by table name
        items_records = [
            record for table_name, record in arrays if "items" in table_name
        ]
        tags_records = [record for table_name, record in arrays if "tags" in table_name]

        # Verify we have the expected number of records for each table
        assert len(items_records) == 2
        assert len(tags_records) == 2

        # Verify items records
        item1 = next((r for r in items_records if r["id"] == "item1"), None)
        item2 = next((r for r in items_records if r["id"] == "item2"), None)
        assert item1 is not None
        assert item2 is not None
        assert item1["value"] == 10
        assert item2["value"] == 20

        # Verify tags records
        tag1 = next((r for r in tags_records if r["name"] == "tag1"), None)
        tag2 = next((r for r in tags_records if r["name"] == "tag2"), None)
        assert tag1 is not None
        assert tag2 is not None
        assert tag1["priority"] == 1
        assert tag2["priority"] == 2

        # Verify parent ID is included
        for record in items_records + tags_records:
            assert "__parent_extract_id" in record
            assert record["__parent_extract_id"] == parent_id

    def test_stream_process_structure(self):
        """Test stream_process_structure function."""
        # Create test data
        data = {
            "id": "main-record",
            "name": "Main Record",
            "items": [
                {"id": "item1", "name": "Item 1"},
                {"id": "item2", "name": "Item 2"},
            ],
        }

        # Call the streaming function
        extract_time = "2023-01-01"
        entity_name = "test"
        result, child_tables_gen = stream_process_structure(
            data, entity_name=entity_name, extract_time=extract_time, visit_arrays=True
        )

        # Verify main record
        assert "id" in result
        assert result["id"] == "main-record"
        assert result["name"] == "Main Record"
        assert "__extract_id" in result
        assert "__extract_datetime" in result
        assert result["__extract_datetime"] == extract_time

        # Verify child tables generator
        child_tables = list(child_tables_gen)
        # Current implementation yields individual records instead of grouped records
        # Two items in the items array = two child table entries
        assert len(child_tables) == 2

        # Debug - print actual table names
        for table_name, _ in child_tables:
            print(f"Table name: {table_name}")

        # All records should be for the 'test_items' table
        assert all(table_name == "test_items" for table_name, _ in child_tables)

        # Extract records from child_tables
        records = [record for _, record in child_tables]

        # Verify we have the right number of records
        assert len(records) == 2

        # Find individual records
        item1 = next((r for r in records if r["id"] == "item1"), None)
        item2 = next((r for r in records if r["id"] == "item2"), None)

        # Verify child records
        assert item1 is not None
        assert item1["name"] == "Item 1"
        assert item2 is not None
        assert item2["name"] == "Item 2"

        # Verify parent-child relationship
        for record in records:
            assert "__extract_id" in record
            assert "__parent_extract_id" in record
            assert record["__parent_extract_id"] == result["__extract_id"]
            assert "__extract_datetime" in record
            assert record["__extract_datetime"] == extract_time

    def test_stream_process_records(self):
        """Test stream_process_records function with a batch of records."""
        # Create test data
        data = [
            {"id": 1, "name": "Record 1", "tags": [{"name": "tag1"}, {"name": "tag2"}]},
            {"id": 2, "name": "Record 2", "tags": [{"name": "tag3"}]},
        ]

        # Call the streaming function
        extract_time = "2023-01-01"
        entity_name = "records"
        main_records, child_tables_gen = stream_process_records(
            data, entity_name=entity_name, extract_time=extract_time, visit_arrays=True
        )

        # Verify main records
        assert len(main_records) == 2
        assert main_records[0]["id"] == "1"
        assert main_records[1]["id"] == "2"

        # Each record should have an extract ID and timestamp
        for record in main_records:
            assert "__extract_id" in record
            assert "__extract_datetime" in record
            assert record["__extract_datetime"] == extract_time

        # Convert child tables generator to a list for testing
        child_tables = list(child_tables_gen)

        # Should have 1 child table (tags)
        assert len(child_tables) == 1

        # Check child table
        table_name, records = child_tables[0]
        assert table_name == f"{entity_name}_tags"
        assert len(records) == 3  # Total of 3 tags from both records

        # Count tags per parent
        parent_1_tags = [
            r
            for r in records
            if r["__parent_extract_id"] == main_records[0]["__extract_id"]
        ]
        parent_2_tags = [
            r
            for r in records
            if r["__parent_extract_id"] == main_records[1]["__extract_id"]
        ]

        assert len(parent_1_tags) == 2  # First record has 2 tags
        assert len(parent_2_tags) == 1  # Second record has 1 tag

        # Verify tag content
        assert any(t["name"] == "tag1" for t in parent_1_tags)
        assert any(t["name"] == "tag2" for t in parent_1_tags)
        assert any(t["name"] == "tag3" for t in parent_2_tags)

    def test_stream_process_records_with_options(self):
        """Test stream_process_records with different options."""
        # Create test data with nested structures
        data = [
            {
                "id": 1,
                "address": {"city": "New York", "zip": "10001"},
                "contact": [{"type": "email", "value": "test@example.com"}],
            },
            {
                "id": 2,
                "address": {"city": "Boston", "zip": "02108"},
                "contact": [{"type": "phone", "value": "555-1234"}],
            },
        ]

        # Call with custom separator and no array processing
        # Explicitly disable abbreviation to see the full path
        main_records_1, child_tables_gen_1 = stream_process_records(
            data,
            entity_name="custom",
            separator=".",
            visit_arrays=False,
            abbreviate_field_names=False,
            abbreviate_table_names=False,
        )

        # Verify flattened structure uses custom separator
        assert "address.city" in main_records_1[0]
        assert "address.zip" in main_records_1[0]

        # No child tables when visit_arrays=False
        child_tables_1 = list(child_tables_gen_1)
        assert len(child_tables_1) == 0

        # Call with abbreviation enabled
        main_records_2, child_tables_gen_2 = stream_process_records(
            data, entity_name="abbrev", abbreviate_field_names=True, visit_arrays=True
        )

        # Fields should be abbreviated
        assert "addr_city" in main_records_2[0] or "add_city" in main_records_2[0]

        # Child tables should be generated
        child_tables_2 = list(child_tables_gen_2)
        assert len(child_tables_2) > 0

    def test_stream_process_with_deterministic_ids(self):
        """Test streaming process with deterministic IDs."""
        # Create test data
        data = [
            {
                "id": "STABLE-1",
                "name": "Record 1",
                "items": [{"item_id": "ITEM-1", "value": 100}],
            },
            {
                "id": "STABLE-2",
                "name": "Record 2",
                "items": [{"item_id": "ITEM-2", "value": 200}],
            },
        ]

        # Process with deterministic IDs - set a specific extract time to ensure deterministic behavior
        extract_time = "2023-01-01T00:00:00Z"
        main_records, child_tables_gen = stream_process_records(
            data,
            entity_name="test",
            extract_time=extract_time,
            deterministic_id_fields={
                "": "id",  # Root level uses id field
                "items": "item_id",  # Items use item_id field
            },
            visit_arrays=True,
        )

        # Verify that main records are processed correctly
        assert len(main_records) == 2
        assert main_records[0]["id"] == "STABLE-1"
        assert main_records[1]["id"] == "STABLE-2"

        # Verify that each main record has an extract ID
        assert "__extract_id" in main_records[0]
        assert "__extract_id" in main_records[1]

        # Track which child records we've seen
        seen_child_items = set()

        # Process child tables
        for table_name, records in child_tables_gen:
            # The table name should match our entity name
            assert table_name == "test_items"

            # Process records
            for record in records:
                # Each record should have extract ID and parent ID
                assert "__extract_id" in record
                assert "__parent_extract_id" in record

                # Track the itemid to ensure we see all expected records
                seen_child_items.add(record["itemid"])

        # Verify we saw all expected child records
        assert seen_child_items == {"ITEM-1", "ITEM-2"}
