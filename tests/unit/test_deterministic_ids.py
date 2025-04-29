"""
Tests for deterministic ID generation functionality.
"""

import uuid
import pytest

from transmog import Processor, TransmogConfig
from transmog.core.metadata import (
    generate_extract_id,
    generate_deterministic_id,
    generate_composite_id,
    TRANSMOG_NAMESPACE,
)


class TestDeterministicIds:
    """Tests for deterministic ID generation."""

    def test_deterministic_id_generation(self):
        """Test that deterministic ID generation produces consistent IDs."""
        # Generate IDs for the same value multiple times
        value = "test_value"
        id1 = generate_deterministic_id(value)
        id2 = generate_deterministic_id(value)
        id3 = generate_deterministic_id(value)

        # Verify they're all the same
        assert id1 == id2 == id3

        # Verify format is a UUID string
        uuid_obj = uuid.UUID(id1)
        assert str(uuid_obj) == id1

        # Verify it's a UUID5 (SHA1-based UUID)
        assert uuid_obj.version == 5

        # Verify it matches direct UUID5 generation
        expected = str(uuid.uuid5(TRANSMOG_NAMESPACE, value))
        assert id1 == expected

    def test_different_values_produce_different_ids(self):
        """Test that different values produce different IDs."""
        id1 = generate_deterministic_id("value1")
        id2 = generate_deterministic_id("value2")
        assert id1 != id2

    def test_composite_id_generation(self):
        """Test composite ID generation with multiple fields."""
        record = {"id": 123, "name": "test", "date": "2023-01-01"}

        # Generate composite ID with multiple fields
        id1 = generate_composite_id(record, ["id", "name"])
        id2 = generate_composite_id(record, ["id", "name"])

        # Verify they're the same
        assert id1 == id2

        # Different field combinations should produce different IDs
        id3 = generate_composite_id(record, ["id", "date"])
        assert id1 != id3

    def test_extract_id_with_source_field(self):
        """Test generate_extract_id with a source field."""
        record = {"id": "ABC123", "name": "Test Record"}

        # Random ID (default behavior)
        random_id = generate_extract_id()

        # Deterministic ID with source field
        deterministic_id = generate_extract_id(record, source_field="id")
        deterministic_id2 = generate_extract_id(record, source_field="id")

        # Verify random IDs are different
        assert random_id != deterministic_id

        # Verify deterministic IDs are the same
        assert deterministic_id == deterministic_id2

        # Verify it matches direct deterministic ID generation
        expected = generate_deterministic_id(record["id"])
        assert deterministic_id == expected

    def test_custom_id_generation_strategy(self):
        """Test custom ID generation strategy function."""
        record = {"id": 123, "name": "test"}

        # Define a custom ID generation strategy
        def custom_strategy(rec):
            return f"CUSTOM-{rec['id']}-{rec['name']}"

        # Generate ID with custom strategy
        custom_id = generate_extract_id(record, id_generation_strategy=custom_strategy)

        # Verify result
        assert custom_id == "CUSTOM-123-test"

    def test_processor_with_deterministic_ids(self):
        """Test processor with deterministic ID generation."""
        # Create test data
        data = {
            "id": "ROOT123",
            "name": "Root Record",
            "nested": {"id": "NESTED456", "value": "Nested Value"},
            "items": [
                {"id": "ITEM1", "name": "Item 1"},
                {"id": "ITEM2", "name": "Item 2"},
            ],
        }

        # Create processor with deterministic ID fields
        deterministic_id_fields = {
            "": "id",  # Root level
            "nested": "id",  # Nested object
            "items": "id",  # Items array
        }
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields=deterministic_id_fields
        )
        processor = Processor(config=config)

        # Process data twice
        result1 = processor.process(data, entity_name="test")
        result2 = processor.process(data, entity_name="test")

        # Get results as dictionaries
        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Each table is a list of records
        main_table1 = tables1["main"]
        main_table2 = tables2["main"]

        # Verify we have records
        assert len(main_table1) > 0
        assert len(main_table2) > 0

        # The IDs should be the same in both runs (first record in each table)
        assert main_table1[0]["__extract_id"] == main_table2[0]["__extract_id"]

        # The nested array IDs should also be the same
        if "test_items" in tables1 and "test_items" in tables2:
            items1 = sorted(tables1["test_items"], key=lambda x: x["id"])
            items2 = sorted(tables2["test_items"], key=lambda x: x["id"])

            for i in range(len(items1)):
                assert items1[i]["__extract_id"] == items2[i]["__extract_id"]

    def test_processor_with_custom_id_strategy(self):
        """Test processor with custom ID generation strategy."""
        # Create test data
        data = {"id": 123, "name": "test"}

        # Define custom ID strategy
        def custom_strategy(record):
            return f"CUSTOM-{record.get('id', 'UNKNOWN')}"

        # Create processor with custom strategy
        config = TransmogConfig.default().with_metadata(
            id_generation_strategy=custom_strategy
        )
        processor = Processor(config=config)

        # Process data
        result = processor.process(data, entity_name="test")

        # Get result as dictionary
        tables = result.to_dict()

        # Each table is a list of records
        main_table = tables["main"]

        # Verify we have at least one record
        assert len(main_table) > 0

        # Verify ID follows custom strategy
        assert main_table[0]["__extract_id"] == "CUSTOM-123"

    # New tests for path matching, edge cases, and integration

    def test_path_wildcard_matching(self):
        """Test wildcard path matching for deterministic ID fields."""
        # Create test data with multiple levels of nesting
        data = {
            "id": "ROOT123",
            "items": [
                {"id": "ITEM1", "name": "Item 1"},
                {"id": "ITEM2", "name": "Item 2"},
            ],
            "categories": [
                {
                    "id": "CAT1",
                    "name": "Category 1",
                    "subcategories": [
                        {"id": "SUBCAT1", "name": "SubCategory 1"},
                    ],
                },
            ],
        }

        # Create processor with wildcard path matching
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields={
                "*": "id",  # Match any path
            }
        )
        processor = Processor(config=config)

        # Process data twice to verify ID stability
        result1 = processor.process(data, entity_name="test")
        result2 = processor.process(data, entity_name="test")

        # Get results as dictionaries
        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Verify main table IDs are deterministic
        assert tables1["main"][0]["__extract_id"] == tables2["main"][0]["__extract_id"]

        # If items and categories were extracted as child tables, verify their IDs
        for table_name in tables1:
            if table_name in tables2 and table_name != "main":
                # Sort records by id to ensure matching order
                records1 = sorted(tables1[table_name], key=lambda x: x.get("id", ""))
                records2 = sorted(tables2[table_name], key=lambda x: x.get("id", ""))

                # Compare extract IDs for each record
                for i in range(min(len(records1), len(records2))):
                    assert records1[i]["__extract_id"] == records2[i]["__extract_id"]

    def test_path_prefix_matching(self):
        """Test path prefix matching for deterministic ID fields."""
        # Create test data with multiple levels of nesting
        data = {
            "id": "ROOT123",
            "details": {
                "code": "DETAIL100",
                "type": "main",
                "attributes": {
                    "color": "blue",
                    "size": "large",
                    "tags": [
                        {"id": "TAG1", "value": "important"},
                        {"id": "TAG2", "value": "urgent"},
                    ],
                },
            },
        }

        # Create processor with prefix path matching
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields={
                "": "id",  # Root level uses id
                "details_attributes_*": "code",  # Any path under details_attributes uses code
            }
        )
        processor = Processor(config=config)

        # Process data twice to verify ID stability
        result1 = processor.process(data, entity_name="test")
        result2 = processor.process(data, entity_name="test")

        # Compare IDs across both processing results
        main1 = result1.get_main_table()
        main2 = result2.get_main_table()

        # Main record ID should be deterministic
        assert main1[0]["__extract_id"] == main2[0]["__extract_id"]

        # Check for tags table if it was extracted
        if "test_details_attributes_tags" in result1.get_table_names():
            tags1 = result1.get_child_table("test_details_attributes_tags")
            tags2 = result2.get_child_table("test_details_attributes_tags")

            # Sort by id for comparison
            tags1 = sorted(tags1, key=lambda x: x.get("id", ""))
            tags2 = sorted(tags2, key=lambda x: x.get("id", ""))

            # Compare tag IDs
            for i in range(min(len(tags1), len(tags2))):
                assert tags1[i]["__extract_id"] == tags2[i]["__extract_id"]

    def test_missing_deterministic_fields(self):
        """Test handling of missing fields for deterministic ID generation."""
        # Create test data with missing id fields
        data = {
            "name": "Root Record",  # No id field
            "items": [
                {"id": "ITEM1", "name": "Item 1"},
                {"name": "Item 2"},  # No id field
            ],
        }

        # Create processor that expects id fields
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields={
                "": "id",  # Root level
                "items": "id",  # Items array
            }
        )
        processor = Processor(config=config)

        # Process the data
        result = processor.process(data, entity_name="test")

        # Get tables
        tables = result.to_dict()
        main_table = tables["main"]

        # Root record should have an ID even though the source field is missing
        assert "__extract_id" in main_table[0]

        # If items were extracted to their own table, check them too
        if "test_items" in tables:
            items = tables["test_items"]
            assert len(items) > 0

            # Each item should have an extract ID
            for item in items:
                assert "__extract_id" in item

            # Items with the same id field should have the same extract ID
            # when processed twice
            result2 = processor.process(data, entity_name="test")
            tables2 = result2.to_dict()

            if "test_items" in tables2:
                items2 = tables2["test_items"]

                # Sort by name for matching
                items_sorted = sorted(items, key=lambda x: x.get("name", ""))
                items2_sorted = sorted(items2, key=lambda x: x.get("name", ""))

                # Item with ID should have consistent extract ID
                for i in range(len(items_sorted)):
                    if "id" in items_sorted[i] and "id" in items2_sorted[i]:
                        assert (
                            items_sorted[i]["__extract_id"]
                            == items2_sorted[i]["__extract_id"]
                        )

    def test_integration_mixed_id_strategies(self):
        """Integration test with mixed deterministic and random ID strategies."""
        # Create complex test data
        data = {
            "id": "ROOT123",
            "name": "Root Record",
            "status": "active",
            "regular_items": [  # These will use random IDs
                {"name": "Regular Item 1"},
                {"name": "Regular Item 2"},
            ],
            "tracked_items": [  # These will use deterministic IDs
                {"id": "TRACKED1", "name": "Tracked Item 1"},
                {"id": "TRACKED2", "name": "Tracked Item 2"},
            ],
        }

        # Create processor with selective deterministic ID fields
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields={
                "": "id",  # Root level uses id field
                "tracked_items": "id",  # Only tracked_items use deterministic IDs
            }
        )
        processor = Processor(config=config)

        # Process data twice
        result1 = processor.process(data, entity_name="mixed")
        result2 = processor.process(data, entity_name="mixed")

        # Get tables
        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Main record should have consistent ID
        assert tables1["main"][0]["__extract_id"] == tables2["main"][0]["__extract_id"]

        # Regular items should have different IDs
        if "mixed_regular_items" in tables1 and "mixed_regular_items" in tables2:
            reg_items1 = sorted(
                tables1["mixed_regular_items"], key=lambda x: x.get("name", "")
            )
            reg_items2 = sorted(
                tables2["mixed_regular_items"], key=lambda x: x.get("name", "")
            )

            if len(reg_items1) > 0 and len(reg_items2) > 0:
                # Some regular item IDs should differ
                any_different = False
                for i in range(min(len(reg_items1), len(reg_items2))):
                    if reg_items1[i]["__extract_id"] != reg_items2[i]["__extract_id"]:
                        any_different = True
                        break

                assert any_different, "Expected some random IDs to differ"

        # Tracked items should have consistent IDs
        if "mixed_tracked_items" in tables1 and "mixed_tracked_items" in tables2:
            track_items1 = sorted(
                tables1["mixed_tracked_items"], key=lambda x: x.get("id", "")
            )
            track_items2 = sorted(
                tables2["mixed_tracked_items"], key=lambda x: x.get("id", "")
            )

            for i in range(min(len(track_items1), len(track_items2))):
                assert (
                    track_items1[i]["__extract_id"] == track_items2[i]["__extract_id"]
                )

    def test_deterministic_ids_with_different_data(self):
        """Test deterministic IDs with different data but same ID fields."""
        # Create two different data records with the same ID values
        data1 = {
            "id": "SAME_ID",
            "name": "First Record",
            "items": [
                {"id": "ITEM1", "name": "Item in first record"},
            ],
        }

        data2 = {
            "id": "SAME_ID",
            "name": "Second Record",  # Different content, same ID
            "items": [
                {
                    "id": "ITEM1",
                    "name": "Item in second record",
                },  # Different content, same ID
            ],
        }

        # Create processor with deterministic ID fields
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields={
                "": "id",
                "items": "id",
            }
        )
        processor = Processor(config=config)

        # Process both datasets
        result1 = processor.process(data1, entity_name="test")
        result2 = processor.process(data2, entity_name="test")

        # Get main tables
        main1 = result1.get_main_table()
        main2 = result2.get_main_table()

        # Root records should have the same ID due to same id field value
        assert main1[0]["__extract_id"] == main2[0]["__extract_id"]

        # Verify items also have the same IDs when their id fields match
        items1 = result1.get_child_table("test_items")
        items2 = result2.get_child_table("test_items")

        # Find items with the same ID and verify their extract IDs match
        for item1 in items1:
            for item2 in items2:
                if item1["id"] == item2["id"]:
                    assert item1["__extract_id"] == item2["__extract_id"]

    def test_stability_across_processing_modes(self):
        """Test ID stability across different processing modes."""
        # Create test data
        data = {
            "id": "STABILITY_TEST",
            "name": "Stability Test",
            "items": [
                {"id": "ITEM1", "name": "Item 1"},
                {"id": "ITEM2", "name": "Item 2"},
            ],
        }

        # Create processor with deterministic ID fields
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields={
                "": "id",
                "items": "id",
            }
        )
        processor = Processor(config=config)

        # Process with standard mode
        result_standard = processor.process(data, entity_name="test")

        # Process with chunked mode (should give the same deterministic IDs)
        result_chunked = processor.process_chunked(
            [data], entity_name="test", chunk_size=1
        )

        # Get main records
        main_standard = result_standard.get_main_table()
        main_chunked = result_chunked.get_main_table()

        # Should have same root ID
        assert main_standard[0]["__extract_id"] == main_chunked[0]["__extract_id"]

        # Verify consistent IDs for items too
        items_standard = result_standard.get_child_table("test_items")
        items_chunked = result_chunked.get_child_table("test_items")

        # Sort by id for comparison
        items_standard = sorted(items_standard, key=lambda x: x.get("id", ""))
        items_chunked = sorted(items_chunked, key=lambda x: x.get("id", ""))

        # Verify consistent IDs
        for i in range(min(len(items_standard), len(items_chunked))):
            assert items_standard[i]["__extract_id"] == items_chunked[i]["__extract_id"]

    def test_invalid_custom_strategy(self):
        """Test handling of invalid custom ID generation strategy."""
        # Create test data
        data = {"id": 123, "name": "test"}

        # Define a broken ID strategy that raises an exception
        def broken_strategy(record):
            # Intentionally access a non-existent key
            return record["non_existent_key"]

        # Create processor with broken strategy
        config = TransmogConfig.default().with_metadata(
            id_generation_strategy=broken_strategy
        )
        processor = Processor(config=config)

        # Process data - should handle exception and fall back to default
        result = processor.process(data, entity_name="test")

        # Verify we have a result despite the exception
        assert len(result.get_main_table()) > 0
        assert "__extract_id" in result.get_main_table()[0]
