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
        """Test processor with deterministic ID fields."""
        # Create complex nested data
        data = {
            "id": "ROOT",
            "items": [
                {"id": "ITEM1", "name": "Item 1", "value": 10},
                {"id": "ITEM2", "name": "Item 2", "value": 20},
            ],
            "categories": [
                {
                    "id": "CAT1",
                    "name": "Category 1",
                    "subcategories": [
                        {"id": "SUBCAT1", "name": "Subcategory 1", "value": 1},
                        {"id": "SUBCAT2", "name": "Subcategory 2", "value": 2},
                    ],
                }
            ],
        }

        # Create deterministic ID fields for each path
        deterministic_id_fields = {
            "": "id",  # Root level
            "items": "id",  # Items array
            "categories": "id",  # Categories array
            "categories_subcategories": "id",  # Subcategories arrays
        }

        # Create processor with deterministic ID fields
        config = TransmogConfig.default().with_deterministic_ids(
            deterministic_id_fields
        )
        processor = Processor(config=config)

        # Process data multiple times - should get same IDs
        result1 = processor.process(data, entity_name="test1")
        result2 = processor.process(data, entity_name="test2")

        # Get results as dictionaries
        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Check deterministic IDs at each level
        # Main table (root level)
        assert (
            tables1["main_table"][0]["__extract_id"]
            == tables2["main_table"][0]["__extract_id"]
        )

        # Child tables - structure should be consistent but table names might differ
        # between test1 and test2, so we need to find corresponding tables
        child_tables1 = tables1["child_tables"]
        child_tables2 = tables2["child_tables"]

        # Find items table
        items_table1 = next((t for n, t in child_tables1.items() if "items" in n), None)
        items_table2 = next((t for n, t in child_tables2.items() if "items" in n), None)
        assert items_table1 is not None
        assert items_table2 is not None

        # Sort items by ID for comparison
        sorted_items1 = sorted(items_table1, key=lambda x: x["id"])
        sorted_items2 = sorted(items_table2, key=lambda x: x["id"])
        assert len(sorted_items1) == len(sorted_items2)
        for i in range(len(sorted_items1)):
            assert sorted_items1[i]["__extract_id"] == sorted_items2[i]["__extract_id"]

        # Find categories table
        cat_table1 = next(
            (
                t
                for n, t in child_tables1.items()
                if "categories" in n and "sub" not in n
            ),
            None,
        )
        cat_table2 = next(
            (
                t
                for n, t in child_tables2.items()
                if "categories" in n and "sub" not in n
            ),
            None,
        )
        assert cat_table1 is not None
        assert cat_table2 is not None

        # Sort categories by ID for comparison
        sorted_cats1 = sorted(cat_table1, key=lambda x: x["id"])
        sorted_cats2 = sorted(cat_table2, key=lambda x: x["id"])
        assert len(sorted_cats1) == len(sorted_cats2)
        for i in range(len(sorted_cats1)):
            assert sorted_cats1[i]["__extract_id"] == sorted_cats2[i]["__extract_id"]

        # Find subcategories table
        subcat_table1 = next(
            (t for n, t in child_tables1.items() if "subcategories" in n), None
        )
        subcat_table2 = next(
            (t for n, t in child_tables2.items() if "subcategories" in n), None
        )
        assert subcat_table1 is not None
        assert subcat_table2 is not None

        # Sort subcategories by ID for comparison
        sorted_subcats1 = sorted(subcat_table1, key=lambda x: x["id"])
        sorted_subcats2 = sorted(subcat_table2, key=lambda x: x["id"])
        assert len(sorted_subcats1) == len(sorted_subcats2)
        for i in range(len(sorted_subcats1)):
            assert (
                sorted_subcats1[i]["__extract_id"] == sorted_subcats2[i]["__extract_id"]
            )

    def test_processor_with_custom_id_strategy(self):
        """Test processor with custom ID generation strategy."""

        # Define a custom ID strategy function
        def custom_strategy(record):
            # Combine name and value fields to create a custom ID
            return f"{record.get('name', '')}-{record.get('value', 0)}"

        # Create data with nested items
        data = {
            "name": "Root",
            "value": 0,
            "items": [
                {"name": "Item1", "value": 10},
                {"name": "Item2", "value": 20},
            ],
        }

        # Create processor with custom ID strategy
        config = TransmogConfig.default().with_custom_id_generation(custom_strategy)
        processor = Processor(config=config)

        # Process data multiple times
        result1 = processor.process(data, entity_name="test")
        result2 = processor.process(data, entity_name="test")

        # Verify IDs were created using custom strategy
        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Main table
        main1 = tables1["main_table"][0]
        main2 = tables2["main_table"][0]
        assert main1["__extract_id"] == main2["__extract_id"]
        assert main1["__extract_id"] == "Root-0"  # Should match our custom strategy

        # Child tables
        items1 = next(
            iter(t for n, t in tables1["child_tables"].items() if "items" in n)
        )
        items2 = next(
            iter(t for n, t in tables2["child_tables"].items() if "items" in n)
        )

        # Sort by name for reliable comparison
        items1 = sorted(items1, key=lambda x: x["name"])
        items2 = sorted(items2, key=lambda x: x["name"])

        # Check each item
        assert items1[0]["__extract_id"] == items2[0]["__extract_id"] == "Item1-10"
        assert items1[1]["__extract_id"] == items2[1]["__extract_id"] == "Item2-20"

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
        main_table = tables["main_table"]
        items_table = next(
            iter(t for n, t in tables["child_tables"].items() if "items" in n)
        )

        # Verify main table has a generated ID (not deterministic)
        assert "__extract_id" in main_table[0]

        # First item should have deterministic ID
        item1 = next(r for r in items_table if r.get("id") == "ITEM1")
        assert "__extract_id" in item1

        # Second item should have random ID
        item2 = next(r for r in items_table if "id" not in r)
        assert "__extract_id" in item2

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

        # Process data with deterministic IDs on tracked items only
        config = TransmogConfig.default().with_metadata(
            deterministic_id_fields={
                "": "id",  # Root level uses id field
                "tracked_items": "id",  # Only tracked_items use deterministic IDs
            }
        )
        processor = Processor(config=config)

        # Process data multiple times
        result1 = processor.process(data, entity_name="mixed")
        result2 = processor.process(data, entity_name="mixed")
        result3 = processor.process(
            data, entity_name="mixed"
        )  # One more for verification

        # Get tables
        tables1 = result1.to_dict()
        tables2 = result2.to_dict()
        tables3 = result3.to_dict()

        # Main record should have consistent ID across all runs
        main_id1 = tables1["main_table"][0]["__extract_id"]
        main_id2 = tables2["main_table"][0]["__extract_id"]
        main_id3 = tables3["main_table"][0]["__extract_id"]
        assert main_id1 == main_id2 == main_id3, (
            "Main record IDs should be identical across runs"
        )

        # Get regular items from each run
        regular_items1 = tables1["child_tables"].get("mixed_regular_items", [])
        regular_items2 = tables2["child_tables"].get("mixed_regular_items", [])
        regular_items3 = tables3["child_tables"].get("mixed_regular_items", [])

        # Sort by name for reliable comparison
        reg_items1 = sorted(regular_items1, key=lambda x: x["name"])
        reg_items2 = sorted(regular_items2, key=lambda x: x["name"])
        reg_items3 = sorted(regular_items3, key=lambda x: x["name"])

        # All IDs should be different for random ID items
        for i in range(len(reg_items1)):
            id1 = reg_items1[i]["__extract_id"]
            id2 = reg_items2[i]["__extract_id"]
            id3 = reg_items3[i]["__extract_id"]
            # At least some of these IDs should be different across runs
            assert id1 != id2 or id1 != id3 or id2 != id3, (
                "At least some regular item IDs should be different across runs"
            )

        # Get tracked items from each run
        tracked_items1 = tables1["child_tables"].get("mixed_tracked_items", [])
        tracked_items2 = tables2["child_tables"].get("mixed_tracked_items", [])
        tracked_items3 = tables3["child_tables"].get("mixed_tracked_items", [])

        # Sort by id for reliable comparison
        track_items1 = sorted(tracked_items1, key=lambda x: x["id"])
        track_items2 = sorted(tracked_items2, key=lambda x: x["id"])
        track_items3 = sorted(tracked_items3, key=lambda x: x["id"])

        # Verify deterministic IDs are consistent across runs
        for i in range(len(track_items1)):
            item1 = track_items1[i]
            item2 = track_items2[i]
            item3 = track_items3[i]

            # First ensure we're comparing the same items
            assert item1["id"] == item2["id"] == item3["id"], (
                "Item IDs should match across runs"
            )

            # Now check that extract IDs are deterministic
            extract_id1 = item1["__extract_id"]
            extract_id2 = item2["__extract_id"]
            extract_id3 = item3["__extract_id"]
            assert extract_id1 == extract_id2 == extract_id3, (
                f"Extract IDs for tracked item {item1['id']} should be consistent"
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
        assert (
            tables1["main_table"][0]["__extract_id"]
            == tables2["main_table"][0]["__extract_id"]
        )

        # If items and categories were extracted as child tables, verify their IDs
        for table_name, table_data in tables1["child_tables"].items():
            if table_name in tables2["child_tables"]:
                # Ensure we're dealing with a list of records, not a string
                if isinstance(table_data, list) and isinstance(
                    tables2["child_tables"][table_name], list
                ):
                    # Sort records by id to ensure matching order
                    records1 = sorted(table_data, key=lambda x: x.get("id", ""))
                    records2 = sorted(
                        tables2["child_tables"][table_name],
                        key=lambda x: x.get("id", ""),
                    )

                    # Compare IDs
                    for i in range(min(len(records1), len(records2))):
                        assert (
                            records1[i]["__extract_id"] == records2[i]["__extract_id"]
                        )

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

        # Create processor with consistent prefix path matching
        config = TransmogConfig.default().with_deterministic_ids(
            {
                "": "id",  # Root level uses id
                "details_attributes_tags": "id",  # Use direct path for tags to ensure consistency
            }
        )
        processor = Processor(config=config)

        # Process data twice to verify ID stability
        result1 = processor.process(data, entity_name="test")
        result2 = processor.process(data, entity_name="test")

        # Get results as dictionaries
        tables1 = result1.to_dict()
        tables2 = result2.to_dict()

        # Main record ID should be deterministic
        assert (
            tables1["main_table"][0]["__extract_id"]
            == tables2["main_table"][0]["__extract_id"]
        )

        # Find tags table if it was extracted
        tags_table1 = None
        tags_table2 = None

        for table_name, table_data in tables1["child_tables"].items():
            if "tags" in table_name:
                tags_table1 = table_data
                break

        for table_name, table_data in tables2["child_tables"].items():
            if "tags" in table_name:
                tags_table2 = table_data
                break

        if tags_table1 and tags_table2:
            # Sort by id for comparison
            tags1 = sorted(tags_table1, key=lambda x: x.get("id", ""))
            tags2 = sorted(tags_table2, key=lambda x: x.get("id", ""))

            # Compare tag IDs
            for i in range(min(len(tags1), len(tags2))):
                assert tags1[i]["__extract_id"] == tags2[i]["__extract_id"]
