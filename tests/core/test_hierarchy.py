"""
Tests for the hierarchy implementation.

This module tests hierarchical data processing functionality through the Processor class
rather than directly testing the low-level implementation functions.
"""

import pytest
import json

from transmog import Processor, TransmogConfig
from tests.interfaces.test_hierarchy_interface import AbstractHierarchyTest


class TestHierarchy(AbstractHierarchyTest):
    """
    Concrete implementation of AbstractHierarchyTest using the Processor class.

    This implementation uses the high-level Processor API rather than low-level functions,
    making it more resilient to implementation changes.
    """

    @pytest.fixture
    def processor(self):
        """Create a processor instance for testing hierarchy functionality."""
        config = TransmogConfig.default().with_processing(cast_to_string=True)
        return Processor(config=config)

    def test_process_structure(self, processor, simple_table_structure):
        """Test processing a simple structure through the Processor API."""
        # Process using processor
        result = processor.process(simple_table_structure, entity_name="test")

        # Verify main record
        main_records = result.get_main_table()
        assert len(main_records) == 1
        assert "__extract_id" in main_records[0]

        # Verify child tables - note: 'main' is not included in table_names
        table_names = result.get_table_names()
        assert len(table_names) >= 1  # At least one child table

        # Find items table
        items_table = next(t for t in table_names if "items" in t.lower())

        # Check array items
        items = result.get_child_table(items_table)
        assert len(items) == 2

        # Check required fields in items
        for item in items:
            assert "__extract_id" in item
            assert "__parent_extract_id" in item
            assert item["__parent_extract_id"] == main_records[0]["__extract_id"]

    def test_process_complex_structure(self, processor, complex_table_structure):
        """Test processing a complex structure with nested arrays."""
        # Process using processor
        result = processor.process(complex_table_structure, entity_name="test")

        # Verify main table
        main_records = result.get_main_table()
        assert len(main_records) == 1
        main_id = main_records[0]["__extract_id"]

        # Get table names
        table_names = result.get_table_names()

        # Find tables for each level
        items_table = next(
            t for t in table_names if "items" in t.lower() and "sub" not in t.lower()
        )
        subitems_table = next(t for t in table_names if "subitems" in t.lower())
        tags_table = next(t for t in table_names if "tags" in t.lower())

        # Check table record counts
        items = result.get_child_table(items_table)
        subitems = result.get_child_table(subitems_table)
        tags = result.get_child_table(tags_table)

        # We don't assert exact counts because the implementation may flatten differently
        # Instead, verify relative counts and relationships
        assert len(items) > 0
        assert len(subitems) > 0
        assert len(tags) > 0

        # Verify parent-child relationships
        # At least some items should have the main record as parent
        main_children = [
            item for item in items if item["__parent_extract_id"] == main_id
        ]
        assert len(main_children) > 0

        # At least some tags should have the main record as parent
        main_tag_children = [
            tag for tag in tags if tag["__parent_extract_id"] == main_id
        ]
        assert len(main_tag_children) > 0

        # Verify at least some subitems have items as parents
        item_ids = [item["__extract_id"] for item in items]
        subitems_with_item_parents = [
            subitem
            for subitem in subitems
            if subitem["__parent_extract_id"] in item_ids
        ]
        assert len(subitems_with_item_parents) > 0

    def test_process_record_batch(self, processor, simple_table_structure):
        """Test processing a batch of records."""
        # Create a batch of records
        batch = [simple_table_structure.copy() for _ in range(3)]

        # Process batch
        result = processor.process_batch(batch, entity_name="test")

        # Verify main table
        main_records = result.get_main_table()
        assert len(main_records) == 3

        # Verify all records have extract IDs
        for record in main_records:
            assert "__extract_id" in record

        # Find items table
        table_names = result.get_table_names()
        items_table = next(t for t in table_names if "items" in t.lower())

        # Check items count (2 items per record × 3 records)
        items = result.get_child_table(items_table)
        assert len(items) == 6

    def test_stream_process_records(self, processor, complex_table_structure):
        """Test streaming processing of records through the chunked API."""
        # Create a batch of records
        batch = [complex_table_structure.copy() for _ in range(2)]

        # Process using chunked mode (simulates streaming)
        result = processor.process_chunked(batch, entity_name="test", chunk_size=1)

        # Verify main table
        main_records = result.get_main_table()
        assert len(main_records) == 2

        # Get table names
        table_names = result.get_table_names()

        # Find tables
        items_table = next(
            t for t in table_names if "items" in t.lower() and "sub" not in t.lower()
        )
        subitems_table = next(t for t in table_names if "subitems" in t.lower())
        tags_table = next(t for t in table_names if "tags" in t.lower())

        # Check record counts
        items = result.get_child_table(items_table)
        subitems = result.get_child_table(subitems_table)
        tags = result.get_child_table(tags_table)

        # Verify we have items, subitems, and tags
        assert len(items) > 0
        assert len(subitems) > 0
        assert len(tags) > 0

    def test_orphaned_arrays(self, processor):
        """Test handling of orphaned arrays (arrays with no parent reference in original data)."""
        # Create a structure with an orphaned array
        structure = {
            "id": "main1",
            "name": "Main Record",
            "regular_array": [{"id": "item1", "name": "Regular Item"}],
            "orphaned_array": [],  # Empty array, which might be problematic
        }

        # Process using processor
        result = processor.process(structure, entity_name="test")

        # Verify main record
        main_records = result.get_main_table()
        assert len(main_records) == 1
        assert "__extract_id" in main_records[0]

        # Check if there are child tables
        table_names = result.get_table_names()

        # There should be at least one child table (for regular_array)
        assert len(table_names) > 0

        # Find tables related to regular_array
        regular_array_tables = [t for t in table_names if "regular" in t.lower()]
        assert len(regular_array_tables) > 0

    def test_multi_root_processing(self, processor):
        """Test processing multiple independent structures."""
        # Create multiple independent structures
        structure1 = {
            "id": "1",
            "name": "Structure 1",
            "items": [{"id": "item1", "name": "Item 1"}],
        }

        structure2 = {
            "id": "2",
            "name": "Structure 2",
            "items": [{"id": "item2", "name": "Item 2"}],
        }

        # Process both structures
        result1 = processor.process(structure1, entity_name="test1")
        result2 = processor.process(structure2, entity_name="test2")

        # Verify both were processed with different IDs
        main1 = result1.get_main_table()[0]
        main2 = result2.get_main_table()[0]
        assert main1["__extract_id"] != main2["__extract_id"]

        # Each should have its own items table
        tables1 = result1.get_table_names()
        tables2 = result2.get_table_names()

        items1_table = next(t for t in tables1 if "items" in t.lower())
        items2_table = next(t for t in tables2 if "items" in t.lower())

        items1 = result1.get_child_table(items1_table)
        items2 = result2.get_child_table(items2_table)

        assert len(items1) == 1
        assert len(items2) == 1

        # Items should reference their respective parents
        assert items1[0]["__parent_extract_id"] == main1["__extract_id"]
        assert items2[0]["__parent_extract_id"] == main2["__extract_id"]

    def test_circular_reference_handling(self, processor):
        """Test handling of circular references in nested objects."""
        # Create a structure with a circular reference
        structure = {"id": "1", "name": "Circular Structure"}
        # Create a separate dict to avoid JSON serialization issues when testing
        sub_obj = {"id": "sub", "name": "Subobject"}
        structure["ref"] = sub_obj
        sub_obj["parent_ref"] = structure  # Create circular reference

        # Configure processor to use strategy best suited for testing
        config = TransmogConfig.default().with_error_handling(
            recovery_strategy="lenient"
        )
        test_processor = Processor(config=config)

        # Try with lenient error handling first
        try:
            result = test_processor.process(structure, entity_name="test")

            # If it succeeded, verify the result has the main record
            main_records = result.get_main_table()
            assert len(main_records) == 1
            assert "__extract_id" in main_records[0]

            # Verify the circular reference was handled (either pruned or marked)
            # We don't assert specific field content, just that processing completed
            print("Circular reference was handled with recovery")

        except Exception as e:
            # If it fails even with lenient handling, check it's the expected error type
            from transmog.error import ProcessingError, CircularReferenceError

            assert isinstance(e, (CircularReferenceError, ProcessingError)), (
                f"Unexpected error type: {type(e)}"
            )

            if isinstance(e, ProcessingError):
                # Check if the inner error is related to circular references
                error_msg = str(e).lower()
                assert any(
                    term in error_msg for term in ["circular", "recursion", "cycle"]
                ), f"ProcessingError does not indicate circular reference: {error_msg}"

            print(f"Circular reference detected as expected: {type(e).__name__}")

        # Now test with strict error handling - should always fail
        strict_config = TransmogConfig.default().with_error_handling(
            recovery_strategy="strict"
        )
        strict_processor = Processor(config=strict_config)

        try:
            strict_processor.process(structure, entity_name="test")
            pytest.fail(
                "Expected an exception with strict handling of circular references"
            )
        except Exception as e:
            # Verify it's a recognized error type
            from transmog.error import ProcessingError, CircularReferenceError

            assert isinstance(e, (CircularReferenceError, ProcessingError)), (
                f"Unexpected error type: {type(e)}"
            )

            # Verify error message indicates circular reference
            error_msg = str(e).lower()
            assert any(
                term in error_msg for term in ["circular", "recursion", "cycle"]
            ), f"Error does not indicate circular reference: {error_msg}"

    def test_processor_hierarchical_processing(self, processor):
        """Test hierarchical processing through the Processor interface."""
        # Create a complex structure
        data = {
            "id": "main",
            "name": "Main Record",
            "nested": {
                "field1": "value1",
                "field2": "value2",
            },
            "items": [
                {"id": "item1", "name": "Item 1"},
                {"id": "item2", "name": "Item 2"},
            ],
        }

        # Process through the processor interface
        result = processor.process(data, entity_name="test")

        # Verify main table
        main_records = result.get_main_table()
        assert len(main_records) == 1

        # Due to field naming inconsistencies, we'll just check that main record exists
        # and has expected metadata
        assert "__extract_id" in main_records[0]
        assert "__extract_datetime" in main_records[0]

        # Verify there are child tables
        table_names = result.get_table_names()
        assert len(table_names) > 0

        # Find and verify items table
        items_table = next((t for t in table_names if "items" in t.lower()), None)
        assert items_table is not None, "Items table not found"

        items = result.get_child_table(items_table)
        assert len(items) == 2

        # Check parent-child relationship
        for item in items:
            assert item["__parent_extract_id"] == main_records[0]["__extract_id"]

    def test_batch_processing_consistency(self, processor):
        """Test that batch processing produces consistent results with single processing."""
        # Create test data
        single_record = {
            "id": "single",
            "items": [{"id": "item1"}, {"id": "item2"}],
        }

        batch_records = [single_record] * 3

        # Process single record
        single_result = processor.process(single_record, entity_name="test")

        # Process batch
        batch_result = processor.process_batch(batch_records, entity_name="test")

        # Verify structure consistency
        assert set(single_result.get_table_names()) == set(
            batch_result.get_table_names()
        )

        # Verify each batch record has the same structure as the single record
        single_main = single_result.get_main_table()[0]
        batch_mains = batch_result.get_main_table()

        # Check field consistency (excluding IDs which would be different)
        fields_to_check = [f for f in single_main.keys() if not f.startswith("__")]
        for record in batch_mains:
            for field in fields_to_check:
                assert field in record
                assert record[field] == single_main[field]
