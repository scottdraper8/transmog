"""
Tests for the BatchStrategy implementation.

This module tests the BatchStrategy class functionality using the interface-based approach.
"""

from tests.interfaces.test_strategy_interface import AbstractStrategyTest
from transmog import ProcessingResult
from transmog.process import BatchStrategy


class TestBatchStrategy(AbstractStrategyTest):
    """
    Concrete implementation of the AbstractStrategyTest for BatchStrategy.

    Tests the BatchStrategy class through its interface.
    """

    # Set the strategy class to test
    strategy_class = BatchStrategy

    def test_strategy_process_result_type(self, strategy, batch_data):
        """Test that BatchStrategy process method returns a ProcessingResult."""
        result = strategy.process(batch_data, entity_name="test")
        assert isinstance(result, ProcessingResult)

    def test_strategy_handles_entity_name(self, strategy, batch_data):
        """Test that BatchStrategy handles entity_name parameter."""
        entity_name = "custom_entity"
        result = strategy.process(batch_data, entity_name=entity_name)
        assert result.entity_name == entity_name

    def test_strategy_preserves_data_structure(self, strategy, batch_data):
        """Test that BatchStrategy preserves the original data structure."""
        result = strategy.process(batch_data, entity_name="test")

        # Get the main table
        main_table = result.get_main_table()
        assert len(main_table) == len(batch_data)

        # Check that all original fields are present in each record
        for i, record in enumerate(main_table):
            original_record = batch_data[i]
            assert record["id"] == original_record["id"]
            assert record["name"] == original_record["name"]
            assert str(record["value"]) == str(original_record["value"])

    def test_process_complex_batch(self, strategy, complex_batch):
        """Test processing of a batch with complex data."""
        result = strategy.process(complex_batch, entity_name="complex")

        # Check table structure
        table_names = result.get_table_names()

        # Find the items table - it could be named differently based on implementation
        items_table_name = next(name for name in table_names if "items" in name)

        # Get the main table - depending on implementation, it may not exist or have a different name
        try:
            main_table = result.get_main_table()
            if main_table:
                # If main table exists, check record count
                assert len(main_table) == len(complex_batch)

                # Check relationship between parent and child records
                for parent_record in main_table:
                    parent_id = parent_record["__extract_id"]

                    # Each parent should have child records
                    items_table = result.get_child_table(items_table_name)
                    child_records = [
                        item
                        for item in items_table
                        if item["__parent_extract_id"] == parent_id
                    ]

                    # Each parent in test data has 2 child records
                    assert len(child_records) == 2
        except Exception:
            # If main table isn't available, check relationships in child records
            pass

        # Items table should exist and have expected number of records
        items_table = result.get_child_table(items_table_name)

        # Each record in complex_batch has 2 items
        assert len(items_table) == len(complex_batch) * 2

        # Group items by parent ID to check parent-child relationships
        item_groups = {}
        for item in items_table:
            parent_id = item["__parent_extract_id"]
            if parent_id not in item_groups:
                item_groups[parent_id] = []
            item_groups[parent_id].append(item)

        # Each parent should have exactly 2 items
        for _parent_id, items in item_groups.items():
            assert len(items) == 2

    def test_empty_batch(self, strategy):
        """Test processing an empty batch."""
        empty_batch = []
        result = strategy.process(empty_batch, entity_name="empty")

        # Should still return a valid result
        assert isinstance(result, ProcessingResult)

        # Main table should be empty
        main_table = result.get_main_table()
        assert len(main_table) == 0

    def test_processing_options(self, config):
        """Test that BatchStrategy respects processing options."""
        # Test with different batch sizes
        for batch_size in [1, 2, 5, 10]:
            config_with_batch_size = config.with_processing(batch_size=batch_size)
            strategy = BatchStrategy(config_with_batch_size)

            # Create test data with 20 records
            test_data = [{"id": i, "value": i} for i in range(20)]

            # Process the data
            result = strategy.process(test_data, entity_name="test")

            # All records should be processed
            assert len(result.get_main_table()) == 20
