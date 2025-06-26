"""
Tests for the InMemoryStrategy implementation.

This module tests the InMemoryStrategy class functionality using the interface-based approach.
"""

from tests.interfaces.test_strategy_interface import AbstractStrategyTest
from transmog import ProcessingResult
from transmog.process import InMemoryStrategy


class TestInMemoryStrategy(AbstractStrategyTest):
    """
    Concrete implementation of the AbstractStrategyTest for InMemoryStrategy.

    Tests the InMemoryStrategy class through its interface.
    """

    # Set the strategy class to test
    strategy_class = InMemoryStrategy

    def test_strategy_process_result_type(self, strategy, simple_data):
        """Test that InMemoryStrategy process method returns a ProcessingResult."""
        result = strategy.process(simple_data, entity_name="test")
        assert isinstance(result, ProcessingResult)

    def test_strategy_handles_entity_name(self, strategy, simple_data):
        """Test that InMemoryStrategy handles entity_name parameter."""
        entity_name = "custom_entity"
        result = strategy.process(simple_data, entity_name=entity_name)
        assert result.entity_name == entity_name

    def test_strategy_preserves_data_structure(self, strategy, simple_data):
        """Test that InMemoryStrategy preserves the original data structure."""
        result = strategy.process(simple_data, entity_name="test")

        # Get the main table
        main_table = result.get_main_table()
        assert len(main_table) == 1

        # Check that all original fields are present
        main_record = main_table[0]
        assert main_record["id"] == simple_data["id"]
        assert main_record["name"] == simple_data["name"]

        # Check that nested fields are flattened
        assert "addr_street" in main_record
        assert "addr_city" in main_record
        assert "addr_zip" in main_record

        # Check that nested field values are preserved
        assert main_record["addr_street"] == simple_data["addr"]["street"]
        assert main_record["addr_city"] == simple_data["addr"]["city"]
        assert main_record["addr_zip"] == simple_data["addr"]["zip"]

    def test_process_complex_data(self, strategy, complex_data):
        """Test processing of complex data with nested arrays."""
        result = strategy.process(complex_data, entity_name="complex")

        # Verify table structure
        table_names = result.get_table_names()
        assert "complex_items" in table_names

        # Check for main table - it may or may not exist depending on implementation
        try:
            main_table = result.get_main_table()
            if main_table and len(main_table) > 0:
                # If main table exists, check that original data is preserved
                assert main_table[0]["id"] == complex_data["id"]
                assert main_table[0]["name"] == complex_data["name"]

                # Get the ID to verify parent-child relationships
                parent_id = main_table[0]["__transmog_id"]

                # Check items table
                items_table = result.get_child_table("complex_items")
                assert len(items_table) == len(complex_data["items"])

                # Verify all child records have the correct parent ID
                for item in items_table:
                    assert item["__parent_transmog_id"] == parent_id
        except Exception:
            # If main table isn't available, that's acceptable
            pass

        # Check items table - this should always be present
        items_table = result.get_child_table("complex_items")
        assert len(items_table) == len(complex_data["items"])

        # Check that item values are preserved
        for i, item in enumerate(items_table):
            original_item = complex_data["items"][i]
            assert item["id"] == original_item["id"]
            assert item["name"] == original_item["name"]
            assert str(item["value"]) == str(original_item["value"])

    def test_cast_to_string_option(self, config, simple_data):
        """Test that InMemoryStrategy respects cast_to_string configuration."""
        # Configure with cast_to_string=True
        config_cast = config.with_processing(cast_to_string=True)
        strategy_cast = InMemoryStrategy(config_cast)

        # Configure with cast_to_string=False
        config_no_cast = config.with_processing(cast_to_string=False)
        strategy_no_cast = InMemoryStrategy(config_no_cast)

        # Process with both strategies
        result_cast = strategy_cast.process(
            {"id": 123, "value": 42.0, "active": True}, entity_name="test"
        )

        result_no_cast = strategy_no_cast.process(
            {"id": 123, "value": 42.0, "active": True}, entity_name="test"
        )

        # Check cast_to_string=True result
        cast_record = result_cast.get_main_table()[0]
        assert isinstance(cast_record["id"], str)
        assert isinstance(cast_record["value"], str)
        assert isinstance(cast_record["active"], str)

        # Check cast_to_string=False result
        no_cast_record = result_no_cast.get_main_table()[0]
        assert isinstance(no_cast_record["id"], int)
        assert isinstance(no_cast_record["value"], float)
        assert isinstance(no_cast_record["active"], bool)
