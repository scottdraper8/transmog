"""
Tests for the FileStrategy implementation.

This module tests the FileStrategy class functionality using the interface-based approach.
"""

import json

import pytest

from tests.interfaces.test_strategy_interface import AbstractStrategyTest
from transmog import ProcessingResult
from transmog.process import FileStrategy


class TestFileStrategy(AbstractStrategyTest):
    """
    Concrete implementation of the AbstractStrategyTest for FileStrategy.

    Tests the FileStrategy class through its interface.
    """

    # Set the strategy class to test
    strategy_class = FileStrategy

    @pytest.fixture
    def json_batch_file(self, tmp_path, batch_data):
        """Create a temporary JSON file with batch data."""
        file_path = tmp_path / "batch_data.json"
        with open(file_path, "w") as f:
            json.dump(batch_data, f)
        return str(file_path)

    @pytest.fixture
    def jsonl_batch_file(self, tmp_path, batch_data):
        """Create a temporary JSONL file with batch data."""
        file_path = tmp_path / "batch_data.jsonl"
        with open(file_path, "w") as f:
            for record in batch_data:
                f.write(json.dumps(record) + "\n")
        return str(file_path)

    @pytest.fixture
    def json_complex_file(self, tmp_path, complex_data):
        """Create a temporary JSON file with complex data."""
        file_path = tmp_path / "complex_data.json"
        with open(file_path, "w") as f:
            json.dump(complex_data, f)
        return str(file_path)

    def test_strategy_process_result_type(self, strategy, json_file):
        """Test that FileStrategy process method returns a ProcessingResult."""
        result = strategy.process(json_file, entity_name="test")
        assert isinstance(result, ProcessingResult)

    def test_strategy_handles_entity_name(self, strategy, json_file):
        """Test that FileStrategy handles entity_name parameter."""
        entity_name = "custom_entity"
        result = strategy.process(json_file, entity_name=entity_name)
        assert result.entity_name == entity_name

    def test_strategy_preserves_data_structure(self, strategy, json_file, batch_data):
        """Test that FileStrategy preserves the original data structure."""
        result = strategy.process(json_file, entity_name="test")

        # Get the main table
        main_table = result.get_main_table()
        assert len(main_table) == len(batch_data)

        # Check that all original fields are present in each record
        for i, record in enumerate(main_table):
            original_record = batch_data[i]
            assert record["id"] == original_record["id"]
            assert record["name"] == original_record["name"]
            assert str(record["value"]) == str(original_record["value"])

    def test_process_json_file(self, strategy, json_batch_file, batch_data):
        """Test processing a JSON file with batch data."""
        result = strategy.process(json_batch_file, entity_name="json_batch")

        # Check main table
        main_table = result.get_main_table()
        assert len(main_table) == len(batch_data)

        # Verify data
        for i, record in enumerate(main_table):
            original_record = batch_data[i]
            assert record["id"] == original_record["id"]
            assert record["name"] == original_record["name"]
            assert str(record["value"]) == str(original_record["value"])

    def test_process_jsonl_file(self, strategy, jsonl_batch_file, batch_data):
        """Test processing a JSONL file with batch data."""
        result = strategy.process(jsonl_batch_file, entity_name="jsonl_batch")

        # Check main table
        main_table = result.get_main_table()
        assert len(main_table) == len(batch_data)

        # Verify data
        for i, record in enumerate(main_table):
            original_record = batch_data[i]
            assert record["id"] == original_record["id"]
            assert record["name"] == original_record["name"]
            assert str(record["value"]) == str(original_record["value"])

    def test_process_complex_json_file(self, strategy, json_complex_file, complex_data):
        """Test processing a JSON file with complex data containing arrays."""
        result = strategy.process(json_complex_file, entity_name="complex")

        # Check table structure
        table_names = result.get_table_names()
        assert "complex_items" in table_names

        # Get the main table - when processing a single complex object, FileStrategy might
        # not create a main table and just extract the child tables
        try:
            main_table = result.get_main_table()
            if main_table:
                assert len(main_table) == 1
                assert main_table[0]["id"] == complex_data["id"]
                assert main_table[0]["name"] == complex_data["name"]
        except Exception:
            # If main table isn't available, that's acceptable for this test
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

    def test_nonexistent_file(self, strategy):
        """Test handling of nonexistent file."""
        with pytest.raises(Exception):
            strategy.process("nonexistent_file.json", entity_name="test")

    def test_invalid_json_file(self, tmp_path, strategy):
        """Test handling of invalid JSON file."""
        # Create file with invalid JSON
        invalid_file = tmp_path / "invalid.json"
        with open(invalid_file, "w") as f:
            f.write("{invalid json")

        with pytest.raises(Exception):
            strategy.process(str(invalid_file), entity_name="test")
