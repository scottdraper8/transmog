"""
Tests for the JSON writer component.
"""

import json
import os
import tempfile
from unittest import mock

import pytest

from transmog.io.json_writer import JsonWriter


class TestJsonWriter:
    """Test class for the JSON Writer."""

    def test_initialization(self):
        """Test that the writer initializes correctly."""
        writer = JsonWriter()
        assert writer is not None

    def test_write_single_table(self):
        """Test writing a single table to a JSON file."""
        # Setup test data
        test_data = [{"id": 1, "name": "Test"}, {"id": 2, "name": "Test2"}]

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = JsonWriter()

            # Write to file
            output_path = os.path.join(temp_dir, "test_table.json")
            file_path = writer.write_table(
                table_data=test_data, output_path=output_path, indent=2
            )

            # Check that file exists
            assert os.path.exists(file_path)

            # Read the file and check contents
            with open(file_path, "r") as f:
                content = json.load(f)
                assert content == test_data

    def test_write_all_tables(self):
        """Test writing multiple tables to JSON files."""
        # Setup test data
        main_table = [{"id": 1, "name": "Main"}]
        child_tables = {
            "child1": [{"id": 1, "parent_id": 1, "name": "Child1"}],
            "child2": [{"id": 2, "parent_id": 1, "name": "Child2"}],
        }

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = JsonWriter()

            # Write all tables
            result = writer.write_all_tables(
                main_table=main_table,
                child_tables=child_tables,
                base_path=temp_dir,
                entity_name="test_entity",
                indent=2,
            )

            # Check results
            assert "main" in result
            assert "child1" in result
            assert "child2" in result

            # Check files exist
            assert os.path.exists(result["main"])
            assert os.path.exists(result["child1"])
            assert os.path.exists(result["child2"])

            # Verify main table content
            with open(result["main"], "r") as f:
                content = json.load(f)
                assert content == main_table

            # Verify child tables content
            for table_name in ["child1", "child2"]:
                with open(result[table_name], "r") as f:
                    content = json.load(f)
                    assert content == child_tables[table_name]

    def test_write_with_error(self):
        """Test handling of errors during writing."""
        # Setup test data
        test_data = [{"id": 1, "name": "Test"}]

        # Mock open to raise an error
        with mock.patch("builtins.open", side_effect=IOError("Mock IO Error")):
            # Create writer
            writer = JsonWriter()

            # Test that exception is raised
            with pytest.raises(IOError):
                writer.write_table(
                    table_data=test_data, output_path="/tmp/error_table.json"
                )

    def test_write_with_empty_data(self):
        """Test writing empty data."""
        # Setup empty test data
        test_data = []

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = JsonWriter()

            # Write to file
            output_path = os.path.join(temp_dir, "empty_table.json")
            file_path = writer.write_table(
                table_data=test_data, output_path=output_path
            )

            # Check that file exists
            assert os.path.exists(file_path)

            # Read the file and check contents
            with open(file_path, "r") as f:
                content = json.load(f)
                assert content == []
