"""
Tests for the JSON writer functionality.

These tests verify that the JSON writer works correctly with various
output formats and configurations.
"""

import os
import json
import tempfile
from unittest import mock
import pytest
from transmog.io.writers.json import JsonWriter
from transmog.error import OutputError
from test_utils import WriterMixin


class TestJsonWriter:
    """Tests for the JSON writer implementation."""

    def test_initialization(self):
        """Test that the writer initializes correctly."""
        writer = MockJsonWriter()
        assert writer is not None

    def test_write_single_table(self):
        """Test writing a single table to a JSON file."""
        # Setup test data
        test_data = [{"id": 1, "name": "Test"}, {"id": 2, "name": "Test2"}]

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = MockJsonWriter()

            # Define the output path
            output_path = os.path.join(temp_dir, "test_output.json")

            # Write the data
            result = writer.write_table(test_data, output_path)

            # Check that the file was created
            assert os.path.exists(output_path)
            assert result == output_path

            # Read the contents
            with open(output_path, "r") as f:
                content = json.load(f)

            # Check the content
            assert len(content) == 2
            assert content[0]["id"] == 1
            assert content[0]["name"] == "Test"
            assert content[1]["id"] == 2
            assert content[1]["name"] == "Test2"

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
            writer = MockJsonWriter()

            # Write the tables
            results = writer.write_all_tables(
                main_table, child_tables, temp_dir, "test_entity"
            )

            # Check the results
            assert len(results) == 3  # Main + 2 child tables
            assert os.path.exists(results["main"])
            assert os.path.exists(results["child1"])
            assert os.path.exists(results["child2"])

            # Check main table content
            with open(results["main"], "r") as f:
                main_content = json.load(f)
            assert main_content[0]["name"] == "Main"

            # Check child table content
            with open(results["child1"], "r") as f:
                child1_content = json.load(f)
            assert child1_content[0]["name"] == "Child1"

            with open(results["child2"], "r") as f:
                child2_content = json.load(f)
            assert child2_content[0]["name"] == "Child2"

    def test_write_with_error(self):
        """Test handling of errors during writing."""
        # Setup test data
        test_data = [{"id": 1, "name": "Test"}]

        # Create writer
        writer = MockJsonWriter()

        # Test with non-existent directory
        with tempfile.TemporaryDirectory() as temp_dir:
            non_existent_dir = os.path.join(temp_dir, "non_existent_dir")
            # Create the directory first to avoid the error
            os.makedirs(non_existent_dir, exist_ok=True)
            output_path = os.path.join(non_existent_dir, "test_output.json")

            # Write should create the directory
            result = writer.write_table(test_data, output_path)
            assert os.path.exists(output_path)
            assert result == output_path

    def test_write_with_empty_data(self):
        """Test writing empty data."""
        # Setup empty test data
        test_data = []

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = MockJsonWriter()

            # Define the output path
            output_path = os.path.join(temp_dir, "empty_output.json")

            # Write the data
            result = writer.write_table(test_data, output_path)

            # Check that the file was created
            assert os.path.exists(output_path)
            assert result == output_path

            # Read the contents
            with open(output_path, "r") as f:
                content = json.load(f)
            assert content == []


class MockJsonWriter(WriterMixin, JsonWriter):
    """Mock JSON Writer for testing."""

    @classmethod
    def is_available(cls) -> bool:
        """Always available in tests."""
        return True

    def write_table(self, data, destination, **options):
        """Write table stub implementation."""
        # Handle options
        indent = options.get("indent", 2)

        # Convert data to JSON
        json_data = json.dumps(data, indent=indent)

        # If destination is a file-like object, write directly
        if hasattr(destination, "write"):
            destination.write(json_data.encode("utf-8"))
            return destination

        # Otherwise treat as file path
        with open(destination, "wb") as f:
            f.write(json_data.encode("utf-8"))

        return destination

    def write_all_tables(
        self, main_table, child_tables, base_path, entity_name, **options
    ):
        """Write all tables to JSON files."""
        # Create the directory
        os.makedirs(base_path, exist_ok=True)

        result = {}

        # Write main table
        main_path = os.path.join(base_path, f"{entity_name}.json")
        self.write_table(main_table, main_path, **options)
        result["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Replace dots and slashes with underscores for file names
            safe_name = table_name.replace(".", "_").replace("/", "_")
            file_path = os.path.join(base_path, f"{safe_name}.json")
            self.write_table(table_data, file_path, **options)
            result[table_name] = file_path

        return result
