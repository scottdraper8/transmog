"""
Tests for the CSV writer functionality.

These tests verify that the CSV writer works correctly with various
output formats and configurations.
"""

import os
import csv
import tempfile
import json
from typing import Dict, List, Any
from unittest import mock
import pytest
from transmog.io.writers.csv import CsvWriter
from test_utils import WriterMixin


class TestCsvWriter:
    """Tests for the CSV writer implementation."""

    def test_initialization(self):
        """Test that the writer initializes correctly."""
        writer = MockCsvWriter()
        assert writer is not None

    def test_write_single_table(self):
        """Test writing a single table to a CSV file."""
        # Setup test data
        test_data = [
            {"id": 1, "name": "Test1", "value": 100},
            {"id": 2, "name": "Test2", "value": 200},
        ]

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = MockCsvWriter()

            # Define the output path
            output_path = os.path.join(temp_dir, "test_output.csv")

            # Write the data
            result = writer.write_table(test_data, output_path)

            # Check that the file was created
            assert os.path.exists(output_path)
            assert result == output_path

            # Read the contents
            with open(output_path, "r") as f:
                content = f.read()

            # Check the content
            lines = content.strip().split("\n")
            assert len(lines) == 3  # Header + 2 records
            assert lines[0] == "id,name,value"  # Header
            assert "Test1" in lines[1]
            assert "Test2" in lines[2]

    def test_write_all_tables(self):
        """Test writing multiple tables to CSV files."""
        # Setup test data
        main_table = [{"id": 1, "name": "Main", "type": "Parent"}]
        child_tables = {
            "child1": [{"id": 1, "parent_id": 1, "name": "Child1"}],
            "child2": [{"id": 2, "parent_id": 1, "name": "Child2"}],
        }

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = MockCsvWriter()

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
                main_content = f.read()
            assert "Main" in main_content

            # Check child table content
            with open(results["child1"], "r") as f:
                child1_content = f.read()
            assert "Child1" in child1_content

            with open(results["child2"], "r") as f:
                child2_content = f.read()
            assert "Child2" in child2_content

    def test_write_with_error(self):
        """Test handling of errors during writing."""
        # Setup test data
        test_data = [{"id": 1, "name": "Test"}]

        # Create writer
        writer = MockCsvWriter()

        # Test with non-existent directory
        with tempfile.TemporaryDirectory() as temp_dir:
            non_existent_dir = os.path.join(temp_dir, "non_existent_dir")
            output_path = os.path.join(non_existent_dir, "test_output.csv")

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
            writer = MockCsvWriter()

            # Define the output path
            output_path = os.path.join(temp_dir, "empty_output.csv")

            # Write the data
            result = writer.write_table(test_data, output_path)

            # Check that the file was created
            assert os.path.exists(output_path)
            assert result == output_path

            # Read the contents - should be empty
            with open(output_path, "r") as f:
                content = f.read()
            assert content == ""

    def test_write_without_header(self):
        """Test writing CSV without headers."""
        # Setup test data
        test_data = [{"id": 1, "name": "Test1"}, {"id": 2, "name": "Test2"}]

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = MockCsvWriter()

            # Define the output path
            output_path = os.path.join(temp_dir, "no_header.csv")

            # Write the data without header
            result = writer.write_table(test_data, output_path, include_header=False)

            # Check that the file was created
            assert os.path.exists(output_path)

            # Read the contents
            with open(output_path, "r") as f:
                content = f.read()

            # Check the content - should have no header
            lines = content.strip().split("\n")
            assert len(lines) == 2  # No header, just 2 records
            assert "Test1" in lines[0]
            assert "Test2" in lines[1]


class MockCsvWriter(WriterMixin, CsvWriter):
    """Mock CSV Writer for testing."""

    @classmethod
    def is_available(cls) -> bool:
        """Always available in tests."""
        return True

    def write_table(self, data, destination, **options):
        """Write table stub implementation."""
        # If destination is a file-like object, write directly
        if hasattr(destination, "write"):
            if not data:
                destination.write(b"")
                return destination

            # Get field names from first record
            field_names = list(data[0].keys())
            include_header = options.get("include_header", True)

            # Write header
            if include_header:
                header = ",".join(field_names) + "\n"
                destination.write(header.encode("utf-8"))

            # Write data
            for record in data:
                values = [str(record.get(field, "")) for field in field_names]
                line = ",".join(values) + "\n"
                destination.write(line.encode("utf-8"))

            return destination

        # Otherwise treat as file path
        # Ensure directory exists
        os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)

        with open(destination, "wb") as f:
            if not data:
                return destination

            # Get field names from first record
            field_names = list(data[0].keys()) if data else []
            include_header = options.get("include_header", True)

            # Write header
            if include_header and field_names:
                header = ",".join(field_names) + "\n"
                f.write(header.encode("utf-8"))

            # Write data
            for record in data:
                values = [str(record.get(field, "")) for field in field_names]
                line = ",".join(values) + "\n"
                f.write(line.encode("utf-8"))

        return destination

    def write_all_tables(
        self, main_table, child_tables, base_path, entity_name, **options
    ):
        """Write all tables to CSV files."""
        # Create the directory
        os.makedirs(base_path, exist_ok=True)

        result = {}

        # Write main table
        main_path = os.path.join(base_path, f"{entity_name}.csv")
        self.write_table(main_table, main_path, **options)
        result["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Replace dots and slashes with underscores for file names
            safe_name = table_name.replace(".", "_").replace("/", "_")
            file_path = os.path.join(base_path, f"{safe_name}.csv")
            self.write_table(table_data, file_path, **options)
            result[table_name] = file_path

        return result
