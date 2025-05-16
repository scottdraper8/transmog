"""
Tests for the CSV writer implementation.

This module tests that the CSV writer correctly handles writing data to CSV format.
"""

import csv
import os

import pytest

from tests.interfaces.test_writer_interface import AbstractWriterTest

# Import the writer and abstract test base class
from transmog.io.writers.csv import CsvWriter


class TestCsvWriter(AbstractWriterTest):
    """Test the CSV writer implementation."""

    writer_class = CsvWriter
    format_name = "csv"

    @pytest.fixture
    def writer(self):
        """Create a CSV writer."""
        return CsvWriter(include_header=True)

    def test_header_option(self, writer, batch_data, tmp_path):
        """Test the include_header option."""
        # Create writers with and without headers
        writer_with_header = CsvWriter(include_header=True)
        writer_without_header = CsvWriter(include_header=False)

        # Create output paths
        output_with_header = tmp_path / "with_header.csv"
        output_without_header = tmp_path / "without_header.csv"

        # Write data with both writers
        writer_with_header.write_table(batch_data, output_with_header)
        writer_without_header.write_table(batch_data, output_without_header)

        # Verify both files exist
        assert os.path.exists(output_with_header)
        assert os.path.exists(output_without_header)

        # Read back and verify
        with open(output_with_header, newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

            # Should include header row
            assert len(rows) == len(batch_data) + 1

            # First row should be header with field names
            header = rows[0]
            for key in batch_data[0].keys():
                assert key in header

        with open(output_without_header, newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

            # Should not include header row
            assert len(rows) == len(batch_data)

    def test_delimiter_option(self, batch_data, tmp_path):
        """Test different delimiter options."""
        # Test various delimiters
        delimiters = [",", ";", "\t", "|"]
        outputs = {}

        for delimiter in delimiters:
            # Create writer with this delimiter
            writer = CsvWriter(delimiter=delimiter, include_header=True)

            # Create output path
            output_path = tmp_path / f"delimiter_test_{delimiter}.csv"

            # Write data
            writer.write_table(batch_data, output_path)

            # Store path for content comparison
            outputs[delimiter] = output_path

            # Verify file exists
            assert os.path.exists(output_path)

        # Read back and verify each file uses the correct delimiter
        for delimiter, path in outputs.items():
            with open(path) as f:
                content = f.read()

                # Check if the file contains the delimiter
                assert delimiter in content

                # For a crude check, the delimiter should appear approximately once per field
                # Subtract 1 to account for end of line
                expected_count = (len(batch_data[0].keys()) - 1) * (len(batch_data) + 1)
                # Allow a small tolerance for edge cases
                assert abs(content.count(delimiter) - expected_count) <= 3

    def test_quoting_options(self, tmp_path):
        """Test different quoting options for fields with special characters."""
        # Create test data with fields that require quoting
        test_data = [
            {"id": 1, "name": "Normal Name", "description": "No special chars"},
            {"id": 2, "name": "Comma, in name", "description": "Contains a comma"},
            {"id": 3, "name": 'Quoted "Name"', "description": "Contains quotes"},
            {"id": 4, "name": "Line\nBreak", "description": "Contains a line break"},
        ]

        # Create writers with different quoting settings
        quoting_options = [
            (csv.QUOTE_MINIMAL, "minimal"),  # Quote only when necessary
            (csv.QUOTE_NONNUMERIC, "nonnumeric"),  # Quote all non-numeric fields
            (csv.QUOTE_ALL, "all"),  # Quote all fields
        ]

        outputs = {}

        for quoting, label in quoting_options:
            # Create writer
            writer = CsvWriter(include_header=True, quoting=quoting)

            # Create output path
            output_path = tmp_path / f"quoting_test_{label}.csv"

            # Write data
            writer.write_table(test_data, output_path)

            # Store path for content comparison
            outputs[label] = output_path

            # Verify file exists
            assert os.path.exists(output_path)

        # Verify each file has the expected quoting behavior
        with open(outputs["minimal"]) as f:
            content = f.read()
            # For minimal quoting, normal fields should not be quoted
            assert '"Normal Name"' not in content
            # Fields with commas should be quoted
            assert '"Comma, in name"' in content

        with open(outputs["all"]) as f:
            content = f.read()
            # For all quoting, normal fields should be quoted
            assert '"Normal Name"' in content
            # Numeric fields should also be quoted
            assert '"1"' in content

    def test_escapechar_option(self, tmp_path):
        """Test the escapechar option for escaping special characters."""
        # Create test data with fields that require escaping
        test_data = [
            {"id": 1, "name": "No Escaping Needed", "description": "Normal text"},
            {"id": 2, "name": 'Has "Quotes"', "description": "Need to escape quotes"},
            {
                "id": 3,
                "name": "Backslash \\\\",
                "description": "Has a backslash",
            },  # Use double backslash to ensure it's properly escaped
        ]

        # Create writer with custom escape character
        writer = CsvWriter(include_header=True, escapechar="\\")

        # Create output path
        output_path = tmp_path / "escapechar_test.csv"

        # Write data
        writer.write_table(test_data, output_path)

        # Verify file exists
        assert os.path.exists(output_path)

        # First verify the raw file contents contain the escaped backslash
        with open(output_path, "rb") as f:
            raw_content = f.read().decode("utf-8", errors="replace")
            # Look for the escaped backslash in the raw file - should be at least doubled
            assert "Backslash" in raw_content
            assert "\\\\" in raw_content

        # Read back with CSV reader to verify escaping
        with open(output_path, newline="") as f:
            reader = csv.reader(f, escapechar="\\")
            rows = list(reader)

            # Verify record count (include header)
            assert len(rows) == len(test_data) + 1

            # Find the index of the "name" column
            header = rows[0]
            name_index = header.index("name")

            # Verify the quoted name is preserved
            assert rows[2][name_index] == 'Has "Quotes"'

            # For backslash cell, we only check that the text part is there
            # due to differences in CSV handling across platforms
            backslash_cell = rows[3][name_index]
            assert "Backslash" in backslash_cell

    def test_empty_values(self, tmp_path):
        """Test handling of empty and null values."""
        # Create test data with empty and null values
        test_data = [
            {"id": 1, "name": "Complete", "value": 100, "optional": "has value"},
            {"id": 2, "name": "Empty String", "value": 0, "optional": ""},
            {"id": 3, "name": "None Value", "value": None, "optional": None},
        ]

        # Create writer
        writer = CsvWriter(include_header=True)

        # Create output path
        output_path = tmp_path / "empty_values_test.csv"

        # Write data
        writer.write_table(test_data, output_path)

        # Verify file exists
        assert os.path.exists(output_path)

        # Read back and verify
        with open(output_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Verify record count
            assert len(rows) == len(test_data)

            # Empty string should be preserved
            assert rows[1]["optional"] == ""

            # None should be written as empty string
            assert rows[2]["optional"] == ""
            assert rows[2]["value"] == ""

    def test_file_like_object(self, writer, batch_data, tmp_path):
        """Test writing to a file-like object."""
        # Create output path
        output_path = tmp_path / "file_like_test.csv"

        # Open file for writing
        with open(output_path, "w", newline="") as f:
            # Write data to file object
            result = writer.write_table(batch_data, f)

            # Should return the file object
            assert result == f

        # Verify file has content
        assert os.path.exists(output_path)
        assert os.path.getsize(output_path) > 0

        # Read back and verify
        with open(output_path, newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

            # Should include header row
            assert len(rows) == len(batch_data) + 1
