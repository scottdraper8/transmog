"""
Tests for the CSV streaming writer.

This module tests the CsvStreamingWriter implementation.
"""

import csv
import io
import os

import pytest

from tests.interfaces.test_streaming_writer_interface import AbstractStreamingWriterTest
from transmog.io.writers.csv import CsvStreamingWriter


class TestCsvStreamingWriter(AbstractStreamingWriterTest):
    """Test class for CsvStreamingWriter."""

    @pytest.fixture
    def writer_class(self):
        """Return the writer class being tested."""
        return CsvStreamingWriter

    @pytest.fixture
    def writer_options(self):
        """Return options for initializing the writer."""
        return {
            "entity_name": "test_entity",
            "delimiter": ",",
            "quotechar": '"',
            "include_header": True,
        }

    def test_write_to_memory_content(self, memory_writer, sample_records):
        """Test that writing to memory produces valid CSV content."""
        writer, buffer = memory_writer

        # Write records
        writer.initialize_main_table()
        writer.write_main_records(sample_records)
        writer.finalize()

        # Verify CSV content
        buffer.seek(0)
        content = buffer.getvalue()
        assert content

        # Parse CSV content (convert bytes to string)
        content_str = content.decode("utf-8")
        lines = content_str.strip().split("\n")

        # Verify headers and number of records
        assert len(lines) == 4  # Header + 3 data rows
        assert lines[0].split(",")[0] == "id"  # Header should include id

        # Verify id values
        id_values = [line.split(",")[0] for line in lines[1:]]
        assert set(id_values) == {"1", "2", "3"}

    def test_initialize_tables(self, writer_instance):
        """Test initializing main and child tables."""
        writer = writer_instance

        # Initialize tables
        writer.initialize_main_table()
        writer.initialize_child_table("child1")
        writer.initialize_child_table("child2")

        # CsvStreamingWriter doesn't do anything on initialization
        # It only creates writers when actual data is written
        # So we just verify the writer instance exists
        assert writer is not None
        assert hasattr(writer, "writers")
        assert hasattr(writer, "headers")
        assert isinstance(writer.writers, dict)
        assert isinstance(writer.headers, dict)

    def test_file_naming_convention(self, writer_instance, sample_records, temp_dir):
        """Test that files are named according to convention."""
        writer = writer_instance

        # Write main and child records
        writer.initialize_main_table()
        writer.initialize_child_table("child_table")

        # Need to actually write records to create files
        writer.write_main_records(sample_records)
        writer.write_child_records("child_table", [{"id": 101, "value": "test"}])

        writer.finalize()

        # Verify file names - use file_paths from the writer object
        assert "main" in writer.file_paths
        assert "child_table" in writer.file_paths

        main_file = writer.file_paths["main"]
        child_file = writer.file_paths["child_table"]

        assert os.path.exists(main_file)
        assert os.path.exists(child_file)

        # Verify file contents
        with open(main_file) as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == len(sample_records) + 1  # +1 for header

        with open(child_file) as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 2  # 1 header + 1 data row

    def test_header_option(self, temp_dir):
        """Test that header option works correctly."""
        # With header
        buffer_with_header = io.StringIO()
        writer_with_header = CsvStreamingWriter(
            destination=buffer_with_header, include_header=True
        )

        # Without header
        buffer_no_header = io.StringIO()
        writer_no_header = CsvStreamingWriter(
            destination=buffer_no_header, include_header=False
        )

        # Test data
        test_record = {"id": 1, "name": "Test"}

        # Write to both
        writer_with_header.initialize_main_table()
        writer_with_header.write_main_records([test_record])
        writer_with_header.finalize()

        writer_no_header.initialize_main_table()
        writer_no_header.write_main_records([test_record])
        writer_no_header.finalize()

        # Get output
        buffer_with_header.seek(0)
        content_with_header = buffer_with_header.getvalue()

        buffer_no_header.seek(0)
        content_no_header = buffer_no_header.getvalue()

        # Header version should have more lines
        lines_with_header = content_with_header.strip().split("\n")
        lines_no_header = content_no_header.strip().split("\n")

        assert len(lines_with_header) == 2  # header + data
        assert len(lines_no_header) == 1  # data only

        # First line of header version should have field names
        assert "id" in lines_with_header[0]
        assert "name" in lines_with_header[0]

    def test_delimiter_option(self, temp_dir):
        """Test that delimiter option works correctly."""
        # Comma delimiter
        buffer_comma = io.StringIO()
        writer_comma = CsvStreamingWriter(destination=buffer_comma, delimiter=",")

        # Tab delimiter
        buffer_tab = io.StringIO()
        writer_tab = CsvStreamingWriter(destination=buffer_tab, delimiter="\t")

        # Test data
        test_record = {"id": 1, "name": "Test"}

        # Write to both
        writer_comma.initialize_main_table()
        writer_comma.write_main_records([test_record])
        writer_comma.finalize()

        writer_tab.initialize_main_table()
        writer_tab.write_main_records([test_record])
        writer_tab.finalize()

        # Get output
        buffer_comma.seek(0)
        content_comma = buffer_comma.getvalue()

        buffer_tab.seek(0)
        content_tab = buffer_tab.getvalue()

        # Comma version should have commas
        assert "," in content_comma
        assert "\t" not in content_comma

        # Tab version should have tabs
        assert "\t" in content_tab
        assert "," not in content_tab

    def test_combined_batches_header_is_not_repeated(
        self, writer_instance, sample_records, temp_dir
    ):
        """Test that header appears only once when writing multiple batches."""
        writer = writer_instance

        # Split records
        batch1 = sample_records[:1]
        batch2 = sample_records[1:]

        # Write batches
        writer.initialize_main_table()
        writer.write_main_records(batch1)
        writer.write_main_records(batch2)
        writer.finalize()

        # Verify combined content - use file path from writer object
        assert "main" in writer.file_paths
        main_file = writer.file_paths["main"]

        with open(main_file) as f:
            lines = f.readlines()

            # Should have header + all data lines
            assert len(lines) == len(sample_records) + 1

            # Count lines with id,name,value pattern (header)
            header_pattern = "id,name,value"
            header_count = sum(1 for line in lines if header_pattern in line)

            # Should only have one header
            assert header_count == 1

    def test_handles_special_characters(self, memory_writer):
        """Test writing records with special characters."""
        writer, buffer = memory_writer

        # Records with special characters
        records = [
            {"id": 1, "text": "Comma, in text"},
            {"id": 2, "text": 'Quote " in text'},
            {"id": 3, "text": "New\nline in text"},
        ]

        # Write records
        writer.initialize_main_table()
        writer.write_main_records(records)
        writer.finalize()

        # Verify content
        buffer.seek(0)
        content = buffer.getvalue()

        # Convert binary content to string
        content_str = content.decode("utf-8")

        # CSV handles newlines by quoting them, so the actual line count will be affected by
        # the newline in the data. Let's just check overall content instead of line count.

        # Check that the headers are present
        assert "id,text" in content_str

        # Check that the special characters are properly handled
        assert "Comma, in text" in content_str
        assert 'Quote "" in text' in content_str  # CSV escapes quotes as double quotes
        assert "New\nline in text" in content_str.replace(
            "\r\n", "\n"
        )  # Handle different newline formats
