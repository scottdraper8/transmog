"""
Tests for the JSON streaming writer.

This module tests the JsonStreamingWriter implementation.
"""

import io
import json
import os

import pytest

from tests.interfaces.test_streaming_writer_interface import AbstractStreamingWriterTest
from transmog.io.writers.json import JsonStreamingWriter


class TestJsonStreamingWriter(AbstractStreamingWriterTest):
    """Test class for JsonStreamingWriter."""

    @pytest.fixture
    def writer_class(self):
        """Return the writer class being tested."""
        return JsonStreamingWriter

    @pytest.fixture
    def writer_options(self):
        """Return options for initializing the writer."""
        return {"indent": 2, "entity_name": "test_entity", "use_orjson": False}

    def test_write_to_memory_content(self, memory_writer, sample_records):
        """Test that writing to memory produces valid JSON content."""
        writer, buffer = memory_writer

        # Write records
        writer.initialize_main_table()
        writer.write_main_records(sample_records)
        writer.finalize()

        # Verify content
        buffer.seek(0)
        content = buffer.getvalue()

        # Convert binary content to string
        content_str = content.decode("utf-8")

        # Parse JSON
        json_data = json.loads(content_str)

        # Verify data
        assert isinstance(json_data, list)
        assert len(json_data) == len(sample_records)

        # Check record content
        for i, record in enumerate(sample_records):
            assert json_data[i]["id"] == record["id"]
            assert json_data[i]["name"] == record["name"]
            assert json_data[i]["value"] == record["value"]

    def test_file_naming_convention(self, writer_instance, sample_records, temp_dir):
        """Test that files are named according to convention."""
        writer = writer_instance

        # Write main and child records
        writer.initialize_main_table()
        writer.initialize_child_table("child_table")

        writer.write_main_records(sample_records)
        writer.write_child_records("child_table", [{"id": 101, "value": "test"}])

        writer.finalize()

        # Verify file names
        main_file = os.path.join(temp_dir, "test_entity.json")
        child_file = os.path.join(temp_dir, "child_table.json")

        assert os.path.exists(main_file)
        assert os.path.exists(child_file)

        # Verify file contents
        with open(main_file) as f:
            main_data = json.load(f)
            assert len(main_data) == len(sample_records)

        with open(child_file) as f:
            child_data = json.load(f)
            assert len(child_data) == 1
            assert child_data[0]["id"] == 101

    def test_indentation_option(self):
        """Test that indentation is applied correctly."""
        # No indentation
        buffer_no_indent = io.StringIO()
        writer_no_indent = JsonStreamingWriter(
            destination=buffer_no_indent, indent=None, use_orjson=False
        )

        # With indentation
        buffer_with_indent = io.StringIO()
        writer_with_indent = JsonStreamingWriter(
            destination=buffer_with_indent, indent=2, use_orjson=False
        )

        # Test data
        test_record = {"id": 1, "name": "Test"}

        # Write to both
        writer_no_indent.initialize_main_table()
        writer_no_indent.write_main_records([test_record])
        writer_no_indent.finalize()

        writer_with_indent.initialize_main_table()
        writer_with_indent.write_main_records([test_record])
        writer_with_indent.finalize()

        # Get output
        buffer_no_indent.seek(0)
        content_no_indent = buffer_no_indent.getvalue()

        buffer_with_indent.seek(0)
        content_with_indent = buffer_with_indent.getvalue()

        # Non-indented should be more compact
        assert len(content_no_indent) < len(content_with_indent)

        # Both should parse as valid JSON
        assert json.loads(content_no_indent) == json.loads(content_with_indent)

    def test_combined_batches(self, writer_instance, sample_records, temp_dir):
        """Test that multiple batches combine correctly."""
        writer = writer_instance

        # Split records
        batch1 = sample_records[:1]
        batch2 = sample_records[1:]

        # Write batches
        writer.initialize_main_table()
        writer.write_main_records(batch1)
        writer.write_main_records(batch2)
        writer.finalize()

        # Verify combined content
        main_file = os.path.join(temp_dir, "test_entity.json")
        with open(main_file) as f:
            data = json.load(f)
            assert len(data) == len(sample_records)

            # Verify order is preserved
            for i, record in enumerate(sample_records):
                assert data[i]["id"] == record["id"]

    def test_empty_batches(self, temp_dir):
        """Test handling empty record lists."""
        buffer = io.StringIO()
        writer = JsonStreamingWriter(destination=buffer, use_orjson=False)

        # Initialize and write empty records
        writer.initialize_main_table()
        writer.write_main_records([])
        writer.finalize()

        # Verify output
        buffer.seek(0)
        content = buffer.getvalue()

        # Should still be valid JSON
        data = json.loads(content)
        assert isinstance(data, list)
        assert len(data) == 0

    def test_handles_special_characters(self, memory_writer):
        """Test writing records with special characters."""
        writer, buffer = memory_writer

        # Records with special characters
        records = [
            {"id": 1, "text": 'Special "quotes" and backslash \\'},
            {"id": 2, "text": "New\nline and tab\t characters"},
            {"id": 3, "text": "Unicode ☺ characters"},
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

        # Parse JSON
        json_data = json.loads(content_str)

        # Verify data
        assert isinstance(json_data, list)
        assert len(json_data) == len(records)

        # Check special characters
        assert json_data[0]["text"] == records[0]["text"]
        assert json_data[1]["text"] == records[1]["text"]
        assert json_data[2]["text"] == records[2]["text"]
