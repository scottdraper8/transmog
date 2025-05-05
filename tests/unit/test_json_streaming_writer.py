"""
Tests for the JSON streaming writer functionality.

These tests verify that the JSON streaming writer works correctly with various
output formats and configurations.
"""

import os
import json
import tempfile
import io
from unittest import mock
import pytest
from transmog.io.writers.json import JsonStreamingWriter


class TestJsonStreamingWriter:
    """Tests for the JSON streaming writer functionality."""

    def test_initialization(self):
        """Test that the streaming writer initializes correctly."""
        writer = JsonStreamingWriter()
        assert writer is not None
        assert writer.entity_name == "entity"
        assert writer.indent == 2

        # Test custom parameters
        writer = JsonStreamingWriter(entity_name="custom", indent=4)
        assert writer.entity_name == "custom"
        assert writer.indent == 4

    def test_streaming_write_to_memory(self):
        """Test streaming write to an in-memory buffer."""
        # Setup test data
        records = [
            {"id": 1, "name": "Record1"},
            {"id": 2, "name": "Record2"},
            {"id": 3, "name": "Record3"},
        ]

        # Create a memory buffer
        buffer = io.StringIO()

        # Create writer with the buffer
        writer = JsonStreamingWriter(destination=buffer, indent=None)

        # Initialize, write records, and finalize
        writer.initialize_main_table()
        writer.write_main_records(records[:2])  # First batch
        writer.write_main_records(records[2:])  # Second batch
        writer.finalize()

        # Check buffer content
        buffer.seek(0)
        content = json.loads(buffer.getvalue())
        assert content == records

        # Clean up
        writer.close()

    def test_streaming_write_to_files(self):
        """Test streaming write to files in a directory."""
        # Setup test data
        main_records = [
            {"id": 1, "name": "Main1"},
            {"id": 2, "name": "Main2"},
        ]

        child_records = [
            {"id": 101, "parent_id": 1, "name": "Child1"},
            {"id": 102, "parent_id": 2, "name": "Child2"},
        ]

        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create writer
            writer = JsonStreamingWriter(
                destination=temp_dir, entity_name="test_entity"
            )

            # Write main and child tables
            writer.initialize_main_table()
            writer.initialize_child_table("children")

            # Write records in batches to simulate streaming
            writer.write_main_records(main_records[:1])
            writer.write_main_records(main_records[1:])

            writer.write_child_records("children", child_records[:1])
            writer.write_child_records("children", child_records[1:])

            writer.finalize()
            writer.close()

            # Verify files exist
            main_file = os.path.join(temp_dir, "test_entity.json")
            child_file = os.path.join(temp_dir, "children.json")

            assert os.path.exists(main_file)
            assert os.path.exists(child_file)

            # Verify content
            with open(main_file, "r") as f:
                content = json.load(f)
                assert content == main_records

            with open(child_file, "r") as f:
                content = json.load(f)
                assert content == child_records

    def test_empty_records(self):
        """Test handling empty record batches."""
        buffer = io.StringIO()
        writer = JsonStreamingWriter(destination=buffer)

        writer.initialize_main_table()
        writer.write_main_records([])  # Empty batch should be handled
        writer.write_main_records([{"id": 1, "name": "Test"}])
        writer.write_main_records([])  # Another empty batch
        writer.finalize()

        buffer.seek(0)
        content = json.loads(buffer.getvalue())
        assert content == [{"id": 1, "name": "Test"}]

        writer.close()

    def test_error_handling(self):
        """Test error handling during streaming writes."""
        # Setup test data
        records = [{"id": 1, "name": "Test"}]

        # Create a custom mock with a write method that raises an error
        class MockStreamWithError:
            def __init__(self):
                pass

            def write(self, data):
                raise IOError("Mock IO Error")

            def flush(self):
                pass

            def close(self):
                pass

        # Use the custom mock
        mock_stream = MockStreamWithError()
        writer = JsonStreamingWriter(destination=mock_stream)

        # The error should be raised during initialization
        with pytest.raises(IOError):
            writer.initialize_main_table()

        # The following shouldn't be executed due to the exception, but if it is,
        # it should also raise an exception
        with pytest.raises(IOError):
            writer.write_main_records(records)

        writer.close()

    def test_orjson_fallback(self):
        """Test fallback to standard library if orjson is not available."""
        records = [{"id": 1, "name": "Test"}]

        # Force use of standard library
        buffer = io.StringIO()
        writer = JsonStreamingWriter(destination=buffer, use_orjson=False)

        writer.initialize_main_table()
        writer.write_main_records(records)
        writer.finalize()

        buffer.seek(0)
        content = json.loads(buffer.getvalue())
        assert content == records

        writer.close()

    def test_multiple_tables(self):
        """Test writing to multiple tables concurrently."""
        # Setup test data
        main_data = [{"id": 1, "name": "Main"}]
        child1_data = [{"id": 101, "parent_id": 1, "name": "Child1"}]
        child2_data = [{"id": 201, "parent_id": 1, "name": "Child2"}]

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = JsonStreamingWriter(destination=temp_dir)

            # Initialize all tables
            writer.initialize_main_table()
            writer.initialize_child_table("child1")
            writer.initialize_child_table("child2")

            # Write to tables in an interleaved pattern
            writer.write_main_records(main_data)
            writer.write_child_records("child1", child1_data)
            writer.write_child_records("child2", child2_data)

            writer.finalize()
            writer.close()

            # Verify all files have correct content
            main_file = os.path.join(temp_dir, "entity.json")
            child1_file = os.path.join(temp_dir, "child1.json")
            child2_file = os.path.join(temp_dir, "child2.json")

            with open(main_file, "r") as f:
                assert json.load(f) == main_data

            with open(child1_file, "r") as f:
                assert json.load(f) == child1_data

            with open(child2_file, "r") as f:
                assert json.load(f) == child2_data
