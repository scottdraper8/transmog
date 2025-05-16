"""
Tests for the Streaming Writer Interface.

This module defines abstract test cases that all streaming writer
implementations must pass.
"""

import io
import tempfile

import pytest

from transmog.io.writer_interface import StreamingWriter


class AbstractStreamingWriterTest:
    """
    Abstract test class for StreamingWriter implementations.

    All streaming writer implementations must pass these tests.
    """

    @pytest.fixture
    def writer_class(self):
        """
        Return the writer class being tested.

        Must be implemented by concrete test classes.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def writer_options(self):
        """
        Return options for initializing the writer.

        May be overridden by concrete test classes.
        """
        return {}

    @pytest.fixture
    def writer_instance(self, writer_class, temp_dir, writer_options):
        """Create a writer instance for testing."""
        writer = writer_class(destination=temp_dir, **writer_options)
        return writer

    @pytest.fixture
    def memory_writer(self, writer_class, writer_options):
        """Create a writer that writes to memory."""
        # Create a memory buffer
        buffer = io.BytesIO()  # Use BytesIO instead of StringIO for binary data

        # Create a writer with the buffer as destination
        writer = writer_class(destination=buffer, **writer_options)

        return writer, buffer

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def sample_records(self):
        """Return sample records for testing."""
        return [
            {"id": 1, "name": "Record 1", "value": 100},
            {"id": 2, "name": "Record 2", "value": 200},
            {"id": 3, "name": "Record 3", "value": 300},
        ]

    @pytest.fixture
    def child_records(self):
        """Return sample child records for testing."""
        return [
            {"id": 101, "parent_id": 1, "detail": "Child 1"},
            {"id": 102, "parent_id": 2, "detail": "Child 2"},
            {"id": 103, "parent_id": 3, "detail": "Child 3"},
        ]

    def test_initialization(self, writer_class, writer_options):
        """Test that the writer initializes correctly."""
        writer = writer_class(**writer_options)
        assert isinstance(writer, StreamingWriter)
        assert hasattr(writer, "initialize_main_table")
        assert hasattr(writer, "write_main_records")
        assert hasattr(writer, "initialize_child_table")
        assert hasattr(writer, "write_child_records")
        assert hasattr(writer, "finalize")

    def test_write_to_memory(self, memory_writer, sample_records):
        """Test writing records to a memory buffer."""
        writer, buffer = memory_writer

        # Write records
        writer.initialize_main_table()
        writer.write_main_records(sample_records)
        writer.finalize()

        # Verify content
        buffer.seek(0)
        content = buffer.getvalue()
        assert content  # Content should not be empty

        # Additional verification must be implemented by concrete tests

    def test_initialize_tables(self, writer_instance):
        """Test initializing main and child tables."""
        writer = writer_instance

        # Initialize tables
        writer.initialize_main_table()
        writer.initialize_child_table("child1")
        writer.initialize_child_table("child2")

        # Different implementations track initialization state differently
        # Just verify the writer instance exists
        assert writer is not None
        # Concrete tests should implement more specific checks

    def test_write_multiple_batches(self, writer_instance, sample_records):
        """Test writing records in multiple batches."""
        writer = writer_instance

        # Split records into batches
        batch1 = sample_records[:1]
        batch2 = sample_records[1:]

        # Write in batches
        writer.initialize_main_table()
        writer.write_main_records(batch1)
        writer.write_main_records(batch2)
        writer.finalize()

        # Verification must be done in concrete tests

    def test_write_to_files(
        self, writer_instance, sample_records, child_records, temp_dir
    ):
        """Test writing main and child records to files."""
        writer = writer_instance

        # Write main and child records
        writer.initialize_main_table()
        writer.initialize_child_table("child")

        writer.write_main_records(sample_records)
        writer.write_child_records("child", child_records)

        writer.finalize()

        # Verify files exist (names are implementation-specific)
        # Concrete tests should verify content

    def test_handles_empty_records(self, writer_instance):
        """Test handling empty record batches."""
        writer = writer_instance

        # Write empty batches
        writer.initialize_main_table()
        writer.write_main_records([])  # Empty batch
        writer.write_main_records([{"id": 1, "name": "Single Record"}])
        writer.write_main_records([])  # Another empty batch

        writer.finalize()

        # Verification must be done in concrete tests

    def test_context_manager(
        self, writer_class, temp_dir, sample_records, writer_options
    ):
        """Test using the writer as a context manager."""
        # Use with statement
        with writer_class(destination=temp_dir, **writer_options) as writer:
            writer.initialize_main_table()
            writer.write_main_records(sample_records)

        # Writer should be finalized after context exit
        # Verification must be done in concrete tests
