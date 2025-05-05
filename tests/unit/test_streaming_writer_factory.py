"""
Unit tests for the streaming writer factory.

Tests the functionality to register, create, and query streaming writers.
"""

import os
import io
import tempfile
import pytest
from unittest import mock

from transmog.io.writer_factory import (
    register_streaming_writer,
    create_streaming_writer,
    get_supported_streaming_formats,
    is_streaming_format_available,
)
from transmog.io.writer_interface import StreamingWriter
from transmog.io.writers.json import JsonStreamingWriter
from transmog.error import MissingDependencyError, ConfigurationError


class CustomStreamingWriter(StreamingWriter):
    """Custom streaming writer for testing."""

    def __init__(self, destination=None, entity_name="entity", **options):
        self.destination = destination
        self.entity_name = entity_name
        self.options = options
        self.is_initialized = False
        self.is_finalized = False
        self.main_records = []
        self.child_tables = {}

    def initialize_main_table(self):
        self.is_initialized = True

    def initialize_child_table(self, table_name):
        self.child_tables[table_name] = []

    def write_main_records(self, records):
        self.main_records.extend(records)

    def write_child_records(self, table_name, records):
        if table_name not in self.child_tables:
            self.initialize_child_table(table_name)
        self.child_tables[table_name].extend(records)

    def finalize(self):
        self.is_finalized = True

    def close(self):
        pass


class TestStreamingWriterFactory:
    """Tests for the streaming writer factory functionality."""

    def test_register_streaming_writer(self):
        """Test registering a custom streaming writer."""
        # Make sure the format doesn't exist before registering
        assert not is_streaming_format_available("custom_format")

        # Register the custom writer
        register_streaming_writer("custom_format", CustomStreamingWriter)

        # Verify registration
        assert is_streaming_format_available("custom_format")
        assert "custom_format" in get_supported_streaming_formats()

        # Clean up - unregister the format
        # Note: There's no public unregister function, but we can access the registry
        from transmog.io.writer_factory import _STREAMING_WRITER_REGISTRY

        if "custom_format" in _STREAMING_WRITER_REGISTRY:
            del _STREAMING_WRITER_REGISTRY["custom_format"]

    def test_create_streaming_writer(self):
        """Test creating a streaming writer."""
        # Register the custom writer
        register_streaming_writer("custom_format", CustomStreamingWriter)

        try:
            # Create the writer
            writer = create_streaming_writer(
                format_name="custom_format",
                destination="test_destination",
                entity_name="test_entity",
                custom_option="value",
            )

            # Verify writer properties
            assert isinstance(writer, CustomStreamingWriter)
            assert writer.destination == "test_destination"
            assert writer.entity_name == "test_entity"
            assert writer.options.get("custom_option") == "value"

        finally:
            # Clean up
            from transmog.io.writer_factory import _STREAMING_WRITER_REGISTRY

            if "custom_format" in _STREAMING_WRITER_REGISTRY:
                del _STREAMING_WRITER_REGISTRY["custom_format"]

    def test_create_streaming_writer_case_insensitive(self):
        """Test that format names are case-insensitive."""
        # Register with lowercase
        register_streaming_writer("mixed_case", CustomStreamingWriter)

        try:
            # Create with uppercase
            writer1 = create_streaming_writer(
                format_name="MIXED_CASE", entity_name="test1"
            )

            # Create with mixed case
            writer2 = create_streaming_writer(
                format_name="Mixed_Case", entity_name="test2"
            )

            # Verify both writers
            assert isinstance(writer1, CustomStreamingWriter)
            assert isinstance(writer2, CustomStreamingWriter)
            assert writer1.entity_name == "test1"
            assert writer2.entity_name == "test2"

        finally:
            # Clean up
            from transmog.io.writer_factory import _STREAMING_WRITER_REGISTRY

            if "mixed_case" in _STREAMING_WRITER_REGISTRY:
                del _STREAMING_WRITER_REGISTRY["mixed_case"]

    def test_format_not_available(self):
        """Test behavior when requested format is not available."""
        # Try to create a writer for non-existent format
        with pytest.raises(ConfigurationError):
            create_streaming_writer(
                format_name="non_existent_format", entity_name="test"
            )

    def test_get_supported_streaming_formats(self):
        """Test getting supported streaming formats."""
        # Get initial formats
        initial_formats = get_supported_streaming_formats()

        # Register a new format
        register_streaming_writer("test_format", CustomStreamingWriter)

        try:
            # Get updated formats
            updated_formats = get_supported_streaming_formats()

            # Verify the new format is included
            assert "test_format" in updated_formats
            assert len(updated_formats) == len(initial_formats) + 1

        finally:
            # Clean up
            from transmog.io.writer_factory import _STREAMING_WRITER_REGISTRY

            if "test_format" in _STREAMING_WRITER_REGISTRY:
                del _STREAMING_WRITER_REGISTRY["test_format"]

    def test_is_streaming_format_available(self):
        """Test checking if a streaming format is available."""
        # Check format that doesn't exist
        assert not is_streaming_format_available("non_existent_format")

        # Register a format
        register_streaming_writer("available_format", CustomStreamingWriter)

        try:
            # Check format that exists
            assert is_streaming_format_available("available_format")

            # Check case insensitivity
            assert is_streaming_format_available("AVAILABLE_FORMAT")
            assert is_streaming_format_available("Available_Format")

        finally:
            # Clean up
            from transmog.io.writer_factory import _STREAMING_WRITER_REGISTRY

            if "available_format" in _STREAMING_WRITER_REGISTRY:
                del _STREAMING_WRITER_REGISTRY["available_format"]

    def test_create_json_streaming_writer(self):
        """Test creating a JSON streaming writer."""
        # JSON writer should be registered by default
        assert is_streaming_format_available("json")

        # Create writer with in-memory buffer
        buffer = io.StringIO()
        writer = create_streaming_writer(
            format_name="json", destination=buffer, entity_name="json_test", indent=4
        )

        # Verify writer properties
        assert isinstance(writer, JsonStreamingWriter)
        assert writer.indent == 4
        assert writer.entity_name == "json_test"

        # Test writing
        writer.initialize_main_table()
        writer.write_main_records([{"id": 1, "name": "Test"}])
        writer.finalize()

        # Verify output
        buffer.seek(0)
        assert "Test" in buffer.getvalue()
        assert "id" in buffer.getvalue()

        writer.close()

    def test_writer_creation_with_dependency_error(self):
        """Test error handling when writer creation fails due to missing dependency."""

        # Create a mock writer class that raises MissingDependencyError
        class DependentStreamingWriter(StreamingWriter):
            def __init__(self, *args, **kwargs):
                raise MissingDependencyError(
                    "Missing required dependency", package="test_dep"
                )

            def initialize_main_table(self):
                pass

            def initialize_child_table(self, table_name):
                pass

            def write_main_records(self, records):
                pass

            def write_child_records(self, table_name, records):
                pass

            def finalize(self):
                pass

            def close(self):
                pass

        # Register the dependent writer
        register_streaming_writer("dependent_format", DependentStreamingWriter)

        try:
            # Check that the format is registered
            assert "dependent_format" in get_supported_streaming_formats()

            # Creating the writer should raise an informative error
            with pytest.raises(MissingDependencyError):
                create_streaming_writer(
                    format_name="dependent_format", entity_name="test"
                )

        finally:
            # Clean up
            from transmog.io.writer_factory import _STREAMING_WRITER_REGISTRY

            if "dependent_format" in _STREAMING_WRITER_REGISTRY:
                del _STREAMING_WRITER_REGISTRY["dependent_format"]

    def test_streaming_writer_integration(self):
        """Integration test for streaming writer factory and custom writer."""
        # Register the custom writer
        register_streaming_writer("integration_test", CustomStreamingWriter)

        try:
            # Create the writer
            writer = create_streaming_writer(
                format_name="integration_test", entity_name="test_entity"
            )

            # Initialize and write data
            writer.initialize_main_table()
            writer.write_main_records([{"id": 1, "name": "Main Record"}])

            writer.initialize_child_table("child_table")
            writer.write_child_records(
                "child_table",
                [
                    {"id": 101, "parent_id": 1, "name": "Child 1"},
                    {"id": 102, "parent_id": 1, "name": "Child 2"},
                ],
            )

            writer.finalize()
            writer.close()

            # Verify writer state
            assert writer.is_initialized
            assert writer.is_finalized
            assert len(writer.main_records) == 1
            assert writer.main_records[0]["name"] == "Main Record"
            assert len(writer.child_tables) == 1
            assert len(writer.child_tables["child_table"]) == 2

        finally:
            # Clean up
            from transmog.io.writer_factory import _STREAMING_WRITER_REGISTRY

            if "integration_test" in _STREAMING_WRITER_REGISTRY:
                del _STREAMING_WRITER_REGISTRY["integration_test"]

    def test_writer_creation_with_unknown_destination(self):
        """Test creating a writer with an unknown destination type."""
        # JSON writer should handle various destination types

        # Test with string path
        with tempfile.TemporaryDirectory() as temp_dir:
            writer1 = create_streaming_writer(
                format_name="json", destination=temp_dir, entity_name="test1"
            )
            assert isinstance(writer1, JsonStreamingWriter)
            writer1.close()

        # Test with file-like object
        buffer = io.StringIO()
        writer2 = create_streaming_writer(
            format_name="json", destination=buffer, entity_name="test2"
        )
        assert isinstance(writer2, JsonStreamingWriter)
        writer2.close()

        # Test with None (should use memory)
        writer3 = create_streaming_writer(
            format_name="json", destination=None, entity_name="test3"
        )
        assert isinstance(writer3, JsonStreamingWriter)
        writer3.close()
