"""
Tests for the writer factory module.

This module tests the writer factory's ability to manage writers.
"""

import importlib
import sys
from unittest import mock

import pytest

from transmog.io.writer_factory import WriterFactory
from transmog.io import DataWriter


class MockWriter(DataWriter):
    """Mock writer class for testing."""

    @classmethod
    def format_name(cls) -> str:
        """Return the format name."""
        return "mock"

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer is available."""
        return True

    def __init__(self, **options):
        """Initialize with options."""
        self.options = options

    def write_table(self, table_data, output_path, **kwargs):
        """Mock write method."""
        return "mock_output"

    def write_all_tables(
        self, main_table, child_tables, base_path, entity_name, **kwargs
    ):
        """Mock write all tables method."""
        return {"main": "mock_output"}


class UnavailableWriter(DataWriter):
    """Mock writer class that is not available."""

    @classmethod
    def format_name(cls) -> str:
        """Return the format name."""
        return "unavailable"

    @classmethod
    def is_available(cls) -> bool:
        """Check if this writer is available."""
        return False

    def __init__(self, **options):
        """Initialize with options."""
        raise ImportError("This writer is not available")

    def write_table(self, table_data, output_path, **kwargs):
        """Mock write method."""
        return None

    def write_all_tables(
        self, main_table, child_tables, base_path, entity_name, **kwargs
    ):
        """Mock write all tables method."""
        return None


class TestWriterFactory:
    """Test class for the WriterFactory."""

    def setup_method(self):
        """Set up test environment."""
        # Clear the registry before each test
        WriterFactory._writers = {}

    def test_register(self):
        """Test registering a writer class."""
        # Register the mock writer
        WriterFactory.register("mock", MockWriter)

        # Check registration
        assert "mock" in WriterFactory._writers
        assert WriterFactory._writers["mock"] == MockWriter

    def test_create_writer(self):
        """Test creating a writer instance."""
        # Register the mock writer
        WriterFactory.register("mock", MockWriter)

        # Create a writer
        writer = WriterFactory.create_writer("mock", option1="value1")

        # Check results
        assert isinstance(writer, MockWriter)
        assert writer.options["option1"] == "value1"

    def test_create_writer_unavailable(self):
        """Test creating a writer instance for an unavailable format."""
        # Try to create a writer for a non-registered format
        writer = WriterFactory.create_writer("nonexistent")

        # Should return None, not raise an exception
        assert writer is None

    def test_get_writer_class(self):
        """Test getting a writer class without instantiating it."""
        # Register the mock writer
        WriterFactory.register("mock", MockWriter)

        # Get the writer class
        writer_class = WriterFactory.get_writer_class("mock")

        # Check result
        assert writer_class == MockWriter

        # Test non-existent format
        assert WriterFactory.get_writer_class("nonexistent") is None

    def test_list_available_formats(self):
        """Test listing available formats."""
        # Register writers
        WriterFactory.register("mock", MockWriter)
        WriterFactory.register("another", MockWriter)

        # List available formats
        formats = WriterFactory.list_available_formats()

        # Check results - order not guaranteed
        assert set(formats) == {"mock", "another"}

    def test_is_format_available(self):
        """Test checking if a format is available."""
        # Register the mock writer
        WriterFactory.register("mock", MockWriter)

        # Check availability
        assert WriterFactory.is_format_available("mock") is True
        assert WriterFactory.is_format_available("nonexistent") is False
