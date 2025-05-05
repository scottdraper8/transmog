"""
Tests for the writer factory module.

This module tests the writer factory's ability to manage writers.
"""

import importlib
import sys
from unittest import mock
import os
import tempfile
from typing import Dict, List, Any, Optional, Union, BinaryIO, TextIO

import pytest

from transmog.io.writer_factory import (
    register_writer,
    create_writer,
    is_format_available,
    WriterFactory,
)
from transmog.io import DataWriter
from transmog.io.writer_interface import DataWriter as WriterInterface
from transmog.error import ConfigurationError
from test_utils import WriterMixin


class MockWriter(WriterMixin, DataWriter):
    """A mock writer for testing."""

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
        self.initialize_called = True

    def write_table(self, data, destination, **options):
        """Mock write_table method."""
        # Combine constructor options with per-call options
        all_options = {**self.options, **options}

        # Return a mock result
        if hasattr(destination, "write"):
            destination.write(b"MOCK_CONTENT")
            return destination
        else:
            # Ensure directory exists
            os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)
            # Write to file
            with open(destination, "wb") as f:
                f.write(b"MOCK_CONTENT")
            return destination

    def write_all_tables(
        self, main_table, child_tables, base_path, entity_name, **options
    ):
        """Mock method for writing all tables."""
        # Create the directory
        os.makedirs(base_path, exist_ok=True)

        result = {}

        # Write main table
        main_path = os.path.join(base_path, f"{entity_name}.mock")
        self.write_table(main_table, main_path, **options)
        result["main"] = main_path

        # Write child tables
        for table_name, table_data in child_tables.items():
            # Replace dots and slashes with underscores for file names
            safe_name = table_name.replace(".", "_").replace("/", "_")
            file_path = os.path.join(base_path, f"{safe_name}.mock")
            self.write_table(table_data, file_path, **options)
            result[table_name] = file_path

        return result


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
    """Tests for the WriterFactory."""

    def setup_method(self):
        """Set up for tests."""
        # Clear registry
        WriterFactory._writers = {}

    def test_registration(self):
        """Test registering a writer class."""
        # Register the mock writer
        WriterFactory.register("mock", MockWriter)

        # Check that it's registered
        assert "mock" in WriterFactory._writers
        assert WriterFactory._writers["mock"] == MockWriter

    def test_create_writer(self):
        """Test creating a writer instance."""
        # Register the mock writer
        WriterFactory.register("mock", MockWriter)

        # Create a writer
        writer = WriterFactory.create_writer("mock", option1="value1")

        # Verify it's the right type
        assert isinstance(writer, MockWriter)
        assert writer.options["option1"] == "value1"

    def test_get_writer_class(self):
        """Test getting a writer class without instantiating."""
        # Register the mock writer
        WriterFactory.register("mock", MockWriter)

        # Get the class
        writer_class = WriterFactory.get_writer_class("mock")

        # Verify it's the right class
        assert writer_class == MockWriter

    def test_list_available_formats(self):
        """Test listing available formats."""
        # Register several mock writers
        WriterFactory.register("mock1", MockWriter)
        WriterFactory.register("mock2", MockWriter)

        # List formats
        formats = WriterFactory.list_available_formats()

        # Verify results
        assert "mock1" in formats
        assert "mock2" in formats

    def test_is_format_available(self):
        """Test checking if a format is available."""
        # Register only one format
        WriterFactory.register("mock1", MockWriter)

        # Check availability
        assert WriterFactory.is_format_available("mock1") is True
        assert WriterFactory.is_format_available("unknown") is False
