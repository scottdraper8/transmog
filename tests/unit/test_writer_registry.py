"""
Tests for the writer registry module.

This module tests the writer registry's ability to manage writers
while avoiding circular dependencies.
"""

import importlib
import sys
from unittest import mock

import pytest

from src.transmog.io.writer_registry import WriterRegistry


class MockWriter:
    """Mock writer class for testing."""

    format_name = "mock"

    @classmethod
    def is_available(cls):
        """Check if writer is available."""
        return True


class UnavailableWriter:
    """Mock writer class that is not available."""

    format_name = "unavailable"

    @classmethod
    def is_available(cls):
        """Check if writer is available."""
        return False


class TestWriterRegistry:
    """Test class for the WriterRegistry."""

    def setup_method(self):
        """Set up test environment."""
        # Clear the registry before each test
        WriterRegistry._writers = {}

    def test_register_direct(self):
        """Test registering a writer class directly."""
        # Register the mock writer
        WriterRegistry.register(MockWriter)

        # Check registration
        assert "mock" in WriterRegistry._writers
        assert WriterRegistry._writers["mock"]["class"] == MockWriter
        assert WriterRegistry._writers["mock"]["loaded"] is True

    def test_register_format(self):
        """Test registering a format without loading the class."""
        # Register a format
        WriterRegistry.register_format("test_format", "test_module", "TestWriter")

        # Check registration
        assert "test_format" in WriterRegistry._writers
        assert WriterRegistry._writers["test_format"]["class"] is None
        assert WriterRegistry._writers["test_format"]["module"] == "test_module"
        assert WriterRegistry._writers["test_format"]["class_name"] == "TestWriter"
        assert WriterRegistry._writers["test_format"]["loaded"] is False

    def test_load_writer_success(self):
        """Test loading a writer class successfully."""
        # Create a mock module
        mock_module = mock.MagicMock()
        mock_module.TestWriter = MockWriter

        # Create a mock import_module function
        with mock.patch.object(importlib, "import_module", return_value=mock_module):
            # Register the format
            WriterRegistry.register_format("test_format", "test_module", "TestWriter")

            # Load the writer
            writer_class = WriterRegistry._load_writer("test_format")

            # Check results
            assert writer_class == MockWriter
            assert WriterRegistry._writers["test_format"]["loaded"] is True
            assert WriterRegistry._writers["test_format"]["class"] == MockWriter

    def test_load_writer_failure(self):
        """Test loading a writer class that fails."""
        # Make import_module raise an exception
        with mock.patch.object(importlib, "import_module", side_effect=ImportError):
            # Register the format
            WriterRegistry.register_format("test_format", "test_module", "TestWriter")

            # Try to load the writer
            writer_class = WriterRegistry._load_writer("test_format")

            # Check results
            assert writer_class is None
            assert WriterRegistry._writers["test_format"]["loaded"] is False

    def test_is_format_available_registered(self):
        """Test checking if a format is available when already registered."""
        # Register the mock writer
        WriterRegistry.register(MockWriter)

        # Check availability
        assert WriterRegistry.is_format_available("mock") is True

    def test_is_format_available_unregistered(self):
        """Test checking if a format is available when not registered."""
        assert WriterRegistry.is_format_available("nonexistent") is False

    def test_is_format_available_lazy_loading(self):
        """Test checking if a format is available with lazy loading."""
        # Create a mock module
        mock_module = mock.MagicMock()
        mock_module.TestWriter = MockWriter

        # Create a mock import_module function
        with mock.patch.object(importlib, "import_module", return_value=mock_module):
            # Register the format
            WriterRegistry.register_format("test_format", "test_module", "TestWriter")

            # Check availability
            assert WriterRegistry.is_format_available("test_format") is True

    def test_create_writer(self):
        """Test creating a writer instance."""
        # Register the mock writer
        WriterRegistry.register(MockWriter)

        # Create a writer
        writer = WriterRegistry.create_writer("mock")

        # Check results
        assert isinstance(writer, MockWriter)

    def test_create_writer_unavailable(self):
        """Test creating a writer instance for an unavailable format."""
        with pytest.raises(ValueError):
            WriterRegistry.create_writer("nonexistent")

    def test_list_available_formats(self):
        """Test listing available formats."""
        # Register writers
        WriterRegistry.register(MockWriter)

        # Setup mocks for import_module with different behaviors based on the module name
        def mock_import_side_effect(module_name):
            if module_name == "test_module":
                mock_module = mock.MagicMock()
                mock_module.TestWriter = MockWriter
                return mock_module
            else:
                raise ImportError(f"No module named '{module_name}'")

        # Mock the import_module function
        with mock.patch.object(
            importlib, "import_module", side_effect=mock_import_side_effect
        ):
            # Register formats
            WriterRegistry.register_format("test_format", "test_module", "TestWriter")
            WriterRegistry.register_format(
                "unavailable", "nonexistent_module", "UnavailableWriter"
            )

            # List available formats
            formats = WriterRegistry.list_available_formats()

            # Check results
            assert set(formats) == {"mock", "test_format"}
            assert "unavailable" not in formats
