"""
Tests for writer interface conformance.

This module tests that all writer implementations correctly implement the expected interface.
"""

import os

import pytest

from transmog.io.writer_factory import FormatRegistry

# Import the writer interface


class TestWriterInterface:
    """Test that writers conform to the required interface."""

    @pytest.fixture(scope="module")
    def registered_writers(self):
        """Get all currently registered writer classes."""
        return FormatRegistry.list_all_writers()

    def test_writers_implement_interface(self, registered_writers):
        """Test that all registered writers implement the required interface."""
        for _format_name, writer_class in registered_writers.items():
            # Check class methods
            assert hasattr(writer_class, "format_name"), (
                f"{writer_class.__name__} is missing format_name class method"
            )
            assert hasattr(writer_class, "is_available"), (
                f"{writer_class.__name__} is missing is_available class method"
            )

            # Check that format_name is callable and returns the format name
            assert callable(writer_class.format_name), (
                f"{writer_class.__name__}.format_name must be callable"
            )
            format_name_value = writer_class.format_name()
            assert isinstance(format_name_value, str), (
                f"{writer_class.__name__}.format_name() must return a string"
            )

            # Check that is_available is callable and returns a boolean
            assert callable(writer_class.is_available), (
                f"{writer_class.__name__}.is_available must be callable"
            )

            # Skip instantiation if not available
            if not writer_class.is_available():
                continue

            # Instantiate the writer
            writer = writer_class()

            # Check instance methods
            assert hasattr(writer, "write"), (
                f"{writer_class.__name__} is missing write method"
            )
            assert callable(writer.write), (
                f"{writer_class.__name__}.write must be callable"
            )

            assert hasattr(writer, "write_table"), (
                f"{writer_class.__name__} is missing write_table method"
            )
            assert callable(writer.write_table), (
                f"{writer_class.__name__}.write_table must be callable"
            )

            assert hasattr(writer, "write_all_tables"), (
                f"{writer_class.__name__} is missing write_all_tables method"
            )
            assert callable(writer.write_all_tables), (
                f"{writer_class.__name__}.write_all_tables must be callable"
            )

    def test_writer_factory_formats(self, registered_writers):
        """Test that FormatRegistry correctly lists available formats."""
        formats = FormatRegistry.list_available_formats()

        # All available writers should be in the list
        for format_name, writer_class in registered_writers.items():
            if writer_class.is_available():
                assert format_name in formats, (
                    f"Available writer {format_name} not listed in available formats"
                )

        # No unavailable writers should be in the list
        for format_name, writer_class in registered_writers.items():
            if not writer_class.is_available():
                assert format_name not in formats, (
                    f"Unavailable writer {format_name} listed in available formats"
                )

    def test_create_writer(self, registered_writers):
        """Test creating writer instances."""
        for format_name, writer_class in registered_writers.items():
            if not writer_class.is_available():
                continue

            # Test creation with no options
            writer = FormatRegistry.create_writer(format_name)
            assert isinstance(writer, writer_class), (
                f"Created writer is not an instance of {writer_class.__name__}"
            )

            # Test creation with options
            options = {"test_option": "test_value"}
            writer = FormatRegistry.create_writer(format_name, **options)
            assert isinstance(writer, writer_class), (
                f"Created writer with options is not an instance of {writer_class.__name__}"
            )


class AbstractWriterTest:
    """
    Abstract base class for writer tests.

    This class defines a standardized set of tests that should apply to all writer implementations.
    Subclasses must define writer_class and appropriate fixtures.
    """

    # To be defined by subclasses
    writer_class = None
    format_name = None

    @pytest.fixture
    def writer(self):
        """Create a writer instance."""
        if self.writer_class is None:
            pytest.fail("Subclass must define writer_class")

        if not self.writer_class.is_available():
            pytest.skip(f"{self.writer_class.__name__} dependencies not available")

        return self.writer_class()

    def test_format_name(self):
        """Test the format_name class method."""
        if self.format_name is not None:
            assert self.writer_class.format_name() == self.format_name
        else:
            assert isinstance(self.writer_class.format_name(), str)

    def test_is_available(self):
        """Test the is_available class method."""
        # This test should always run, since we only skip specific tests that need
        # the actual writer functionality, not tests of the class methods
        assert isinstance(self.writer_class.is_available(), bool)

    def test_write_empty_table(self, writer, tmp_path):
        """Test writing an empty table."""
        # Skip if writer not available
        if not self.writer_class.is_available():
            pytest.skip(f"{self.writer_class.__name__} dependencies not available")

        # Create output path
        output_path = tmp_path / f"empty_table.{self.writer_class.format_name()}"

        # Write empty table
        result = writer.write_table([], output_path)

        # Should create a file
        assert os.path.exists(output_path)

        # Should return the path
        assert result == output_path

    def test_write_single_table(self, writer, simple_data, tmp_path):
        """Test writing a single table."""
        # Skip if writer not available
        if not self.writer_class.is_available():
            pytest.skip(f"{self.writer_class.__name__} dependencies not available")

        # Convert to list of records
        records = [simple_data]

        # Create output path
        output_path = tmp_path / f"single_table.{self.writer_class.format_name()}"

        # Write table
        result = writer.write_table(records, output_path)

        # Should create a file
        assert os.path.exists(output_path)

        # Should return the path
        assert result == output_path

    def test_write_all_tables(self, writer, simple_data, batch_data, tmp_path):
        """Test writing multiple tables."""
        # Skip if writer not available
        if not self.writer_class.is_available():
            pytest.skip(f"{self.writer_class.__name__} dependencies not available")

        # Prepare test data
        main_table = [simple_data]
        child_tables = {
            "child_table": batch_data,
            "empty_table": [],
        }

        # Create output directory
        output_dir = tmp_path / "tables"

        # Write all tables
        results = writer.write_all_tables(
            main_table=main_table,
            child_tables=child_tables,
            base_path=output_dir,
            entity_name="test_entity",
        )

        # Should return mapping of table names to file paths
        assert isinstance(results, dict)
        assert "main" in results
        assert "child_table" in results
        assert "empty_table" in results

        # Should create files
        for path in results.values():
            assert os.path.exists(path)
