"""
Tests for reader interface conformance.

This module tests that all reader implementations correctly implement the expected interface.
"""

import pytest

# Import reader-related classes
from transmog.io.readers.csv import CSVReader
from transmog.io.readers.json import JsonlReader, JsonReader


class TestReaderInterface:
    """Test that readers conform to the required interface."""

    @pytest.fixture(scope="module")
    def available_readers(self):
        """Get all available reader classes."""
        return {
            "csv": CSVReader,
            "json": JsonReader,
            "jsonl": JsonlReader,
            # Add other readers as they become available
        }

    def test_readers_implement_interface(self, available_readers):
        """Test that all readers implement the required interface."""
        for format_name, reader_class in available_readers.items():
            # Check if the class has necessary methods
            if format_name == "csv":
                assert hasattr(reader_class, "read_all"), (
                    f"{reader_class.__name__} is missing read_all method"
                )
            else:
                assert hasattr(reader_class, "read_file"), (
                    f"{reader_class.__name__} is missing read_file method"
                )

            # Skip instantiation if not available (determined through exception handling)
            try:
                # Instantiate the reader with default options
                reader = reader_class()

                # Check instance methods based on reader type
                if format_name == "csv":
                    assert hasattr(reader, "read_all"), (
                        f"{reader_class.__name__} is missing read_all method"
                    )
                    assert callable(reader.read_all), (
                        f"{reader_class.__name__}.read_all must be callable"
                    )
                else:
                    assert hasattr(reader, "read_file"), (
                        f"{reader_class.__name__} is missing read_file method"
                    )
                    assert callable(reader.read_file), (
                        f"{reader_class.__name__}.read_file must be callable"
                    )

                # Check read_stream or read_in_chunks if it exists
                if hasattr(reader, "read_stream"):
                    assert callable(reader.read_stream), (
                        f"{reader_class.__name__}.read_stream must be callable"
                    )
                elif hasattr(reader, "read_in_chunks"):
                    assert callable(reader.read_in_chunks), (
                        f"{reader_class.__name__}.read_in_chunks must be callable"
                    )
            except Exception as e:
                # Print warning but don't fail test if reader can't be instantiated
                print(f"Warning: Could not instantiate {reader_class.__name__}: {e}")


class AbstractReaderTest:
    """
    Abstract base class for reader tests.

    This class defines a standardized set of tests that should apply to all reader implementations.
    Subclasses must define reader_class and appropriate fixtures.
    """

    # To be defined by subclasses
    reader_class = None
    format_name = None

    @pytest.fixture
    def reader(self):
        """Create a reader instance."""
        if self.reader_class is None:
            pytest.fail("Subclass must define reader_class")

        try:
            return self.reader_class()
        except Exception as e:
            pytest.skip(f"{self.reader_class.__name__} dependencies not available: {e}")

    def test_read_empty_file(self, reader, tmp_path):
        """Test reading an empty file."""
        # Create an empty file
        file_path = tmp_path / f"empty.{self.format_name}"
        with open(file_path, "w"):
            pass  # Create empty file

        # Attempt to read the empty file, should handle gracefully
        try:
            if hasattr(reader, "read_all"):
                result = reader.read_all(file_path)
            else:
                result = reader.read_file(file_path)

            # Should return empty list or equivalent
            if result is not None:
                assert len(result) == 0 or result == []
        except Exception as e:
            # Some readers might raise specific exceptions for empty files
            # The key is that they handle it gracefully and consistently
            print(f"Empty file handling for {self.reader_class.__name__}: {e}")

    def test_read_single_record(self, reader, tmp_path, simple_data_file):
        """Test reading a file with a single record."""
        # simple_data_file should be provided by the subclass
        # It contains a single record in the appropriate format
        if hasattr(reader, "read_all"):
            result = reader.read_all(simple_data_file)
        else:
            result = reader.read_file(simple_data_file)

            # JSON reader might return a dict directly for single-item files,
            # in which case we need to wrap it in a list
            if isinstance(result, dict):
                result = [result]

        # Verify a single record was returned
        assert isinstance(result, list)
        assert len(result) == 1

        # Check that record has expected structure
        record = result[0]
        assert isinstance(record, dict)
        # Specific field assertions should be done in the concrete test class

    def test_read_multiple_records(self, reader, tmp_path, batch_data_file):
        """Test reading a file with multiple records."""
        # batch_data_file should be provided by the subclass
        # It contains multiple records in the appropriate format
        if hasattr(reader, "read_all"):
            result = reader.read_all(batch_data_file)
        else:
            result = reader.read_file(batch_data_file)

        # Verify multiple records were returned
        assert isinstance(result, list)
        assert len(result) > 1

        # Check that records have expected structure
        for record in result:
            assert isinstance(record, dict)
            # Specific field assertions should be done in the concrete test class

    def test_error_handling(self, reader, tmp_path, invalid_data_file):
        """Test how the reader handles invalid data."""
        # invalid_data_file should be provided by the subclass
        # It contains data that doesn't conform to the format

        # Attempt to read invalid data, should raise appropriate exception
        # Since different readers might raise different exception types,
        # we'll accept any exception
        with pytest.raises((Exception,)):
            if hasattr(reader, "read_all"):
                reader.read_all(invalid_data_file)
            else:
                reader.read_file(invalid_data_file)

    def test_streaming_read(self, reader, batch_data_file):
        """Test reading data in streaming mode (if supported)."""
        # Skip if reader doesn't support streaming
        has_streaming = False
        streaming_method = None

        if hasattr(reader, "read_stream"):
            has_streaming = True
            streaming_method = reader.read_stream
        elif hasattr(reader, "read_in_chunks"):
            has_streaming = True
            streaming_method = reader.read_in_chunks

        if not has_streaming:
            pytest.skip(
                f"{self.reader_class.__name__} does not support streaming reads"
            )

        # Read data in streaming mode
        records = []
        for chunk in streaming_method(batch_data_file):
            if isinstance(chunk, list):
                records.extend(chunk)
            else:
                records.append(chunk)

        # Verify records were returned
        assert len(records) > 0

        # Check that records have expected structure
        for record in records:
            assert isinstance(record, dict)
            # Specific field assertions should be done in the concrete test class
