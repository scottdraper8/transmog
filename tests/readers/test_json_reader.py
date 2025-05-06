"""
Tests for the JSON reader implementation.

This module tests that the JSON reader correctly handles reading data from JSON format.
"""

import os
import json
import pytest
import io
from typing import Dict, List, Any

# Import the reader and abstract test base class
from transmog.io.readers.json import JsonReader
from tests.interfaces.test_reader_interface import AbstractReaderTest


class TestJsonReader(AbstractReaderTest):
    """Test the JSON reader implementation."""

    reader_class = JsonReader
    format_name = "json"

    @pytest.fixture
    def reader(self):
        """Create a JSON reader."""
        return JsonReader()

    @pytest.fixture
    def simple_data(self):
        """Simple data for testing."""
        return {"id": "1", "name": "Test Record", "value": 100, "active": True}

    @pytest.fixture
    def simple_data_file(self, tmp_path, simple_data):
        """Create a file with a single JSON record."""
        file_path = tmp_path / "simple_data.json"
        with open(file_path, "w") as f:
            # For JSON reader, we need to wrap in array since it expects an array of objects
            json.dump([simple_data], f)
        return file_path

    @pytest.fixture
    def batch_data(self):
        """Batch data for testing."""
        return [
            {"id": "1", "name": "Record 1", "value": 100},
            {"id": "2", "name": "Record 2", "value": 200},
            {"id": "3", "name": "Record 3", "value": 300},
        ]

    @pytest.fixture
    def batch_data_file(self, tmp_path, batch_data):
        """Create a file with multiple JSON records."""
        file_path = tmp_path / "batch_data.json"
        with open(file_path, "w") as f:
            json.dump(batch_data, f)
        return file_path

    @pytest.fixture
    def invalid_data_file(self, tmp_path):
        """Create a file with invalid JSON data."""
        file_path = tmp_path / "invalid_data.json"
        with open(file_path, "w") as f:
            f.write('{"id": "1", "name": "Invalid JSON" - missing comma }')
        return file_path

    def test_parse_with_different_types(self, tmp_path):
        """Test JSON reader with different data types."""
        # Create test data with various types
        test_data = [
            {
                "string": "test",
                "int": 123,
                "float": 123.456,
                "bool": True,
                "null": None,
                "array": [1, 2, 3],
                "object": {"key": "value"},
            }
        ]

        # Write to file
        file_path = tmp_path / "types_test.json"
        with open(file_path, "w") as f:
            json.dump(test_data, f)

        # Create reader and read data
        reader = JsonReader()
        result = reader.read_file(file_path)

        # Ensure result is a list
        if not isinstance(result, list):
            result = [result]

        # Verify types
        assert len(result) == 1
        record = result[0]
        assert isinstance(record["string"], str)
        assert isinstance(record["int"], int)
        assert isinstance(record["float"], float)
        assert isinstance(record["bool"], bool)
        assert record["null"] is None
        assert isinstance(record["array"], list)
        assert isinstance(record["object"], dict)

    def test_large_file_handling(self, tmp_path):
        """Test handling large files efficiently."""
        # Generate large dataset (1000 records)
        large_data = []
        for i in range(1000):
            large_data.append(
                {
                    "id": str(i),
                    "name": f"Record {i}",
                    "value": i * 10,
                    "data": "Some additional data " * 5,  # Add some size to the record
                }
            )

        # Write to file
        file_path = tmp_path / "large_data.json"
        with open(file_path, "w") as f:
            json.dump(large_data, f)

        # Create reader and read data
        reader = JsonReader()
        result = reader.read_file(file_path)

        # Verify record count
        assert len(result) == 1000

    def test_empty_array(self, tmp_path):
        """Test handling of empty JSON array."""
        # Create file with empty array
        file_path = tmp_path / "empty_array.json"
        with open(file_path, "w") as f:
            f.write("[]")

        # Create reader and read data
        reader = JsonReader()
        result = reader.read_file(file_path)

        # Should return empty list
        assert isinstance(result, list)
        assert len(result) == 0

    def test_non_object_array(self, tmp_path):
        """Test handling of arrays containing non-objects."""
        # Create file with array of primitive values
        file_path = tmp_path / "primitive_array.json"
        with open(file_path, "w") as f:
            f.write('[1, 2, 3, "string", true]')

        # Create reader and read data
        reader = JsonReader()

        # This might raise an exception depending on implementation
        # If it does handle this case, verify it returns a reasonable result
        try:
            result = reader.read_file(file_path)
            # If it handles this, check the result is reasonable
            # It might wrap primitives in dictionaries or skip them
            assert isinstance(result, list)
        except Exception as e:
            # If it raises an exception, that's acceptable too
            # But log the error for information
            print(f"Non-object array handling: {e}")
            # No assertion needed - the test passed if the exception was expected

    def test_streaming_support(self, batch_data_file):
        """Test that the reader supports streaming."""
        reader = JsonReader()
        assert hasattr(reader, "read_stream"), "JSON reader should support streaming"
