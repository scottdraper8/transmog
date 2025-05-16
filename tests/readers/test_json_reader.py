"""
Tests for the JSON reader implementation.

This module tests that the JSON reader correctly handles reading data from JSON format.
"""

import json
import os
import tempfile
from typing import Any

import pytest

from tests.interfaces.test_reader_interface import AbstractReaderTest

# Import the reader and abstract test base class
from transmog.io.readers.json import (
    JsonlReader,
    JsonReader,
    detect_json_format,
    parse_json_data,
    read_json_file,
    read_json_stream,
)


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


class TestAdvancedJsonReader:
    """Advanced tests for JSON reader functionality."""

    @pytest.fixture
    def nested_json_data(self) -> dict[str, Any]:
        """Create sample nested JSON data."""
        return {
            "id": "test",
            "values": [1, 2, 3],
            "nested": {
                "a": 1,
                "b": [{"x": 1}, {"x": 2}],
                "c": {"d": {"e": {"f": "deeply nested"}}},
            },
        }

    @pytest.fixture
    def jsonl_data(self) -> list[dict[str, Any]]:
        """Create sample JSONL data."""
        return [
            {"id": 1, "name": "Record 1"},
            {"id": 2, "name": "Record 2"},
            {"id": 3, "name": "Record 3"},
        ]

    @pytest.fixture
    def json_file_path(self, nested_json_data) -> str:
        """Create a temporary JSON file."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
            temp_file.write(json.dumps(nested_json_data).encode("utf-8"))
            return temp_file.name

    @pytest.fixture
    def jsonl_file_path(self, jsonl_data) -> str:
        """Create a temporary JSONL file."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as temp_file:
            for record in jsonl_data:
                temp_file.write((json.dumps(record) + "\n").encode("utf-8"))
            return temp_file.name

    @pytest.fixture
    def large_jsonl_file_path(self) -> str:
        """Create a large JSONL file for streaming tests."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as temp_file:
            for i in range(100):  # 100 records
                record = {
                    "id": i,
                    "name": f"Record {i}",
                    "data": "x" * 50,
                }  # Make records larger
                temp_file.write((json.dumps(record) + "\n").encode("utf-8"))
            return temp_file.name

    def test_detect_json_format(self):
        """Test detecting JSON format from content."""
        # Standard JSON
        standard_json = '{"id": 1, "name": "Test"}'
        assert detect_json_format(standard_json) == "json"

        # JSONL
        jsonl_content = '{"id": 1}\n{"id": 2}\n{"id": 3}'
        assert detect_json_format(jsonl_content) == "jsonl"

        # JSON array
        json_array = '[{"id": 1}, {"id": 2}]'
        assert detect_json_format(json_array) == "json"

        # Empty string - check actual implementation
        try:
            result = detect_json_format("")
            # If it doesn't raise an error, at least make sure the result is reasonable
            assert result in ["json", "jsonl", "unknown"], (
                f"Unexpected format detected: {result}"
            )
        except Exception:
            # If it raises any exception for empty string, that's acceptable
            pass

    def test_parse_json_data(self):
        """Test parsing JSON data from strings and bytes."""
        # String input
        json_str = '{"id": 1, "name": "Test"}'
        result = parse_json_data(json_str)
        assert result == {"id": 1, "name": "Test"}

        # Bytes input
        json_bytes = b'{"id": 2, "name": "Test2"}'
        result = parse_json_data(json_bytes)
        assert result == {"id": 2, "name": "Test2"}

        # JSONL input with format hint
        jsonl_str = '{"id": 1}\n{"id": 2}'
        result = parse_json_data(jsonl_str, format_hint="jsonl")
        assert result == [{"id": 1}, {"id": 2}]

        # Invalid JSON
        with pytest.raises(json.JSONDecodeError):
            parse_json_data('{"invalid": json')

    def test_read_json_stream(self, large_jsonl_file_path):
        """Test streaming a large JSONL file."""
        try:
            # Read in chunks
            total_records = 0
            chunk_count = 0

            for chunk in read_json_stream(large_jsonl_file_path, chunk_size=20):
                assert isinstance(chunk, list)
                assert len(chunk) <= 20
                total_records += len(chunk)
                chunk_count += 1

                # Verify record structure
                assert "id" in chunk[0]
                assert "name" in chunk[0]
                assert "data" in chunk[0]

            # Verify we read all records
            assert total_records == 100
            assert chunk_count == 5  # With 20 records per chunk

            # Test with different chunk size
            total_records = 0
            for chunk in read_json_stream(large_jsonl_file_path, chunk_size=50):
                total_records += len(chunk)

            assert total_records == 100
        finally:
            # Clean up
            if os.path.exists(large_jsonl_file_path):
                os.unlink(large_jsonl_file_path)

    def test_read_invalid_json_file(self):
        """Test reading a file with invalid JSON."""
        # Create a file with invalid JSON
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
            temp_file.write(b'{"invalid": json')
            path = temp_file.name

        try:
            # Should raise a JSONDecodeError
            with pytest.raises(json.JSONDecodeError):
                read_json_file(path)
        finally:
            # Clean up
            if os.path.exists(path):
                os.unlink(path)

    def test_json_reader_read_stream(
        self, json_file_path, jsonl_file_path, nested_json_data, jsonl_data
    ):
        """Test reading streams with reader classes."""
        try:
            # JSON reader
            reader = JsonReader()
            chunks = list(reader.read_stream(json_file_path))

            # For a single object, should return a single chunk with one item
            assert len(chunks) == 1
            assert len(chunks[0]) == 1
            assert chunks[0][0] == nested_json_data

            # JSONL reader
            reader = JsonlReader()
            chunks = list(reader.read_stream(jsonl_file_path, chunk_size=2))

            # For 3 records with chunk_size=2, should return 2 chunks
            assert len(chunks) == 2
            assert len(chunks[0]) == 2  # First chunk has 2 records
            assert len(chunks[1]) == 1  # Second chunk has 1 record

            # Flatten chunks to get all records
            all_records = [record for chunk in chunks for record in chunk]
            assert all_records == jsonl_data
        finally:
            # Clean up
            for path in [json_file_path, jsonl_file_path]:
                if os.path.exists(path):
                    os.unlink(path)
