"""
Tests for JSON reading functionality.

Tests JSON file reading, parsing, and data iteration.
"""

import json
import tempfile
from pathlib import Path

import pytest

from transmog.io.readers.json import (
    JsonlReader,
    JsonReader,
    read_json_file,
    read_jsonl_file,
)


class TestJsonReader:
    """Test the JsonReader class."""

    def test_read_simple_json_file(self, json_file):
        """Test reading a simple JSON file."""
        reader = JsonReader()
        result = reader.read_file(json_file)

        assert result is not None
        assert isinstance(result, (dict, list))

    def test_read_json_object(self, json_file):
        """Test reading JSON containing a single object."""
        reader = JsonReader()
        data = reader.read_file(json_file)

        assert isinstance(data, dict)
        assert "name" in data
        assert data["name"] == "Test Entity"

    def test_read_json_array(self):
        """Test reading JSON containing an array."""
        # Create a JSON array file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}], f)
            array_file = f.name

        try:
            reader = JsonReader()
            data = reader.read_file(array_file)

            assert isinstance(data, list)
            assert len(data) == 2
            assert data[0]["name"] == "Item 1"
        finally:
            Path(array_file).unlink()

    def test_read_nested_json(self, complex_nested_data):
        """Test reading complex nested JSON."""
        # Create temp file with complex data
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(complex_nested_data, f)
            nested_file = f.name

        try:
            reader = JsonReader()
            data = reader.read_file(nested_file)

            assert isinstance(data, dict)
            assert "organization" in data
            assert "departments" in data["organization"]
        finally:
            Path(nested_file).unlink()

    def test_read_empty_json_file(self):
        """Test reading empty JSON file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{}")
            empty_file = f.name

        try:
            reader = JsonReader()
            data = reader.read_file(empty_file)

            assert data == {}
        finally:
            Path(empty_file).unlink()

    def test_read_nonexistent_file(self):
        """Test reading nonexistent file."""
        reader = JsonReader()

        with pytest.raises(FileNotFoundError):
            reader.read_file("nonexistent_file.json")

    def test_read_invalid_json(self):
        """Test reading invalid JSON."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"invalid": json}')  # Missing quotes around json
            invalid_file = f.name

        try:
            reader = JsonReader()

            with pytest.raises(json.JSONDecodeError):
                reader.read_file(invalid_file)
        finally:
            Path(invalid_file).unlink()

    def test_parse_data_string(self):
        """Test parsing JSON data from string."""
        json_string = '{"name": "Test", "value": 42}'
        reader = JsonReader()

        data = reader.parse_data(json_string)

        assert isinstance(data, dict)
        assert data["name"] == "Test"
        assert data["value"] == 42

    def test_parse_data_bytes(self):
        """Test parsing JSON data from bytes."""
        json_bytes = b'{"name": "Test", "value": 42}'
        reader = JsonReader()

        data = reader.parse_data(json_bytes)

        assert isinstance(data, dict)
        assert data["name"] == "Test"
        assert data["value"] == 42

    def test_read_stream_chunks(self, large_json_file):
        """Test reading file in chunks."""
        reader = JsonReader()

        # Read in chunks of 100
        chunks = list(reader.read_stream(large_json_file, chunk_size=100))

        assert len(chunks) > 0
        # Each chunk should be a list
        for chunk in chunks:
            assert isinstance(chunk, list)
            assert len(chunk) <= 100


class TestJsonlReader:
    """Test the JsonlReader class."""

    def test_read_jsonl_file(self, jsonl_file):
        """Test reading JSONL (JSON Lines) file."""
        reader = JsonlReader()
        data = reader.read_file(jsonl_file)

        # JSONL should be read as a list of objects
        assert isinstance(data, list)
        assert len(data) == 10  # From batch_data fixture

    def test_parse_jsonl_data(self):
        """Test parsing JSONL data from string."""
        jsonl_string = '{"id": 1, "name": "Item 1"}\n{"id": 2, "name": "Item 2"}'
        reader = JsonlReader()

        data = reader.parse_data(jsonl_string)

        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["name"] == "Item 1"

    def test_read_jsonl_stream(self, jsonl_file):
        """Test reading JSONL file in chunks."""
        reader = JsonlReader()

        # Read in chunks of 3
        chunks = list(reader.read_stream(jsonl_file, chunk_size=3))

        assert len(chunks) > 0
        # Each chunk should be a list
        for chunk in chunks:
            assert isinstance(chunk, list)
            assert len(chunk) <= 3


class TestStandaloneFunctions:
    """Test standalone JSON reading functions."""

    def test_read_json_file_function(self, json_file):
        """Test the read_json_file function."""
        data = read_json_file(json_file)

        assert isinstance(data, dict)
        assert "name" in data
        assert data["name"] == "Test Entity"

    def test_read_jsonl_file_function(self, jsonl_file):
        """Test the read_jsonl_file function."""
        data = read_jsonl_file(jsonl_file)

        assert isinstance(data, list)
        assert len(data) == 10

    def test_read_json_file_nonexistent(self):
        """Test reading nonexistent file with standalone function."""
        with pytest.raises(FileNotFoundError):
            read_json_file("nonexistent.json")

    def test_read_jsonl_file_nonexistent(self):
        """Test reading nonexistent JSONL file with standalone function."""
        with pytest.raises(FileNotFoundError):
            read_jsonl_file("nonexistent.jsonl")


class TestJsonReaderIntegration:
    """Test JsonReader integration with other components."""

    def test_reader_with_transmog_flatten(self, json_file):
        """Test using JsonReader with transmog flatten."""
        reader = JsonReader()
        data = reader.read_file(json_file)

        # Should be able to flatten the read data
        import transmog as tm

        result = tm.flatten(data, name="from_reader")

        assert len(result.main) == 1
        assert result.main[0]["name"] == "Test Entity"

    def test_reader_error_handling_integration(self):
        """Test JsonReader error handling with transmog."""
        # Create file with valid JSON
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"id": 1, "name": "Valid"}, f)
            valid_file = f.name

        try:
            reader = JsonReader()
            data = reader.read_file(valid_file)

            # Should read successfully
            assert isinstance(data, dict)
            assert data["name"] == "Valid"
        finally:
            Path(valid_file).unlink()

    def test_reader_with_different_formats(self):
        """Test JsonReader with different JSON formats."""
        formats = [
            '{"single": "object"}',
            '[{"array": "of"}, {"objects": "here"}]',
        ]

        reader = JsonReader()

        for format_content in formats:
            data = reader.parse_data(format_content)
            assert data is not None
            assert isinstance(data, (dict, list))


class TestJsonReaderEdgeCases:
    """Test edge cases in JSON reading."""

    def test_read_very_large_json(self):
        """Test reading very large JSON files."""
        # Create a large JSON structure
        large_data = {
            "items": [
                {"id": i, "data": f"item_{i}" * 10}  # Smaller strings for test
                for i in range(1000)
            ]
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(large_data, f)
            large_file = f.name

        try:
            reader = JsonReader()
            data = reader.read_file(large_file)

            assert isinstance(data, dict)
            assert "items" in data
            assert len(data["items"]) == 1000
        finally:
            Path(large_file).unlink()

    def test_read_deeply_nested_json(self):
        """Test reading deeply nested JSON."""
        # Create deeply nested structure
        nested_data = {"level0": {}}
        current = nested_data["level0"]

        for i in range(50):  # 50 levels deep (reduced for test)
            current[f"level{i + 1}"] = {}
            current = current[f"level{i + 1}"]

        current["value"] = "deep"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(nested_data, f)
            deep_file = f.name

        try:
            reader = JsonReader()
            data = reader.read_file(deep_file)

            assert isinstance(data, dict)
            assert "level0" in data
        finally:
            Path(deep_file).unlink()

    def test_read_json_with_special_characters(self):
        """Test reading JSON with special characters."""
        special_data = {
            "unicode": "Hello 世界 🌍",
            "escaped": "Line 1\nLine 2\tTabbed",
            "quotes": 'He said "Hello"',
            "backslashes": "Path\\to\\file",
            "special_keys": {
                "key-with-dash": "value1",
                "key.with.dots": "value2",
                "key with spaces": "value3",
            },
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            json.dump(special_data, f, ensure_ascii=False)
            special_file = f.name

        try:
            reader = JsonReader()
            data = reader.read_file(special_file)

            assert data["unicode"] == "Hello 世界 🌍"
            assert data["quotes"] == 'He said "Hello"'
            assert "key-with-dash" in data["special_keys"]
        finally:
            Path(special_file).unlink()

    def test_read_json_with_numeric_precision(self):
        """Test reading JSON with high-precision numbers."""
        numeric_data = {
            "large_int": 9223372036854775807,  # Max int64
            "high_precision": 3.141592653589793,
            "scientific": 1.23e-10,
            "zero": 0,
            "negative": -123.456,
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(numeric_data, f)
            numeric_file = f.name

        try:
            reader = JsonReader()
            data = reader.read_file(numeric_file)

            assert data["large_int"] == 9223372036854775807
            assert data["zero"] == 0
            assert data["negative"] == -123.456
        finally:
            Path(numeric_file).unlink()

    def test_empty_jsonl_file(self):
        """Test reading empty JSONL file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            # Write empty file
            pass
            empty_file = f.name

        try:
            reader = JsonlReader()
            data = reader.read_file(empty_file)

            assert isinstance(data, list)
            assert len(data) == 0
        finally:
            Path(empty_file).unlink()

    def test_jsonl_with_blank_lines(self):
        """Test reading JSONL file with blank lines."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"id": 1}\n')
            f.write("\n")  # Blank line
            f.write('{"id": 2}\n')
            f.write("  \n")  # Line with spaces
            f.write('{"id": 3}\n')
            blank_lines_file = f.name

        try:
            reader = JsonlReader()
            data = reader.read_file(blank_lines_file)

            assert isinstance(data, list)
            assert len(data) == 3  # Should skip blank lines
            assert data[0]["id"] == 1
            assert data[2]["id"] == 3
        finally:
            Path(blank_lines_file).unlink()
