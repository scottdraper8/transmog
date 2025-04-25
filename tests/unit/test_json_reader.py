"""
Tests for the JSON reader functionality.

These tests verify that the JSON reader works correctly with various
input formats and configurations.
"""

import os
import json
import tempfile
import pytest
from transmog.io.json_reader import (
    read_json_file,
    read_jsonl_file,
    detect_json_format,
    parse_json_data,
    read_json_stream,
)


class TestJsonReader:
    """Tests for the JSON reader functionality."""

    def test_read_json_file(self):
        """Test reading a standard JSON file."""
        # Create a temporary JSON file
        test_data = {"id": 123, "name": "Test Entity"}

        with tempfile.NamedTemporaryFile(
            suffix=".json", mode="w+", delete=False
        ) as temp_file:
            json.dump(test_data, temp_file)
            temp_path = temp_file.name

        try:
            # Read the JSON file
            result = read_json_file(temp_path)

            # Verify correct parsing
            assert isinstance(result, dict)
            assert result["id"] == 123
            assert result["name"] == "Test Entity"
        finally:
            # Clean up
            os.unlink(temp_path)

    def test_read_jsonl_file(self):
        """Test reading a JSON Lines file."""
        # Create a temporary JSONL file
        test_records = [
            {"id": 1, "name": "Record 1"},
            {"id": 2, "name": "Record 2"},
            {"id": 3, "name": "Record 3"},
        ]

        with tempfile.NamedTemporaryFile(
            suffix=".jsonl", mode="w+", delete=False
        ) as temp_file:
            for record in test_records:
                temp_file.write(json.dumps(record) + "\n")
            temp_path = temp_file.name

        try:
            # Read the JSONL file
            result = read_jsonl_file(temp_path)

            # Verify correct parsing
            assert isinstance(result, list)
            assert len(result) == 3
            assert result[0]["id"] == 1
            assert result[1]["name"] == "Record 2"
            assert result[2]["id"] == 3
        finally:
            # Clean up
            os.unlink(temp_path)

    def test_detect_json_format(self):
        """Test detecting JSON format from content."""
        # Test single JSON object
        single_json = '{"id": 123, "name": "Test"}'
        assert detect_json_format(single_json) == "json"

        # Test JSON array
        json_array = '[{"id": 1}, {"id": 2}]'
        assert detect_json_format(json_array) == "json"

        # Test JSONL (multiple objects, one per line)
        jsonl = '{"id": 1}\n{"id": 2}\n{"id": 3}'
        assert detect_json_format(jsonl) == "jsonl"

        # Test with whitespace
        jsonl_with_space = '{"id": 1}\n  {"id": 2}\n{"id": 3}'
        assert detect_json_format(jsonl_with_space) == "jsonl"

    def test_parse_json_data(self):
        """Test parsing JSON data with different formats."""
        # Test parsing a JSON object
        obj_data = '{"id": 123, "name": "Test"}'
        result1 = parse_json_data(obj_data)
        assert isinstance(result1, dict)
        assert result1["id"] == 123

        # Test parsing a JSON array
        array_data = '[{"id": 1}, {"id": 2}]'
        result2 = parse_json_data(array_data)
        assert isinstance(result2, list)
        assert len(result2) == 2

        # Test parsing JSONL
        jsonl_data = '{"id": 1}\n{"id": 2}'
        result3 = parse_json_data(jsonl_data)
        assert isinstance(result3, list)
        assert len(result3) == 2

        # Test with format override
        result4 = parse_json_data(obj_data, format_hint="json")
        assert isinstance(result4, dict)

        result5 = parse_json_data(jsonl_data, format_hint="jsonl")
        assert isinstance(result5, list)

    def test_read_json_stream(self):
        """Test reading a JSON stream with chunking."""
        # Create a temporary JSONL file with many records
        test_records = [{"id": i, "name": f"Record {i}"} for i in range(50)]

        with tempfile.NamedTemporaryFile(
            suffix=".jsonl", mode="w+", delete=False
        ) as temp_file:
            for record in test_records:
                temp_file.write(json.dumps(record) + "\n")
            temp_path = temp_file.name

        try:
            # Read the file in chunks
            chunks = []
            for chunk in read_json_stream(temp_path, chunk_size=10):
                chunks.append(chunk)

            # Verify chunks
            assert len(chunks) == 5  # 50 records in chunks of 10
            assert len(chunks[0]) == 10
            assert chunks[0][0]["id"] == 0
            assert chunks[-1][-1]["id"] == 49

            # Test with a different chunk size
            chunks2 = []
            for chunk in read_json_stream(temp_path, chunk_size=20):
                chunks2.append(chunk)

            assert (
                len(chunks2) == 3
            )  # 50 records in chunks of 20 (with final chunk smaller)
            assert len(chunks2[0]) == 20
            assert len(chunks2[-1]) == 10
        finally:
            # Clean up
            os.unlink(temp_path)

    def test_error_handling(self):
        """Test error handling for invalid JSON."""
        # Test invalid JSON
        invalid_json = '{"id": 123, "name": "Missing quote}'

        with pytest.raises(Exception):
            parse_json_data(invalid_json)

        # Test invalid JSONL (good first line, bad second)
        invalid_jsonl = '{"id": 1}\n{"id": 2, "name": "Missing quote}'

        with pytest.raises(Exception):
            parse_json_data(invalid_jsonl)

        # Test non-existent file
        with pytest.raises(FileNotFoundError):
            read_json_file("non_existent_file.json")

    def test_large_file_handling(self):
        """Test handling large files efficiently."""
        # Create a large temporary JSONL file
        record_count = 100
        test_records = [{"id": i, "data": "x" * 1000} for i in range(record_count)]

        with tempfile.NamedTemporaryFile(
            suffix=".jsonl", mode="w+", delete=False
        ) as temp_file:
            for record in test_records:
                temp_file.write(json.dumps(record) + "\n")
            temp_path = temp_file.name

        try:
            # Read with small buffer size to test chunking
            total_records = 0
            for chunk in read_json_stream(temp_path, chunk_size=10, buffer_size=4096):
                total_records += len(chunk)

                # Check records in each chunk
                for record in chunk:
                    assert isinstance(record, dict)
                    assert "id" in record
                    assert "data" in record
                    assert len(record["data"]) == 1000

            # Verify we read all records
            assert total_records == record_count
        finally:
            # Clean up
            os.unlink(temp_path)
