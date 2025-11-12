"""
Tests for data iteration utilities.

Tests data iteration functionality, batch processing, and streaming operations.
"""

import json
import tempfile
from pathlib import Path

import pytest

from transmog.config import TransmogConfig
from transmog.exceptions import ValidationError
from transmog.iterators import (
    get_data_iterator,
    get_json_data_iterator,
    get_jsonl_data_iterator,
    get_jsonl_file_iterator,
)


class TestGetDataIterator:
    """Test the main data iterator function."""

    def test_iterate_list_of_dicts(self):
        """Test iterating over list of dictionaries."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

        records = list(get_data_iterator(data))
        assert len(records) == 3
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"
        assert records[2]["name"] == "Charlie"

    def test_iterate_single_dict(self):
        """Test iterating over single dictionary."""
        data = {"id": 1, "name": "Alice"}

        records = list(get_data_iterator(data))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_iterate_empty_list(self):
        """Test iterating over empty list."""
        data = []

        records = list(get_data_iterator(data))
        assert len(records) == 0

    def test_iterate_json_string(self):
        """Test iterating over JSON string."""
        data = '{"id": 1, "name": "Alice"}'

        records = list(get_data_iterator(data))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_iterate_json_list_string(self):
        """Test iterating over JSON list string."""
        data = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'

        records = list(get_data_iterator(data))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"

    def test_iterate_with_auto_format_detection(self):
        """Test auto format detection."""
        # JSON data
        json_data = {"id": 1, "name": "Alice"}
        records = list(get_data_iterator(json_data))
        assert len(records) == 1

        # JSONL data
        jsonl_data = '{"id": 1}\n{"id": 2}\n'
        records = list(get_data_iterator(jsonl_data))
        assert len(records) == 2

    def test_iterate_unsupported_type(self):
        """Test iterating over unsupported data type."""
        with pytest.raises(ValidationError):
            list(get_data_iterator(42))


class TestJSONDataIterator:
    """Test the JSON data iterator function."""

    def test_iterate_dict(self):
        """Test iterating over dictionary."""
        data = {"id": 1, "name": "Alice"}

        records = list(get_json_data_iterator(data))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_iterate_list(self):
        """Test iterating over list."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        records = list(get_json_data_iterator(data))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"

    def test_iterate_json_string(self):
        """Test iterating over JSON string."""
        data = '{"id": 1, "name": "Alice"}'

        records = list(get_json_data_iterator(data))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_iterate_json_list_string(self):
        """Test iterating over JSON list string."""
        data = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'

        records = list(get_json_data_iterator(data))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"

    def test_iterate_invalid_json(self):
        """Test iterating over invalid JSON data."""
        data = '{"invalid": json}'

        with pytest.raises(ValidationError):
            list(get_json_data_iterator(data))

    def test_iterate_non_dict_list(self):
        """Test iterating over non-dict/list data."""
        data = "just a string"

        with pytest.raises(ValidationError):
            list(get_json_data_iterator(data))


class TestJSONLDataIterator:
    """Test the JSONL data iterator function."""

    def test_iterate_jsonl_string(self):
        """Test iterating over JSONL string."""
        data = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'

        records = list(get_jsonl_data_iterator(data))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"

    def test_iterate_jsonl_with_empty_lines(self):
        """Test iterating over JSONL with empty lines."""
        data = '{"id": 1, "name": "Alice"}\n\n{"id": 2, "name": "Bob"}\n'

        records = list(get_jsonl_data_iterator(data))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"

    def test_iterate_empty_jsonl(self):
        """Test iterating over empty JSONL."""
        data = ""

        records = list(get_jsonl_data_iterator(data))
        assert len(records) == 0

    def test_iterate_jsonl_bytes(self):
        """Test iterating over JSONL bytes."""
        data = b'{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'

        records = list(get_jsonl_data_iterator(data))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"


class TestJSONLFileIterator:
    """Test the JSONL file iterator function."""

    def test_iterate_jsonl_file(self):
        """Test iterating over JSONL file."""
        jsonl_data = [
            '{"id": 1, "name": "Alice"}',
            '{"id": 2, "name": "Bob"}',
            '{"id": 3, "name": "Charlie"}',
        ]

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as tmp:
            for line in jsonl_data:
                tmp.write(line + "\n")
            tmp_path = tmp.name

        try:
            records = list(get_jsonl_file_iterator(tmp_path))
            assert len(records) == 3
            assert records[0]["name"] == "Alice"
            assert records[1]["name"] == "Bob"
            assert records[2]["name"] == "Charlie"
        finally:
            Path(tmp_path).unlink()

    def test_iterate_empty_jsonl_file(self):
        """Test iterating over empty JSONL file."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as tmp:
            tmp_path = tmp.name

        try:
            records = list(get_jsonl_file_iterator(tmp_path))
            assert len(records) == 0
        finally:
            Path(tmp_path).unlink()

    def test_iterate_nonexistent_file(self):
        """Test iterating over nonexistent file."""
        with pytest.raises(ValidationError):
            list(get_jsonl_file_iterator("/path/that/does/not/exist.jsonl"))

    def test_iterate_large_jsonl_file(self):
        """Test iterating over large JSONL file."""
        # Create large JSONL file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as tmp:
            for i in range(1000):
                json.dump({"id": i, "value": f"item_{i}"}, tmp)
                tmp.write("\n")
            tmp_path = tmp.name

        try:
            records = list(get_jsonl_file_iterator(tmp_path))
            assert len(records) == 1000
            assert records[0]["value"] == "item_0"
            assert records[999]["value"] == "item_999"
        finally:
            Path(tmp_path).unlink()


class TestDataIteratorEdgeCases:
    """Test edge cases in data iteration."""

    def test_iterator_with_unicode_data(self):
        """Test iterator handling unicode data."""
        data = [
            {"name": "Alice", "city": "New York"},
            {"name": "测试", "city": "北京"},
            {"name": "José", "city": "São Paulo"},
            {"name": "🌍", "city": "🏙️"},
        ]

        records = list(get_data_iterator(data))
        assert len(records) == 4
        assert records[1]["name"] == "测试"
        assert records[2]["name"] == "José"
        assert records[3]["name"] == "🌍"

    def test_iterator_with_nested_data(self):
        """Test iterator handling nested data structures."""
        data = {
            "company": "TechCorp",
            "employees": [
                {"name": "Alice", "role": "Engineer"},
                {"name": "Bob", "role": "Designer"},
            ],
            "metadata": {"created": "2023-01-01", "version": "1.0"},
        }

        records = list(get_data_iterator(data))
        assert len(records) == 1
        assert records[0]["company"] == "TechCorp"
        assert "employees" in records[0]
        assert "metadata" in records[0]

    def test_existing_iterator_passthrough(self):
        """Test that existing iterators are passed through."""

        def data_generator():
            yield {"id": 1, "name": "Alice"}
            yield {"id": 2, "name": "Bob"}

        # Should pass through the generator directly
        iterator = get_data_iterator(data_generator())
        records = list(iterator)
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"

    def test_malformed_jsonl_data(self):
        """Test handling malformed JSONL data."""
        data = (
            '{"id": 1, "name": "Alice"}\n{"invalid": json}\n{"id": 2, "name": "Bob"}\n'
        )

        # With default strict error handling, should raise ValidationError for invalid JSON
        with pytest.raises(ValidationError):
            list(get_jsonl_data_iterator(data))

    def test_empty_data_handling(self):
        """Test handling of various empty data scenarios."""
        # Empty list
        assert len(list(get_data_iterator([]))) == 0

        # Empty dict is still a record
        assert len(list(get_data_iterator({}))) == 1

        # Empty string should raise error
        with pytest.raises(ValidationError):
            list(get_data_iterator(""))

        # None data
        with pytest.raises(ValidationError):
            list(get_data_iterator(None))

    def test_format_specification(self):
        """Test explicit format specification."""
        data = '{"id": 1, "name": "Alice"}'

        # Auto-detected as JSON (single line, no newlines)
        records = list(get_data_iterator(data))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

        # JSONL format (multiple lines)
        jsonl_data = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'
        records = list(get_data_iterator(jsonl_data))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"
