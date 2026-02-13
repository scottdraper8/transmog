"""
Tests for data iteration utilities.

Tests data iteration functionality, batch processing, and streaming operations.
"""

import json
import tempfile
from pathlib import Path

import pytest

from transmog.exceptions import ValidationError
from transmog.iterators import (
    get_data_iterator,
    get_hjson_file_iterator,
    get_json5_file_iterator,
    get_json_data_iterator,
    get_jsonl_data_iterator,
    get_jsonl_file_iterator,
)

try:
    import json5 as _json5  # noqa: F401

    JSON5_AVAILABLE = True
except ImportError:
    JSON5_AVAILABLE = False

try:
    import hjson as _hjson  # noqa: F401

    HJSON_AVAILABLE = True
except ImportError:
    HJSON_AVAILABLE = False

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def temp_file():
    """Create a temporary file with given content and suffix."""
    created_files = []

    def _create(content, suffix=".json"):
        with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as tmp:
            tmp.write(content)
            path = tmp.name
        created_files.append(path)
        return path

    yield _create

    # Cleanup
    for path in created_files:
        Path(path).unlink(missing_ok=True)


@pytest.fixture
def alice_data():
    """Single record test data."""
    return {"id": 1, "name": "Alice"}


@pytest.fixture
def alice_bob_data():
    """Two record test data."""
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]


@pytest.fixture
def alice_bob_charlie_data():
    """Three record test data."""
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]


# ============================================================================
# Tests
# ============================================================================


class TestGetDataIterator:
    """Test the main data iterator function."""

    def test_iterate_list_of_dicts(self, alice_bob_charlie_data):
        """Test iterating over list of dictionaries."""
        records = list(get_data_iterator(alice_bob_charlie_data))
        assert len(records) == 3
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"
        assert records[2]["name"] == "Charlie"

    def test_iterate_single_dict(self, alice_data):
        """Test iterating over single dictionary."""
        records = list(get_data_iterator(alice_data))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_iterate_empty_list(self):
        """Test iterating over empty list."""
        records = list(get_data_iterator([]))
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

    def test_iterate_with_auto_format_detection(self, alice_data):
        """Test auto format detection."""
        # JSON data
        records = list(get_data_iterator(alice_data))
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

    def test_iterate_dict(self, alice_data):
        """Test iterating over dictionary."""
        records = list(get_json_data_iterator(alice_data))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_iterate_list(self, alice_bob_data):
        """Test iterating over list."""
        records = list(get_json_data_iterator(alice_bob_data))
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
        with pytest.raises(ValidationError):
            list(get_json_data_iterator('{"invalid": json}'))

    def test_iterate_non_dict_list(self):
        """Test iterating over non-dict/list data."""
        with pytest.raises(ValidationError):
            list(get_json_data_iterator("just a string"))


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
        records = list(get_jsonl_data_iterator(""))
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

    def test_iterate_jsonl_file(self, temp_file):
        """Test iterating over JSONL file."""
        jsonl_data = [
            '{"id": 1, "name": "Alice"}',
            '{"id": 2, "name": "Bob"}',
            '{"id": 3, "name": "Charlie"}',
        ]
        content = "\n".join(jsonl_data) + "\n"
        path = temp_file(content, ".jsonl")

        records = list(get_jsonl_file_iterator(path))
        assert len(records) == 3
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"
        assert records[2]["name"] == "Charlie"

    def test_iterate_empty_jsonl_file(self, temp_file):
        """Test iterating over empty JSONL file."""
        path = temp_file("", ".jsonl")
        records = list(get_jsonl_file_iterator(path))
        assert len(records) == 0

    def test_iterate_nonexistent_file(self):
        """Test iterating over nonexistent file."""
        with pytest.raises(ValidationError):
            list(get_jsonl_file_iterator("/path/that/does/not/exist.jsonl"))

    def test_iterate_large_jsonl_file(self, temp_file):
        """Test iterating over large JSONL file."""
        lines = [json.dumps({"id": i, "value": f"item_{i}"}) for i in range(1000)]
        content = "\n".join(lines) + "\n"
        path = temp_file(content, ".jsonl")

        records = list(get_jsonl_file_iterator(path))
        assert len(records) == 1000
        assert records[0]["value"] == "item_0"
        assert records[999]["value"] == "item_999"


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
        with pytest.raises(ValidationError):
            list(get_jsonl_data_iterator(data))

    def test_empty_data_handling(self):
        """Test handling of various empty data scenarios."""
        assert len(list(get_data_iterator([]))) == 0
        assert len(list(get_data_iterator({}))) == 1

        with pytest.raises(ValidationError):
            list(get_data_iterator(""))

        with pytest.raises(ValidationError):
            list(get_data_iterator(None))

    def test_format_specification(self):
        """Test explicit format specification."""
        # Auto-detected as JSON (single line, no newlines)
        data = '{"id": 1, "name": "Alice"}'
        records = list(get_data_iterator(data))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

        # JSONL format (multiple lines)
        jsonl_data = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'
        records = list(get_data_iterator(jsonl_data))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"


@pytest.mark.skipif(not JSON5_AVAILABLE, reason="json5 not available")
class TestJSON5FileIterator:
    """Test the JSON5 file iterator function."""

    def test_iterate_json5_file(self, temp_file):
        """Test iterating over JSON5 file."""
        json5_data = """{
            // This is a comment
            id: 1,
            name: 'Alice',  // Unquoted keys and single quotes
            tags: ['premium', 'verified'],
        }"""
        path = temp_file(json5_data, ".json5")

        records = list(get_json5_file_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"
        assert records[0]["id"] == 1
        assert records[0]["tags"] == ["premium", "verified"]

    def test_iterate_json5_list_file(self, temp_file):
        """Test iterating over JSON5 file with array."""
        json5_data = """[
            {id: 1, name: 'Alice'},
            {id: 2, name: 'Bob'},
            {id: 3, name: 'Charlie'},
        ]"""
        path = temp_file(json5_data, ".json5")

        records = list(get_json5_file_iterator(path))
        assert len(records) == 3
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"
        assert records[2]["name"] == "Charlie"

    def test_iterate_json5_with_comments(self, temp_file):
        """Test iterating over JSON5 file with comments."""
        json5_data = """{
            // User information
            id: 1,
            /* Multi-line
               comment */
            name: 'Alice',
            // Trailing comma is allowed
            role: 'admin',
        }"""
        path = temp_file(json5_data, ".json5")

        records = list(get_json5_file_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"
        assert records[0]["role"] == "admin"

    def test_iterate_nonexistent_json5_file(self):
        """Test iterating over nonexistent JSON5 file."""
        with pytest.raises(ValidationError):
            list(get_json5_file_iterator("/path/that/does/not/exist.json5"))


@pytest.mark.skipif(not HJSON_AVAILABLE, reason="hjson not available")
class TestHJSONFileIterator:
    """Test the HJSON file iterator function."""

    def test_iterate_hjson_file(self, temp_file):
        """Test iterating over HJSON file."""
        hjson_data = """{
            # This is a comment
            id: 1
            name: Alice
            # No quotes needed for simple strings
            tags: ["premium", "verified"]
        }"""
        path = temp_file(hjson_data, ".hjson")

        records = list(get_hjson_file_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"
        assert records[0]["id"] == 1
        assert records[0]["tags"] == ["premium", "verified"]

    def test_iterate_hjson_list_file(self, temp_file):
        """Test iterating over HJSON file with array."""
        hjson_data = """[
            {id: 1, name: "Alice"},
            {id: 2, name: "Bob"},
            {id: 3, name: "Charlie"}
        ]"""
        path = temp_file(hjson_data, ".hjson")

        records = list(get_hjson_file_iterator(path))
        assert len(records) == 3
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"
        assert records[2]["name"] == "Charlie"

    def test_iterate_hjson_with_multiline_strings(self, temp_file):
        """Test iterating over HJSON file with multiline strings."""
        hjson_data = """{
            id: 1
            name: Alice
            description: '''
                This is a multiline
                description string
            '''
        }"""
        path = temp_file(hjson_data, ".hjson")

        records = list(get_hjson_file_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"
        assert "multiline" in records[0]["description"]

    def test_iterate_nonexistent_hjson_file(self):
        """Test iterating over nonexistent HJSON file."""
        with pytest.raises(ValidationError):
            list(get_hjson_file_iterator("/path/that/does/not/exist.hjson"))


class TestFileExtensionRouting:
    """Test that file extensions route to correct parsers through get_data_iterator."""

    def test_json_extension_routing(self, temp_file):
        """Test .json file routes through standard JSON parser."""
        path = temp_file('{"id": 1, "name": "Alice"}', ".json")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    def test_jsonl_extension_routing(self, temp_file):
        """Test .jsonl file routes to JSONL parser."""
        content = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}'
        path = temp_file(content, ".jsonl")
        records = list(get_data_iterator(path))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"

    def test_ndjson_extension_routing(self, temp_file):
        """Test .ndjson file routes to same parser as .jsonl."""
        content = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}'
        path = temp_file(content, ".ndjson")
        records = list(get_data_iterator(path))
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["name"] == "Bob"

    @pytest.mark.skipif(not JSON5_AVAILABLE, reason="json5 not available")
    def test_json5_extension_routing(self, temp_file):
        """Test .json5 file routes to JSON5 parser via get_data_iterator."""
        json5_data = """{
            // JSON5 comment
            id: 1,
            name: 'Alice',
        }"""
        path = temp_file(json5_data, ".json5")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"
        assert records[0]["id"] == 1

    @pytest.mark.skipif(not HJSON_AVAILABLE, reason="hjson not available")
    def test_hjson_extension_routing(self, temp_file):
        """Test .hjson file routes to HJSON parser via get_data_iterator."""
        hjson_data = """{
            # HJSON comment
            id: 1
            name: Alice
        }"""
        path = temp_file(hjson_data, ".hjson")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"
        assert records[0]["id"] == 1


class TestFormatSpecificFeatures:
    """Test format-specific features that distinguish each format."""

    @pytest.mark.skipif(not JSON5_AVAILABLE, reason="json5 not available")
    def test_json5_allows_trailing_commas(self, temp_file):
        """Test JSON5 allows trailing commas (standard JSON does not)."""
        json5_data = '{"items": [1, 2, 3,], "name": "test",}'
        path = temp_file(json5_data, ".json5")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["items"] == [1, 2, 3]

    @pytest.mark.skipif(not JSON5_AVAILABLE, reason="json5 not available")
    def test_json5_allows_unquoted_keys(self, temp_file):
        """Test JSON5 allows unquoted object keys."""
        json5_data = '{firstName: "Alice", lastName: "Smith", age: 30}'
        path = temp_file(json5_data, ".json5")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["firstName"] == "Alice"
        assert records[0]["lastName"] == "Smith"

    @pytest.mark.skipif(not JSON5_AVAILABLE, reason="json5 not available")
    def test_json5_allows_single_quotes(self, temp_file):
        """Test JSON5 allows single-quoted strings."""
        json5_data = "{'name': 'Alice', 'city': 'NYC'}"
        path = temp_file(json5_data, ".json5")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    @pytest.mark.skipif(not JSON5_AVAILABLE, reason="json5 not available")
    def test_json5_allows_js_comments(self, temp_file):
        """Test JSON5 allows JavaScript-style comments."""
        json5_data = """{
            // Single-line comment
            name: "Alice",
            /* Multi-line
               comment */
            age: 30
        }"""
        path = temp_file(json5_data, ".json5")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    @pytest.mark.skipif(not HJSON_AVAILABLE, reason="hjson not available")
    def test_hjson_allows_hash_comments(self, temp_file):
        """Test HJSON allows hash comments (JSON5 does not)."""
        hjson_data = """{
            # This is a hash comment
            name: Alice
            # Another comment
            age: 30
        }"""
        path = temp_file(hjson_data, ".hjson")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"
        assert records[0]["age"] == 30

    @pytest.mark.skipif(not HJSON_AVAILABLE, reason="hjson not available")
    def test_hjson_allows_unquoted_strings(self, temp_file):
        """Test HJSON allows completely unquoted string values."""
        hjson_data = """{
            name: Alice Smith
            city: New York
            country: USA
        }"""
        path = temp_file(hjson_data, ".hjson")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice Smith"
        assert records[0]["city"] == "New York"

    @pytest.mark.skipif(not HJSON_AVAILABLE, reason="hjson not available")
    def test_hjson_multiline_strings(self, temp_file):
        """Test HJSON multiline string syntax (not available in JSON5)."""
        hjson_data = """{
            name: Alice
            bio: '''
                Alice is a software engineer
                who loves to code.
                She works on data pipelines.
            '''
        }"""
        path = temp_file(hjson_data, ".hjson")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert "Alice is a software engineer" in records[0]["bio"]
        assert "data pipelines" in records[0]["bio"]

    def test_standard_json_rejects_trailing_comma(self, temp_file):
        """Test standard JSON rejects trailing commas."""
        json_data = '{"name": "Alice", "age": 30,}'
        path = temp_file(json_data, ".json")
        with pytest.raises(ValidationError):
            list(get_data_iterator(path))

    def test_standard_json_rejects_comments(self, temp_file):
        """Test standard JSON rejects comments."""
        json_data = '{"name": "Alice"  // This comment will fail\n}'
        path = temp_file(json_data, ".json")
        with pytest.raises(ValidationError):
            list(get_data_iterator(path))


class TestCrossFormatCompatibility:
    """Test that formats handle content from other formats appropriately."""

    @pytest.mark.skipif(not JSON5_AVAILABLE, reason="json5 not available")
    def test_json5_file_with_standard_json_content(self, temp_file):
        """Test .json5 file can handle standard JSON content."""
        json_data = '{"name": "Alice", "age": 30}'
        path = temp_file(json_data, ".json5")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    @pytest.mark.skipif(not HJSON_AVAILABLE, reason="hjson not available")
    def test_hjson_file_with_standard_json_content(self, temp_file):
        """Test .hjson file can handle standard JSON content."""
        json_data = '{"name": "Alice", "age": 30}'
        path = temp_file(json_data, ".hjson")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    @pytest.mark.skipif(not HJSON_AVAILABLE, reason="hjson not available")
    def test_hjson_file_with_json5_content(self, temp_file):
        """Test .hjson file can handle JSON5 content (hjson is superset)."""
        json5_data = """{
            // JSON5 comment
            name: 'Alice',
            age: 30,
        }"""
        path = temp_file(json5_data, ".hjson")
        records = list(get_data_iterator(path))
        assert len(records) == 1
        assert records[0]["name"] == "Alice"

    @pytest.mark.skipif(not JSON5_AVAILABLE, reason="json5 not available")
    def test_json5_file_rejects_hjson_hash_comments(self, temp_file):
        """Test .json5 file rejects HJSON-specific hash comments."""
        hjson_data = """{
            # This is an HJSON comment
            name: Alice
        }"""
        path = temp_file(hjson_data, ".json5")
        with pytest.raises(ValidationError):
            list(get_data_iterator(path))
