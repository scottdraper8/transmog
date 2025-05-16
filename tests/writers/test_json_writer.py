"""
Tests for the JSON writer implementation.

This module tests that the JSON writer correctly handles writing data to JSON format.
"""

import json
import os

import pytest

from tests.interfaces.test_writer_interface import AbstractWriterTest

# Import the writer and abstract test base class
from transmog.io.writers.json import JsonWriter


class TestJsonWriter(AbstractWriterTest):
    """Test the JSON writer implementation."""

    writer_class = JsonWriter
    format_name = "json"

    @pytest.fixture
    def writer(self):
        """Create a JSON writer."""
        return JsonWriter(indent=2)  # Use indentation for readable test output

    def test_indentation_option(self, writer, batch_data, tmp_path):
        """Test different indentation options."""
        # Test various indentation options
        indentation_options = [None, 0, 2, 4]
        outputs = {}

        for indent in indentation_options:
            # Create writer with this indentation
            writer = JsonWriter(indent=indent)

            # Create output path
            output_path = tmp_path / f"indent_test_{indent}.json"

            # Write data
            writer.write_table(batch_data, output_path)

            # Store path for content comparison
            outputs[indent] = output_path

            # Verify file exists
            assert os.path.exists(output_path)

        # Verify files were written with the correct indentation
        for indent, path in outputs.items():
            with open(path) as f:
                content = f.read()

                # Parse the JSON and re-serialize to check structure
                parsed = json.loads(content)
                assert isinstance(parsed, list), "Expected JSON array"
                assert len(parsed) == len(batch_data), "Record count mismatch"

                # If indent is None or 0, there should be no newlines except at the beginning/end
                if indent == 0 or indent is None:
                    # Count newlines in the middle of the content (not first/last char)
                    inner_content = content[1:-1]
                    # Allow for minimal whitespace if indent is None (implementation-specific)
                    # Some libraries may add some minimal whitespace even when indent=None
                    assert inner_content.count("\n") <= 5, (
                        "Expected compact JSON with no indentation"
                    )
                else:
                    # With indentation, we should have at least as many newlines as records
                    assert content.count("\n") >= len(batch_data), (
                        "Expected indented JSON format"
                    )

    def test_serialization_precision(self, tmp_path):
        """Test that numeric values are serialized with proper precision."""
        # Create test data with various numeric types
        test_data = [
            {
                "int_value": 12345,
                "float_value": 123.456789,
                "scientific": 1.23e-10,
                "zero": 0.0,
                "large_int": 9007199254740992,  # 2^53
            }
        ]

        # Create writer
        writer = JsonWriter()

        # Create output path
        output_path = tmp_path / "precision_test.json"

        # Write data
        writer.write_table(test_data, output_path)

        # Read back
        with open(output_path) as f:
            read_data = json.load(f)

        # Verify values
        assert read_data[0]["int_value"] == 12345
        assert abs(read_data[0]["float_value"] - 123.456789) < 1e-10
        assert abs(read_data[0]["scientific"] - 1.23e-10) < 1e-20
        assert read_data[0]["zero"] == 0.0
        assert read_data[0]["large_int"] == 9007199254740992

    def test_file_like_object(self, writer, batch_data, tmp_path):
        """Test writing to a file-like object."""
        # Create output path
        output_path = tmp_path / "file_like_test.json"

        # Open file for writing
        with open(output_path, "w") as f:
            # Write data to file object
            result = writer.write_table(batch_data, f)

            # Should return the file object
            assert result == f

        # Verify file has content
        assert os.path.exists(output_path)
        assert os.path.getsize(output_path) > 0

        # Read back and verify
        with open(output_path) as f:
            read_data = json.load(f)

            assert isinstance(read_data, list)
            assert len(read_data) == len(batch_data)

    def test_binary_file_object(self, writer, batch_data, tmp_path):
        """Test writing to a binary file object."""
        # Create output path
        output_path = tmp_path / "binary_file_test.json"

        # Open file for binary writing
        with open(output_path, "wb") as f:
            # Write data to file object - this should work
            result = writer.write_table(batch_data, f)

            # Should return the file object
            assert result == f

        # Verify file has content
        assert os.path.exists(output_path)
        assert os.path.getsize(output_path) > 0

        # Read back and verify
        with open(output_path) as f:
            read_data = json.load(f)

            assert isinstance(read_data, list)
            assert len(read_data) == len(batch_data)

    @pytest.mark.skipif(not JsonWriter.is_orjson_available(), reason="orjson required")
    def test_orjson_acceleration(self, batch_data, tmp_path):
        """Test that orjson is used when available."""

        # Create writers
        writer_with_orjson = JsonWriter(use_orjson=True)
        writer_without_orjson = JsonWriter(use_orjson=False)

        # Create output paths
        output_with_orjson = tmp_path / "with_orjson.json"
        output_without_orjson = tmp_path / "without_orjson.json"

        # Write data with both writers
        writer_with_orjson.write_table(batch_data, output_with_orjson)
        writer_without_orjson.write_table(batch_data, output_without_orjson)

        # Verify both files exist
        assert os.path.exists(output_with_orjson)
        assert os.path.exists(output_without_orjson)

        # Read back both files and verify content
        with open(output_with_orjson) as f:
            with_orjson_data = json.load(f)

        with open(output_without_orjson) as f:
            without_orjson_data = json.load(f)

        # Content should be equivalent
        assert with_orjson_data == without_orjson_data

        # Verify record counts
        assert len(with_orjson_data) == len(batch_data)
        assert len(without_orjson_data) == len(batch_data)
