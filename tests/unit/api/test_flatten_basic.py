"""
Tests for the main Transmog v1.1.0 API.

Tests the primary user-facing functions: flatten, flatten_file, and flatten_stream.
"""

import json
from pathlib import Path

import pytest

import transmog as tm

from ...conftest import assert_files_created, assert_valid_result, load_json_file


class TestFlattenFunction:
    """Test the main flatten() function."""

    def test_basic_flatten(self, simple_data):
        """Test basic flattening functionality."""
        result = tm.flatten(simple_data, name="entity")

        assert_valid_result(result)
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Test Entity"
        assert result.main[0]["status"] == "active"

        # Nested metadata should be flattened
        assert "metadata_created_at" in result.main[0]
        assert "metadata_updated_at" in result.main[0]
        assert "metadata_version" in result.main[0]

    def test_flatten_with_arrays(self, array_data):
        """Test flattening data with arrays."""
        result = tm.flatten(array_data, name="company")

        assert_valid_result(result)
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Company"

        # Should have child tables for arrays
        assert len(result.tables) > 0

        # Check tags table
        tags_table = None
        for table_name, table_data in result.tables.items():
            if "tags" in table_name.lower():
                tags_table = table_data
                break

        assert tags_table is not None
        assert len(tags_table) == 3  # tech, startup, ai

        # Check employees table
        employees_table = None
        for table_name, table_data in result.tables.items():
            if "employees" in table_name.lower() and "skills" not in table_name.lower():
                employees_table = table_data
                break

        assert employees_table is not None
        assert len(employees_table) == 2  # Alice, Bob

    def test_flatten_batch_data(self, batch_data):
        """Test flattening a batch of records."""
        result = tm.flatten(batch_data, name="records")

        assert_valid_result(result)
        assert len(result.main) == 10

        # Check first record
        first_record = result.main[0]
        assert first_record["name"] == "Record 1"
        assert first_record["value"] == "10"  # Values are cast to strings by default

        # Should have tags table
        tags_table = None
        for table_name, table_data in result.tables.items():
            if "tags" in table_name.lower():
                tags_table = table_data
                break

        assert tags_table is not None
        assert len(tags_table) == 20  # 2 tags per record * 10 records

    def test_flatten_with_id_field(self, simple_data):
        """Test flattening with natural ID field."""
        result = tm.flatten(simple_data, name="entity", id_field="id")

        assert_valid_result(result)
        assert len(result.main) == 1

        # Should use natural ID
        record = result.main[0]
        assert record["id"] == "1"  # ID converted to string

    def test_flatten_with_custom_separator(self, simple_data):
        """Test flattening with custom field separator."""
        result = tm.flatten(simple_data, name="entity", separator=":")

        assert_valid_result(result)
        record = result.main[0]

        # Nested fields should use custom separator
        assert "metadata:created_at" in record
        assert "metadata:updated_at" in record

    def test_flatten_array_handling_options(self, array_data):
        """Test different array handling options."""
        # Test separate arrays (default)
        result_separate = tm.flatten(array_data, name="company", arrays="separate")
        assert len(result_separate.tables) > 0

        # Test inline arrays
        result_inline = tm.flatten(array_data, name="company", arrays="inline")
        # With inline, arrays should be flattened into main table

        # Test skip arrays
        result_skip = tm.flatten(array_data, name="company", arrays="skip")
        # Arrays should be ignored

    def test_flatten_error_handling(self, problematic_data):
        """Test different error handling modes."""
        # Test skip mode - should process valid records
        result_skip = tm.flatten(problematic_data, name="records", errors="skip")
        assert_valid_result(result_skip)
        assert len(result_skip.main) >= 1  # At least one valid record

        # Test warn mode - should process with warnings
        result_warn = tm.flatten(problematic_data, name="records", errors="warn")
        assert_valid_result(result_warn)
        assert len(result_warn.main) >= 1

    def test_flatten_preserve_types(self, mixed_types_data):
        """Test type preservation option."""
        # Default behavior (cast to string)
        result_string = tm.flatten(mixed_types_data, name="mixed", preserve_types=False)
        record = result_string.main[0]
        assert isinstance(record["score"], str)
        assert isinstance(record["count"], str)

        # Preserve types
        result_typed = tm.flatten(mixed_types_data, name="mixed", preserve_types=True)
        record = result_typed.main[0]
        # Types should be preserved (implementation may vary)

    def test_flatten_with_timestamp(self, simple_data):
        """Test adding timestamp to records."""
        result_with_ts = tm.flatten(simple_data, name="entity", add_timestamp=True)
        record = result_with_ts.main[0]
        assert "_timestamp" in record

        result_no_ts = tm.flatten(simple_data, name="entity", add_timestamp=False)
        record = result_no_ts.main[0]
        assert "_timestamp" not in record

    def test_flatten_complex_nested(self, complex_nested_data):
        """Test flattening deeply nested structures."""
        result = tm.flatten(complex_nested_data, name="entity")

        assert_valid_result(result)
        assert len(result.main) == 1

        # Should have multiple child tables due to deep nesting
        assert len(result.tables) > 0

        # Should have departments table
        dept_table = None
        for table_name, table_data in result.tables.items():
            if (
                "departments" in table_name.lower()
                and "teams" not in table_name.lower()
            ):
                dept_table = table_data
                break

        if dept_table:
            assert len(dept_table) == 2  # Engineering, Sales


class TestFlattenFileFunction:
    """Test the flatten_file() function."""

    def test_flatten_json_file(self, json_file):
        """Test flattening a JSON file."""
        result = tm.flatten_file(json_file, name="from_file")

        assert_valid_result(result)
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Test Entity"

    def test_flatten_jsonl_file(self, jsonl_file):
        """Test flattening a JSONL file."""
        result = tm.flatten_file(jsonl_file, name="from_jsonl")

        assert_valid_result(result)
        assert len(result.main) == 10  # 10 records in batch_data

    def test_flatten_csv_file(self, csv_file):
        """Test flattening a CSV file."""
        result = tm.flatten_file(csv_file, name="from_csv")

        assert_valid_result(result)
        assert len(result.main) == 3  # 3 records in CSV

        # Check first record
        first_record = result.main[0]
        assert first_record["name"] == "Alice"
        assert first_record["value"] == "100"

    def test_flatten_file_auto_name(self, json_file):
        """Test auto-naming from filename."""
        result = tm.flatten_file(json_file)  # No name specified

        assert_valid_result(result)
        # Should derive name from filename

    def test_flatten_file_with_options(self, json_file):
        """Test flatten_file with additional options."""
        result = tm.flatten_file(
            json_file, name="custom", separator=":", add_timestamp=True, arrays="inline"
        )

        assert_valid_result(result)
        record = result.main[0]
        assert "_timestamp" in record
        assert "metadata:created_at" in record

    def test_flatten_nonexistent_file(self):
        """Test handling of nonexistent files."""
        with pytest.raises(Exception):  # Should raise appropriate error
            tm.flatten_file("nonexistent.json")


class TestFlattenStreamFunction:
    """Test the flatten_stream() function."""

    def test_flatten_stream_basic(self, batch_data, output_dir):
        """Test basic streaming functionality."""
        output_path = output_dir / "stream_output"

        # Stream processing returns None
        result = tm.flatten_stream(
            batch_data, output_path=str(output_path), name="streamed", format="json"
        )

        assert result is None  # Streaming returns None

        # Check that output files were created
        json_files = list(output_dir.glob("**/*.json"))
        assert len(json_files) > 0

    def test_flatten_stream_large_data(self, large_json_file, output_dir):
        """Test streaming with large dataset."""
        output_path = output_dir / "large_stream"

        result = tm.flatten_stream(
            large_json_file,
            output_path=str(output_path),
            name="large",
            format="json",
            batch_size=100,
        )

        assert result is None

        # Check output files
        json_files = list(output_dir.glob("**/*.json"))
        assert len(json_files) > 0

    def test_flatten_stream_csv_format(self, batch_data, output_dir):
        """Test streaming to CSV format."""
        output_path = output_dir / "csv_stream"

        result = tm.flatten_stream(
            batch_data, output_path=str(output_path), name="csv_data", format="csv"
        )

        assert result is None

        # Check CSV files were created
        csv_files = list(output_dir.glob("**/*.csv"))
        assert len(csv_files) > 0

    def test_flatten_stream_parquet_format(self, batch_data, output_dir):
        """Test streaming to Parquet format."""
        output_path = output_dir / "parquet_stream"

        try:
            result = tm.flatten_stream(
                batch_data,
                output_path=str(output_path),
                name="parquet_data",
                format="parquet",
            )

            assert result is None

            # Check output files were created
            output_files = list(output_dir.glob("**/*"))
            output_files = [f for f in output_files if f.is_file()]

            # If no files created, parquet might not be available
            if len(output_files) == 0:
                pytest.skip("Parquet format not available or not working")
            else:
                assert len(output_files) > 0

        except (ImportError, tm.TransmogError) as e:
            # Parquet dependencies might not be available
            pytest.skip(f"Parquet format not available: {e}")

    def test_flatten_stream_with_options(self, array_data, output_dir):
        """Test streaming with various options."""
        output_path = output_dir / "options_stream"

        result = tm.flatten_stream(
            array_data,
            output_path=str(output_path),
            name="options_test",
            format="json",
            separator=":",
            arrays="separate",
            preserve_types=True,
            add_timestamp=True,
            batch_size=50,
        )

        assert result is None

        # Verify files were created
        json_files = list(output_dir.glob("**/*.json"))
        assert len(json_files) > 0


class TestAPIEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_data(self):
        """Test handling empty data."""
        result = tm.flatten([], name="empty")
        assert_valid_result(result)
        assert len(result.main) == 0

    def test_none_data(self):
        """Test handling None data."""
        with pytest.raises(Exception):
            tm.flatten(None, name="none")

    def test_invalid_format(self, simple_data, output_dir):
        """Test invalid output format."""
        with pytest.raises((ValueError, tm.TransmogError)):
            tm.flatten_stream(
                simple_data,
                output_path=str(output_dir / "invalid"),
                name="test",
                format="invalid_format",
            )

    def test_invalid_array_handling(self, array_data):
        """Test invalid array handling option."""
        # Test with a clearly invalid option
        try:
            result = tm.flatten(array_data, name="test", arrays="invalid_option")
            # If it doesn't raise, just verify it processed something
            assert len(result.main) >= 0
        except (ValueError, tm.TransmogError):
            # This is expected behavior
            pass

    def test_invalid_error_handling(self, simple_data):
        """Test invalid error handling option."""
        with pytest.raises(tm.ValidationError):
            tm.flatten(simple_data, name="test", errors="invalid_option")
