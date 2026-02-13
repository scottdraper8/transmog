"""Tests for flatten() and flatten_stream() API functions."""

import json
from pathlib import Path

import pytest

import transmog as tm
from transmog import TransmogConfig
from transmog.exceptions import ConfigurationError, ValidationError
from transmog.types import ArrayMode

from ..conftest import assert_files_created, assert_valid_result


class TestFlattenBasic:
    """Test basic flatten() functionality."""

    def test_flatten_simple_data(self, simple_data):
        """Test flattening simple nested data."""
        result = tm.flatten(simple_data, name="entity")

        assert_valid_result(result)
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Test Entity"
        assert result.main[0]["status"] == "active"
        assert "metadata_created_at" in result.main[0]
        assert "metadata_updated_at" in result.main[0]
        assert "metadata_version" in result.main[0]

    def test_flatten_with_arrays(self, array_data):
        """Test flattening data with arrays in smart mode."""
        result = tm.flatten(array_data, name="company")

        assert_valid_result(result)
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Company"
        assert "tags" in result.main[0]
        assert result.main[0]["tags"] == ["tech", "startup", "ai"]
        assert len(result.tables) > 0

        employees_table = None
        for table_name, table_data in result.tables.items():
            if "employees" in table_name.lower() and "skills" not in table_name.lower():
                employees_table = table_data
                break

        assert employees_table is not None
        assert len(employees_table) == 2

        for emp in employees_table:
            assert "skills" in emp
            assert isinstance(emp["skills"], list)

    def test_flatten_batch_data(self, batch_data):
        """Test flattening a batch of records."""
        result = tm.flatten(batch_data, name="records")

        assert_valid_result(result)
        assert len(result.main) == 10

        first_record = result.main[0]
        assert first_record["name"] == "Record 1"
        assert first_record["value"] == 10
        assert "tags" in first_record
        assert isinstance(first_record["tags"], list)
        assert len(first_record["tags"]) == 2

    def test_flatten_complex_nested(self, complex_nested_data):
        """Test flattening deeply nested structures."""
        result = tm.flatten(complex_nested_data, name="entity")

        assert_valid_result(result)
        assert len(result.main) == 1
        assert len(result.tables) > 0

        dept_table = None
        for table_name, table_data in result.tables.items():
            if (
                "departments" in table_name.lower()
                and "teams" not in table_name.lower()
            ):
                dept_table = table_data
                break

        if dept_table:
            assert len(dept_table) == 2


class TestFlattenConfiguration:
    """Test flatten() with different configurations."""

    def test_flatten_with_id_field(self, simple_data):
        """Test flattening with natural ID field."""
        config = TransmogConfig(id_generation="natural", id_field="id")
        result = tm.flatten(simple_data, name="entity", config=config)

        assert_valid_result(result)
        assert len(result.main) == 1
        assert result.main[0]["id"] == 1

    def test_flatten_with_timestamp(self, simple_data):
        """Test adding timestamp to records."""
        result_with_ts = tm.flatten(
            simple_data, name="entity", config=TransmogConfig(time_field="_timestamp")
        )
        assert "_timestamp" in result_with_ts.main[0]

        result_no_ts = tm.flatten(
            simple_data, name="entity", config=TransmogConfig(time_field=None)
        )
        assert "_timestamp" not in result_no_ts.main[0]


class TestFlattenArrayModes:
    """Test flatten() with different array handling modes."""

    def test_flatten_separate_arrays(self, array_data):
        """Test SEPARATE array mode."""
        result = tm.flatten(
            array_data,
            name="company",
            config=TransmogConfig(array_mode=ArrayMode.SEPARATE),
        )
        assert len(result.tables) > 0

    def test_flatten_inline_arrays(self, array_data):
        """Test INLINE array mode."""
        result = tm.flatten(
            array_data,
            name="company",
            config=TransmogConfig(array_mode=ArrayMode.INLINE),
        )
        assert_valid_result(result)

    def test_flatten_skip_arrays(self, array_data):
        """Test SKIP array mode."""
        result = tm.flatten(
            array_data,
            name="company",
            config=TransmogConfig(array_mode=ArrayMode.SKIP),
        )
        assert_valid_result(result)


class TestFlattenEdgeCases:
    """Test flatten() edge cases and boundary conditions."""

    def test_flatten_empty_list(self):
        """Test flattening empty list."""
        result = tm.flatten([], name="empty")
        assert_valid_result(result)
        assert len(result.main) == 0

    def test_flatten_empty_dict(self):
        """Test flattening empty dictionary."""
        result = tm.flatten({}, name="empty")
        assert len(result.main) == 0
        assert len(result.tables) == 0

    def test_flatten_none_input(self):
        """Test flattening None input."""
        with pytest.raises(ValidationError):
            tm.flatten(None, name="test")

    def test_flatten_single_value_dict(self):
        """Test flattening dictionary with single primitive value."""
        data = {"value": 42}
        result = tm.flatten(data, name="single")

        assert len(result.main) == 1
        assert result.main[0]["value"] == 42

    def test_flatten_very_deep_nesting(self):
        """Test flattening extremely deep nesting."""
        data = {"level1": {}}
        current = data["level1"]
        for i in range(2, 51):
            current[f"level{i}"] = {}
            current = current[f"level{i}"]
        current["value"] = "deep_value"

        config = TransmogConfig(max_depth=100)
        result = tm.flatten(data, name="deep", config=config)
        assert len(result.main) == 1

    def test_flatten_circular_reference(self):
        """Test flattening data with circular references."""
        data = {"id": 1, "name": "test"}
        data["self"] = data

        config = TransmogConfig(max_depth=10)
        result = tm.flatten(data, name="circular", config=config)
        assert isinstance(result, tm.FlattenResult)
        assert len(result.main) == 1
        assert result.main[0]["name"] == "test"

    def test_flatten_very_large_array(self):
        """Test flattening data with very large array."""
        large_array = [{"id": i, "value": f"item_{i}"} for i in range(10000)]
        data = {"items": large_array}

        result = tm.flatten(data, name="large_array")
        assert len(result.main) == 1
        assert len(result.tables) > 0

    def test_flatten_mixed_type_array(self):
        """Test flattening array with mixed types."""
        data = {
            "mixed_array": [
                {"type": "dict", "value": 1},
                "string_value",
                42,
                True,
                None,
                [1, 2, 3],
            ]
        }

        result = tm.flatten(data, name="mixed")
        assert isinstance(result, tm.FlattenResult)

    def test_flatten_unicode_and_special_chars(self):
        """Test flattening data with unicode and special characters."""
        data = {
            "unicode": "Hello 世界",
            "emoji": "😀🌍",
            "special_chars": "!@#$%^&*()_+-=[]{}|;:,.<>?",
            "newlines": "line1\nline2\r\nline3",
            "tabs": "col1\tcol2\tcol3",
        }

        result = tm.flatten(data, name="unicode")
        assert len(result.main) == 1
        record = result.main[0]
        assert "unicode" in record
        assert "emoji" in record


class TestFlattenFile:
    """Test flatten() function with file paths."""

    def test_flatten_json_file(self, json_file):
        """Test flattening a JSON file."""
        result = tm.flatten(json_file, name="from_file")

        assert_valid_result(result)
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Test Entity"

    def test_flatten_jsonl_file(self, jsonl_file):
        """Test flattening a JSONL file."""
        result = tm.flatten(jsonl_file, name="from_jsonl")

        assert_valid_result(result)
        assert len(result.main) == 10

    def test_flatten_with_file_path_and_config(self, json_file):
        """Test flatten with file path and configuration options."""
        config = TransmogConfig(
            array_mode=ArrayMode.INLINE,
            time_field="_timestamp",
        )
        result = tm.flatten(json_file, name="custom", config=config)

        assert_valid_result(result)
        record = result.main[0]
        assert "_timestamp" in record
        assert "metadata_created_at" in record  # Uses underscore separator

    def test_flatten_nonexistent_file(self):
        """Test handling of nonexistent files."""
        with pytest.raises(Exception):
            tm.flatten("nonexistent.csv")


class TestFlattenStream:
    """Test flatten_stream() function."""

    def test_flatten_stream_basic(self, batch_data, output_dir):
        """Test basic streaming functionality."""
        output_path = output_dir / "stream_output"

        result = tm.flatten_stream(
            batch_data,
            output_path=str(output_path),
            name="streamed",
            output_format="csv",
        )

        assert result is None
        csv_files = list(output_dir.glob("**/*.csv"))
        assert len(csv_files) > 0

    def test_flatten_stream_large_data(self, large_json_file, output_dir):
        """Test streaming with large dataset."""
        output_path = output_dir / "large_stream"

        config = TransmogConfig(batch_size=100)
        result = tm.flatten_stream(
            large_json_file,
            output_path=str(output_path),
            name="large",
            output_format="csv",
            config=config,
        )

        assert result is None
        csv_files = list(output_dir.glob("**/*.csv"))
        assert len(csv_files) > 0

    def test_flatten_stream_csv_format(self, batch_data, output_dir):
        """Test streaming to CSV format."""
        output_path = output_dir / "csv_stream"

        result = tm.flatten_stream(
            batch_data,
            output_path=str(output_path),
            name="csv_data",
            output_format="csv",
        )

        assert result is None
        csv_files = list(output_dir.glob("**/*.csv"))
        assert len(csv_files) > 0

    def test_flatten_stream_parquet_format(self, batch_data, output_dir):
        """Test streaming to Parquet format."""
        import pyarrow.parquet as pq

        output_path = output_dir / "parquet_stream"

        result = tm.flatten_stream(
            batch_data,
            output_path=str(output_path),
            name="parquet_data",
            output_format="parquet",
        )

        assert result is None
        parquet_files = list(output_dir.glob("**/*.parquet"))
        assert len(parquet_files) > 0

        # Verify content of main file
        main_file = next(f for f in parquet_files if "parquet_data" in f.name)
        table = pq.read_table(str(main_file))
        assert table.num_rows == len(batch_data)

    def test_flatten_stream_with_options(self, array_data, output_dir):
        """Test streaming with various configuration options."""
        output_path = output_dir / "options_stream"

        config = TransmogConfig(
            array_mode=ArrayMode.SEPARATE,
            time_field="_timestamp",
            batch_size=50,
        )
        result = tm.flatten_stream(
            array_data,
            output_path=str(output_path),
            name="options_test",
            output_format="csv",
            config=config,
        )

        assert result is None
        csv_files = list(output_dir.glob("**/*.csv"))
        assert len(csv_files) > 0

    def test_flatten_stream_invalid_format(self, simple_data, output_dir):
        """Test streaming with invalid output format."""
        with pytest.raises((ValueError, tm.TransmogError)):
            tm.flatten_stream(
                simple_data,
                output_path=str(output_dir / "invalid"),
                name="test",
                output_format="invalid_format",
            )


class TestParameterValidation:
    """Test parameter validation for flatten functions."""

    def test_negative_batch_size(self):
        """Test flatten with negative batch size."""
        data = {"test": "data"}

        with pytest.raises(ConfigurationError):
            config = TransmogConfig(batch_size=-1)
            tm.flatten(data, name="test", config=config)

    def test_zero_batch_size(self):
        """Test flatten with zero batch size."""
        data = {"test": "data"}

        with pytest.raises(ConfigurationError):
            config = TransmogConfig(batch_size=0)
            tm.flatten(data, name="test", config=config)


class TestBoundaryConditions:
    """Test boundary conditions and edge values."""

    def test_zero_values(self):
        """Test handling of zero values."""
        data = {
            "zero_int": 0,
            "zero_float": 0.0,
            "false_bool": False,
            "empty_string": "",
        }

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1
        record = result.main[0]
        assert "zero_int" in record
        assert "zero_float" in record
        assert "false_bool" in record

    def test_very_large_numbers(self):
        """Test handling of very large numbers."""
        data = {
            "large_int": 9999999999999999999,
            "large_float": 1.7976931348623157e308,
            "small_float": 2.2250738585072014e-308,
        }

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1
        record = result.main[0]
        assert "large_int" in record
        assert "large_float" in record
        assert "small_float" in record

    def test_very_long_field_names(self):
        """Test handling of very long field names."""
        long_key = "a" * 1000
        data = {long_key: "value", "normal": "value"}

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1

    def test_many_fields(self):
        """Test handling of objects with many fields."""
        data = {f"field_{i}": f"value_{i}" for i in range(1000)}

        result = tm.flatten(data, name="test")

        assert len(result.main) == 1
        record = result.main[0]
        assert len(record) > 1000
