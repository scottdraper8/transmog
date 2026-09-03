"""Tests for flatten() and flatten_stream() API functions."""

from pathlib import Path

import pytest

import transmog as tm
from transmog import TransmogConfig
from transmog.exceptions import ConfigurationError, ValidationError
from transmog.types import ArrayMode

from ..conftest import output_file_for, read_csv_rows


class TestFlattenBasic:
    """Test basic flatten() functionality."""

    def test_flatten_simple_data(self, simple_data):
        """Test flattening simple nested data."""
        result = tm.flatten(simple_data, name="entity")

        assert len(result.main) == 1
        assert result.main[0]["name"] == "Test Entity"
        assert result.main[0]["status"] == "active"
        assert "metadata_created_at" in result.main[0]
        assert "metadata_updated_at" in result.main[0]
        assert "metadata_version" in result.main[0]

    def test_flatten_with_arrays(self, array_data):
        """Test flattening data with arrays in smart mode."""
        result = tm.flatten(array_data, name="company")

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
        """SEPARATE extracts arrays into child tables."""
        result = tm.flatten(
            array_data,
            name="company",
            config=TransmogConfig(array_mode=ArrayMode.SEPARATE),
        )
        assert "tags" not in result.main[0]
        assert "company_tags" in result.tables
        assert "company_employees" in result.tables
        assert len(result.tables["company_employees"]) == 2

    def test_flatten_inline_arrays(self, array_data):
        """INLINE keeps arrays on the parent row and creates no child tables."""
        result = tm.flatten(
            array_data,
            name="company",
            config=TransmogConfig(array_mode=ArrayMode.INLINE),
        )
        assert result.tables == {}
        assert "tags" in result.main[0]
        assert "employees" in result.main[0]

    def test_flatten_skip_arrays(self, array_data):
        """SKIP omits arrays from output."""
        result = tm.flatten(
            array_data,
            name="company",
            config=TransmogConfig(array_mode=ArrayMode.SKIP),
        )
        assert result.tables == {}
        assert "tags" not in result.main[0]
        assert "employees" not in result.main[0]
        assert result.main[0]["name"] == "Company"


class TestFlattenEdgeCases:
    """Test flatten() edge cases and boundary conditions."""

    def test_flatten_empty_list(self):
        """Test flattening empty list."""
        result = tm.flatten([], name="empty")
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
        leaf_keys = [k for k in result.main[0] if k.endswith("value")]
        assert leaf_keys
        assert result.main[0][leaf_keys[0]] == "deep_value"

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
        assert len(result.main) == 1
        assert len(result.tables) >= 1

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
        assert record["unicode"] == "Hello 世界"
        assert record["emoji"] == "😀🌍"
        assert record["special_chars"] == "!@#$%^&*()_+-=[]{}|;:,.<>?"


class TestFlattenFile:
    """Test flatten() function with file paths."""

    def test_flatten_json_file(self, json_file):
        """Test flattening a JSON file."""
        result = tm.flatten(json_file, name="from_file")

        assert len(result.main) == 1
        assert result.main[0]["name"] == "Test Entity"

    def test_flatten_jsonl_file(self, jsonl_file):
        """Test flattening a JSONL file."""
        result = tm.flatten(jsonl_file, name="from_jsonl")

        assert len(result.main) == 10

    def test_flatten_with_file_path_and_config(self, json_file):
        """Test flatten with file path and configuration options."""
        config = TransmogConfig(
            array_mode=ArrayMode.INLINE,
            time_field="_timestamp",
        )
        result = tm.flatten(json_file, name="custom", config=config)

        record = result.main[0]
        assert "_timestamp" in record
        assert "metadata_created_at" in record  # Uses underscore separator

    def test_flatten_nonexistent_path_object(self, tmp_path):
        """A Path that does not exist is a missing file, not JSON."""
        missing = tmp_path / "does_not_exist.json"
        with pytest.raises(ValidationError, match="File not found"):
            tm.flatten(missing, name="test")

    def test_flatten_non_json_string(self):
        """A plain string is parsed as JSON, not as a file path."""
        with pytest.raises(ValidationError, match="Error parsing JSON data"):
            tm.flatten("nonexistent.csv", name="test")


class TestFlattenWithIterator:
    """Test flatten() with generator/iterator inputs."""

    def test_flatten_generator_input(self):
        """Test that flatten() accepts a generator and processes all records."""

        def record_generator():
            for i in range(10):
                yield {"id": i, "name": f"Record {i}"}

        result = tm.flatten(record_generator(), name="gen")

        assert len(result.main) == 10
        assert result.main[0]["name"] == "Record 0"
        assert result.main[-1]["name"] == "Record 9"

    def test_flatten_generator_with_nested_data(self):
        """Test that flatten() handles generators yielding nested records."""

        def nested_generator():
            for i in range(5):
                yield {
                    "id": i,
                    "info": {"city": f"City {i}", "country": "US"},
                    "tags": [{"label": f"tag_{i}"}],
                }

        result = tm.flatten(nested_generator(), name="nested_gen")

        assert len(result.main) == 5
        assert "info_city" in result.main[0]
        assert len(result.tables) > 0

    def test_flatten_empty_generator(self):
        """Test that flatten() handles an empty generator."""
        result = tm.flatten(iter([]), name="empty_gen")
        assert len(result.main) == 0


class TestFlattenStream:
    """Test flatten_stream() function."""

    def test_flatten_stream_writes_csv_rows(self, batch_data, output_dir):
        """Streaming CSV output contains every input record."""
        result = tm.flatten_stream(
            batch_data,
            output_path=str(output_dir / "stream_output"),
            name="streamed",
            output_format="csv",
        )

        main_file = output_file_for(result, "streamed", ".csv")
        rows = read_csv_rows(main_file)
        assert len(rows) == len(batch_data)
        assert rows[0]["name"] == "Record 1"
        assert rows[-1]["name"] == "Record 10"

    def test_flatten_stream_large_data(self, large_json_file, output_dir):
        """Streaming a 1000-record file writes 1000 CSV rows."""
        result = tm.flatten_stream(
            large_json_file,
            output_path=str(output_dir / "large_stream"),
            name="large",
            output_format="csv",
            config=TransmogConfig(batch_size=100),
        )

        rows = read_csv_rows(output_file_for(result, "large", ".csv"))
        assert len(rows) == 1000
        assert rows[0]["id"] == "1"
        assert rows[-1]["id"] == "1000"

    def test_flatten_stream_parquet_format(self, batch_data, output_dir):
        """Streaming Parquet output preserves row count and names."""
        import pyarrow.parquet as pq

        result = tm.flatten_stream(
            batch_data,
            output_path=str(output_dir / "parquet_stream"),
            name="parquet_data",
            output_format="parquet",
        )

        main_file = output_file_for(result, "parquet_data", ".parquet")
        table = pq.read_table(str(main_file))
        assert table.num_rows == len(batch_data)
        assert table.column("name").to_pylist()[0] == "Record 1"

    def test_flatten_stream_separate_arrays(self, array_data, output_dir):
        """SEPARATE mode writes child tables as additional CSV files."""
        result = tm.flatten_stream(
            array_data,
            output_path=str(output_dir / "options_stream"),
            name="options_test",
            output_format="csv",
            config=TransmogConfig(
                array_mode=ArrayMode.SEPARATE,
                time_field="_timestamp",
            ),
        )

        main_rows = read_csv_rows(output_file_for(result, "options_test", ".csv"))
        assert len(main_rows) == 1
        assert main_rows[0]["name"] == "Company"
        assert "_timestamp" in main_rows[0]
        assert "tags" not in main_rows[0]

        names = {Path(path).name for path in result}
        assert "options_test_employees.csv" in names
        employee_rows = read_csv_rows(
            output_file_for(result, "options_test_employees", ".csv")
        )
        assert len(employee_rows) == 2

    def test_flatten_stream_consolidation_preserves_all_records(self, output_dir):
        """consolidate=True merges all batches into one file with all records."""
        data = [{"id": i, "value": f"item_{i}"} for i in range(25)]

        result = tm.flatten_stream(
            data,
            output_path=str(output_dir / "consolidated"),
            name="items",
            output_format="csv",
            config=TransmogConfig(batch_size=5),
            consolidate=True,
        )

        main_file = output_file_for(result, "items", ".csv")
        rows = read_csv_rows(main_file)
        assert len(rows) == 25
        assert {row["id"] for row in rows} == {str(i) for i in range(25)}
        assert not list((output_dir / "consolidated").glob("items_part_*.csv"))

    def test_flatten_stream_without_consolidation_keeps_parts(self, output_dir):
        """consolidate=False retains numbered part files."""
        data = [{"id": i, "value": f"item_{i}"} for i in range(10)]

        result = tm.flatten_stream(
            data,
            output_path=str(output_dir / "parts"),
            name="items",
            output_format="csv",
            config=TransmogConfig(batch_size=5),
            consolidate=False,
        )

        part_names = sorted(
            Path(path).name for path in result if "part_" in Path(path).name
        )
        assert part_names == ["items_part_0000.csv", "items_part_0001.csv"]

        rows = []
        for path in result:
            if Path(path).name.startswith("items_part_"):
                rows.extend(read_csv_rows(path))
        assert [row["id"] for row in rows] == [str(i) for i in range(10)]
        assert not (output_dir / "parts" / "items.csv").exists()

    def test_flatten_stream_coerce_schema_unifies_parts(self, output_dir):
        """coerce_schema=True rewrites minority parts onto the majority schema."""
        data = [{"id": i, "name": f"n{i}"} for i in range(4)] + [
            {"id": i, "name": f"n{i}", "extra": f"e{i}"} for i in range(4, 8)
        ]

        result = tm.flatten_stream(
            data,
            output_path=str(output_dir / "coerced"),
            name="items",
            output_format="csv",
            config=TransmogConfig(batch_size=4),
            coerce_schema=True,
            consolidate=False,
        )

        part_paths = sorted(
            Path(path) for path in result if Path(path).name.startswith("items_part_")
        )
        assert len(part_paths) == 2

        schemas = [set(read_csv_rows(path)[0].keys()) for path in part_paths]
        assert schemas[0] == schemas[1]
        assert "extra" in schemas[0]

        all_rows = []
        for path in part_paths:
            all_rows.extend(read_csv_rows(path))
        assert len(all_rows) == 8
        assert all_rows[0]["extra"] == ""
        assert all_rows[-1]["extra"] == "e7"

    def test_flatten_stream_invalid_format(self, simple_data, output_dir):
        """Unknown output formats raise ConfigurationError."""
        with pytest.raises(ConfigurationError, match="Unsupported format"):
            tm.flatten_stream(
                simple_data,
                output_path=str(output_dir / "invalid"),
                name="test",
                output_format="invalid_format",
            )

    def test_flatten_stream_avro_codec_alias(self, output_dir):
        """codec= is accepted as an alias for Avro compression."""
        import fastavro

        result = tm.flatten_stream(
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            output_path=str(output_dir / "avro_codec"),
            name="items",
            output_format="avro",
            codec="deflate",
        )

        avro_file = output_file_for(result, "items", ".avro")
        with open(avro_file, "rb") as handle:
            records = list(fastavro.reader(handle))
        assert [record["name"] for record in records] == ["Alice", "Bob"]


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
        assert record["zero_int"] == 0
        assert record["zero_float"] == 0.0
        assert record["false_bool"] is False

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

    def test_invalid_utf8_bytes(self):
        """Test that invalid UTF-8 bytes raise an appropriate error."""
        invalid_bytes = b"\x80\x81\x82"

        with pytest.raises(UnicodeDecodeError):
            tm.flatten(invalid_bytes, name="test")
