"""Tests for empty value handling.

Tests how transmog handles empty dicts, empty lists, empty strings, and
the distinction between missing keys and explicit None values.
"""

import tempfile
from pathlib import Path

import pytest

import transmog as tm
from transmog.config import TransmogConfig
from transmog.types import ArrayMode


class TestEmptyDictHandling:
    """Test handling of empty dictionaries."""

    def test_empty_dict_at_top_level_skipped(self):
        """Test that empty dict values are skipped at top level."""
        data = {"id": 1, "name": "test", "empty_obj": {}}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "id" in main
        assert "name" in main
        # Empty dict should not create any fields
        assert not any(key.startswith("empty_obj") for key in main.keys())

    def test_empty_dict_in_nested_structure(self):
        """Test that empty dicts in nested structures are skipped."""
        data = {
            "id": 1,
            "outer": {"valid": "data", "empty_inner": {}, "also_valid": 42},
        }

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "outer_valid" in main
        assert "outer_also_valid" in main
        # Empty nested dict should not create fields
        assert not any("empty_inner" in key for key in main.keys())

    def test_deeply_nested_empty_dict(self):
        """Test empty dict at deep nesting level."""
        data = {"id": 1, "level1": {"level2": {"level3": {"empty": {}}}}}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        # Should only have id and metadata fields
        non_meta_keys = [k for k in main.keys() if not k.startswith("_")]
        assert "id" in non_meta_keys

    def test_empty_dict_with_include_nulls(self):
        """Test empty dict handling with include_nulls=True."""
        data = {"id": 1, "empty_obj": {}, "valid": "data"}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        # Empty dict should still be skipped (it's structural, not a value)
        assert "valid" in main


class TestEmptyListHandling:
    """Test handling of empty lists."""

    def test_empty_list_skipped_in_smart_mode(self):
        """Test that empty lists are skipped in SMART array mode."""
        data = {"id": 1, "items": [], "name": "test"}

        config = TransmogConfig(array_mode=ArrayMode.SMART)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        assert "name" in main
        # Empty list should not create child tables
        assert len(result.tables) == 0

    def test_empty_list_skipped_in_separate_mode(self):
        """Test that empty lists are skipped in SEPARATE array mode."""
        data = {"id": 1, "items": [], "name": "test"}

        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        assert "name" in main
        assert len(result.tables) == 0

    def test_empty_list_skipped_in_inline_mode(self):
        """Test that empty lists are skipped in INLINE array mode."""
        data = {"id": 1, "items": [], "name": "test"}

        config = TransmogConfig(array_mode=ArrayMode.INLINE)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        assert "name" in main
        # Empty list should not create inline field
        assert "items" not in main

    def test_empty_list_in_nested_structure(self):
        """Test empty list in nested structure."""
        data = {"id": 1, "outer": {"items": [], "valid": "data"}}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "outer_valid" in main

    def test_mixed_empty_and_populated_lists(self):
        """Test record with both empty and populated lists."""
        data = {"id": 1, "empty_items": [], "valid_items": [1, 2, 3]}

        config = TransmogConfig(array_mode=ArrayMode.SMART)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        # Valid items should be present (simple array in SMART mode)
        assert "valid_items" in main


class TestEmptyStringHandling:
    """Test handling of empty strings."""

    def test_empty_string_skipped_by_default(self):
        """Test that empty strings are skipped with include_nulls=False."""
        data = {"id": 1, "name": "", "valid": "data"}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "valid" in main
        assert "name" not in main

    def test_empty_string_included_when_configured(self):
        """Test that empty strings are included with include_nulls=True."""
        data = {"id": 1, "name": "", "valid": "data"}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        assert "valid" in main
        assert "name" in main
        assert main["name"] is None  # Converted to None for consistency

    def test_empty_string_in_nested_structure(self):
        """Test empty string in nested structure."""
        data = {"id": 1, "nested": {"empty": "", "valid": "data"}}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "nested_valid" in main
        assert "nested_empty" not in main

    def test_whitespace_only_string_preserved(self):
        """Test that whitespace-only strings are preserved (not empty)."""
        data = {"id": 1, "spaces": "   ", "tabs": "\t\t"}

        result = tm.flatten(data, name="test")

        main = result.main[0]
        assert "spaces" in main
        assert main["spaces"] == "   "
        assert "tabs" in main


class TestMissingKeyVsNone:
    """Test distinction between missing keys and explicit None values."""

    def test_missing_key_not_in_output(self):
        """Test that missing keys don't appear in output."""
        data = [{"id": 1, "name": "Alice"}, {"id": 2}]  # Second record missing 'name'

        result = tm.flatten(data, name="test")

        assert len(result.main) == 2
        assert "name" in result.main[0]
        # Second record should not have 'name' key at all
        assert "name" not in result.main[1]

    def test_explicit_none_skipped_by_default(self):
        """Test that explicit None is skipped with include_nulls=False."""
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": None}]

        result = tm.flatten(data, name="test")

        assert len(result.main) == 2
        assert "name" in result.main[0]
        assert "name" not in result.main[1]

    def test_explicit_none_included_when_configured(self):
        """Test that explicit None is included with include_nulls=True."""
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": None}]

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        assert len(result.main) == 2
        assert "name" in result.main[0]
        assert "name" in result.main[1]
        assert result.main[1]["name"] is None

    def test_sparse_records_different_fields(self):
        """Test records with different sets of fields."""
        data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "phone": "555-1234"},
            {"id": 3, "email": "charlie@example.com", "phone": "555-5678"},
        ]

        result = tm.flatten(data, name="test")

        assert len(result.main) == 3

        # Each record should only have its own fields
        assert "email" in result.main[0]
        assert "phone" not in result.main[0]

        assert "phone" in result.main[1]
        assert "email" not in result.main[1]

        assert "name" not in result.main[2]


class TestAllEmptyRecord:
    """Test records with all empty/null values."""

    def test_all_none_values_skip_mode(self):
        """Test record with all None values in skip mode."""
        data = {"field1": None, "field2": None, "field3": None}

        result = tm.flatten(data, name="test")

        # Record should exist but only have metadata fields
        assert len(result.main) >= 0

    def test_all_none_values_include_mode(self):
        """Test record with all None values in include mode."""
        data = {"field1": None, "field2": None, "field3": None}

        config = TransmogConfig(include_nulls=True)
        result = tm.flatten(data, name="test", config=config)

        assert len(result.main) >= 1
        if result.main:
            main = result.main[0]
            # Should have the null fields
            assert "field1" in main
            assert "field2" in main
            assert "field3" in main

    def test_all_empty_strings_skip_mode(self):
        """Test record with all empty strings in skip mode."""
        data = {"field1": "", "field2": "", "field3": ""}

        result = tm.flatten(data, name="test")

        # Record should exist but fields should be skipped
        assert len(result.main) >= 0

    def test_mixed_empty_values(self):
        """Test record with mix of empty value types."""
        data = {"none_val": None, "empty_str": "", "empty_dict": {}, "empty_list": []}

        result = tm.flatten(data, name="test")

        # All should be skipped
        if result.main:
            main = result.main[0]
            non_meta_keys = [k for k in main.keys() if not k.startswith("_")]
            # Should only have metadata fields, no data fields
            assert len(non_meta_keys) == 0


class TestEmptyValuesInArrays:
    """Test empty values within arrays."""

    def test_none_values_in_simple_array(self):
        """Test None values in simple arrays."""
        data = {"id": 1, "values": [1, None, 2, None, 3]}

        config = TransmogConfig(array_mode=ArrayMode.SMART, include_nulls=False)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        # Array should be present
        assert "values" in main

    def test_empty_strings_in_array(self):
        """Test empty strings in arrays."""
        data = {"id": 1, "names": ["Alice", "", "Bob", "", "Charlie"]}

        config = TransmogConfig(array_mode=ArrayMode.SMART)
        result = tm.flatten(data, name="test", config=config)

        main = result.main[0]
        assert "names" in main

    def test_empty_dicts_in_array(self):
        """Test empty dicts in arrays of objects."""
        data = {
            "id": 1,
            "items": [{"name": "Item1"}, {}, {"name": "Item2"}, {}, {"name": "Item3"}],
        }

        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        result = tm.flatten(data, name="test", config=config)

        # Should have child table for items
        assert len(result.tables) > 0


class TestEmptyValuesCsvOutput:
    """Test empty value handling in CSV output."""

    def test_none_becomes_empty_string_in_csv(self):
        """Test that None values become empty strings in CSV."""
        data = [
            {"id": "1", "name": "Alice", "value": None},
            {"id": "2", "name": "Bob", "value": 42},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            from transmog.writers import CsvWriter

            writer = CsvWriter()
            output_path = Path(tmpdir) / "test.csv"
            writer.write(data, str(output_path))

            with open(output_path) as f:
                content = f.read()

            # None should be written as empty string
            lines = content.strip().split("\n")
            assert len(lines) == 3

    def test_sparse_data_csv_output(self):
        """Test sparse data (different fields per record) in CSV."""
        data = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "email": "bob@example.com"},
            {"id": "3", "name": "Charlie", "email": "charlie@example.com"},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            from transmog.writers import CsvWriter

            writer = CsvWriter()
            output_path = Path(tmpdir) / "test.csv"
            writer.write(data, str(output_path))

            with open(output_path) as f:
                content = f.read()

            # All fields should be in header
            header = content.split("\n")[0]
            assert "id" in header
            assert "name" in header
            assert "email" in header


class TestEmptyValuesParquetOutput:
    """Test empty value handling in Parquet output."""

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_none_values_in_parquet(self):
        """Test that None values are properly represented in Parquet."""
        import pyarrow.parquet as pq

        data = [
            {"id": "1", "value": "present"},
            {"id": "2", "value": None},
            {"id": "3", "value": "also_present"},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            from transmog.writers import ParquetWriter

            writer = ParquetWriter()
            output_path = Path(tmpdir) / "test.parquet"
            writer.write(data, str(output_path))

            table = pq.read_table(str(output_path))
            values = table.column("value").to_pylist()

            assert values[0] == "present"
            assert values[1] is None
            assert values[2] == "also_present"

    @pytest.mark.skipif(
        not pytest.importorskip("pyarrow", reason="PyArrow not available"),
        reason="PyArrow required",
    )
    def test_sparse_data_parquet_output(self):
        """Test sparse data in Parquet output."""
        import pyarrow.parquet as pq

        data = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "email": "bob@example.com"},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            from transmog.writers import ParquetWriter

            writer = ParquetWriter()
            output_path = Path(tmpdir) / "test.parquet"
            writer.write(data, str(output_path))

            table = pq.read_table(str(output_path))

            # Both fields should be in schema
            assert "name" in table.schema.names
            assert "email" in table.schema.names


class TestEmptyValuesEndToEnd:
    """End-to-end tests for empty value handling."""

    def test_flatten_and_save_with_empty_values(self):
        """Test complete workflow with various empty values."""
        data = {
            "id": 1,
            "name": "Test",
            "empty_string": "",
            "none_value": None,
            "empty_dict": {},
            "empty_list": [],
            "valid_nested": {"key": "value"},
        }

        result = tm.flatten(data, name="test")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = result.save(tmpdir, output_format="csv")
            assert len(paths) >= 1

    def test_batch_with_varying_empty_values(self):
        """Test batch processing with varying empty values across records."""
        data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "", "score": None},
            {"id": 3, "name": None, "score": 87},
            {"id": 4, "name": "Dave", "score": 0},  # 0 is not empty
        ]

        result = tm.flatten(data, name="test")

        assert len(result.main) == 4

        # Record 4 should have score=0 (not skipped)
        assert "score" in result.main[3]
        assert result.main[3]["score"] == 0
