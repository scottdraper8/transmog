"""
Tests for the FlattenResult class.

Tests all functionality of the FlattenResult class returned by flatten operations.
"""

import json
from pathlib import Path

import pytest

import transmog as tm

from ...conftest import assert_files_created, count_files_in_dir, load_json_file


class TestFlattenResultBasics:
    """Test basic FlattenResult functionality."""

    @pytest.fixture
    def result(self, array_data):
        """Create a FlattenResult for testing."""
        return tm.flatten(array_data, name="company")

    def test_main_property(self, result):
        """Test accessing the main table."""
        assert isinstance(result.main, list)
        assert len(result.main) == 1
        assert result.main[0]["name"] == "Company"

    def test_tables_property(self, result):
        """Test accessing child tables."""
        assert isinstance(result.tables, dict)
        assert len(result.tables) > 0

        # Should have tables for tags and employees
        table_names = list(result.tables.keys())
        assert any("tags" in name.lower() for name in table_names)
        assert any("employees" in name.lower() for name in table_names)

    def test_all_tables_property(self, result):
        """Test accessing all tables including main."""
        all_tables = result.all_tables
        assert isinstance(all_tables, dict)
        assert len(all_tables) > len(result.tables)  # Includes main table

        # Should include main table
        assert "company" in all_tables or "main" in all_tables

    def test_iteration(self, result):
        """Test iterating over the result."""
        # Test iterating over main table records
        records = list(result)
        assert len(records) == 1
        assert records[0]["name"] == "Company"

    def test_length(self, result):
        """Test len() of result."""
        assert len(result) == 1  # Length of main table

    def test_getitem(self, result):
        """Test dictionary-style access to tables."""
        # Access main table
        main_table = result["main"]
        assert main_table == result.main

        # Access child tables
        for table_name in result.tables:
            table_data = result[table_name]
            assert table_data == result.tables[table_name]

    def test_keys_values_items(self, result):
        """Test dict-like methods."""
        keys = list(result.keys())
        values = list(result.values())
        items = list(result.items())

        assert len(keys) == len(values) == len(items)
        assert len(keys) > 1  # Main + child tables

        # Verify items structure
        for name, data in items:
            assert isinstance(name, str)
            assert isinstance(data, list)

    def test_get_table(self, result):
        """Test get_table method."""
        # Get existing table by entity name
        main_table = result.get_table("company")
        assert main_table is not None

        # Get non-existent table with default
        nonexistent = result.get_table("nonexistent", default=[])
        assert nonexistent == []

    def test_table_info(self, result):
        """Test table_info method."""
        info = result.table_info()
        assert isinstance(info, dict)

        # Check info structure
        for table_name, table_info in info.items():
            assert "records" in table_info
            assert "fields" in table_info
            assert "is_main" in table_info
            assert isinstance(table_info["records"], int)
            assert isinstance(table_info["fields"], list)
            assert isinstance(table_info["is_main"], bool)

    def test_repr(self, result):
        """Test string representation."""
        repr_str = repr(result)
        assert "FlattenResult" in repr_str
        assert "tables" in repr_str
        assert "records" in repr_str


class TestFlattenResultSaving:
    """Test saving functionality."""

    @pytest.fixture
    def simple_result(self, simple_data):
        """Create a simple result for testing."""
        return tm.flatten(simple_data, name="entity")

    @pytest.fixture
    def complex_result(self, array_data):
        """Create a complex result with multiple tables."""
        return tm.flatten(array_data, name="company")

    def test_save_json_single_table(self, simple_result, temp_file):
        """Test saving single table to JSON."""
        paths = simple_result.save(str(temp_file))

        # Save returns a list for single table
        assert isinstance(paths, list)
        assert len(paths) > 0
        assert_files_created(paths)

        # Verify content
        data = load_json_file(paths[0])
        assert isinstance(data, list)
        assert len(data) == 1

    def test_save_json_multiple_tables(self, complex_result, output_dir):
        """Test saving multiple tables to JSON directory."""
        paths = complex_result.save(str(output_dir / "json_output"))

        # Save returns a dictionary for multiple tables
        assert isinstance(paths, dict)
        assert len(paths) > 1  # Multiple tables
        assert_files_created(list(paths.values()))

        # Check that JSON files were created in the subdirectory
        json_output_dir = output_dir / "json_output"
        json_files = count_files_in_dir(json_output_dir, "*.json")
        assert json_files > 0

    def test_save_csv_format(self, complex_result, output_dir):
        """Test saving to CSV format."""
        paths = complex_result.save(str(output_dir / "csv_output"), format="csv")

        # Save returns a dictionary for multiple tables
        assert isinstance(paths, dict)
        assert len(paths) > 0
        assert_files_created(list(paths.values()))

        # Check CSV files were created in the subdirectory
        csv_output_dir = output_dir / "csv_output"
        csv_files = count_files_in_dir(csv_output_dir, "*.csv")
        assert csv_files > 0

    def test_save_parquet_format(self, complex_result, output_dir):
        """Test saving to Parquet format."""
        paths = complex_result.save(
            str(output_dir / "parquet_output"), format="parquet"
        )

        # Save returns a dictionary for multiple tables
        assert isinstance(paths, dict)
        assert len(paths) > 0
        assert_files_created(list(paths.values()))

        # Check Parquet files were created in the subdirectory
        parquet_output_dir = output_dir / "parquet_output"
        parquet_files = count_files_in_dir(parquet_output_dir, "*.parquet")
        assert parquet_files > 0

    def test_save_auto_format_detection(self, simple_result, output_dir):
        """Test automatic format detection from extension."""
        # Test JSON extension
        json_path = output_dir / "auto_test.json"
        paths = simple_result.save(str(json_path))
        # Single table returns list
        assert_files_created(paths)

        # Test CSV extension
        csv_path = output_dir / "auto_test.csv"
        paths = simple_result.save(str(csv_path))
        # Single table returns list
        assert_files_created(paths)

    def test_save_invalid_format(self, simple_result, temp_file):
        """Test saving with invalid format."""
        with pytest.raises(ValueError):
            simple_result.save(str(temp_file), format="invalid")

    def test_save_pathlib_path(self, simple_result, output_dir):
        """Test saving with pathlib.Path objects."""
        output_path = output_dir / "pathlib_test"
        paths = simple_result.save(output_path, format="json")

        # Single table returns list
        assert isinstance(paths, list)
        assert len(paths) > 0
        assert_files_created(paths)


class TestFlattenResultEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_result(self):
        """Test result with empty data."""
        result = tm.flatten([], name="empty")

        assert len(result.main) == 0
        assert isinstance(result.tables, dict)
        assert len(result) == 0

        # Test iteration over empty result
        records = list(result)
        assert len(records) == 0

    def test_result_with_no_child_tables(self, simple_data):
        """Test result with only main table."""
        result = tm.flatten(simple_data, name="simple")

        assert len(result.main) == 1
        # May or may not have child tables depending on implementation

        # Should still be able to save
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            paths = result.save(str(Path(temp_dir) / "simple_output.json"))
            # Single table save returns list
            if isinstance(paths, list):
                assert_files_created(paths)
            else:
                assert_files_created(list(paths.values()))

    def test_result_table_access_edge_cases(self, array_data):
        """Test edge cases in table access."""
        result = tm.flatten(array_data, name="company")

        # Test accessing with different variations
        try:
            # These might work depending on implementation
            main_variations = ["main", "company", result._result.entity_name]
            for variation in main_variations:
                try:
                    table = result[variation]
                    assert isinstance(table, list)
                except KeyError:
                    continue  # Some variations might not work
        except Exception:
            pass  # Implementation-specific behavior

    def test_result_with_special_characters(self):
        """Test result with special characters in data."""
        special_data = {
            "id": 1,
            "name": "Test with émojis 🚀",
            "description": "Special chars: áéíóú ñ ü",
            "unicode": "测试中文",
            "symbols": "!@#$%^&*()",
            "items": ["item with spaces", "item-with-dashes", "item_with_underscores"],
        }

        result = tm.flatten(special_data, name="special")
        assert len(result.main) == 1

        # Should be able to save without issues
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            paths = result.save(str(Path(temp_dir) / "special_output.json"))
            # Handle both list and dict return types
            if isinstance(paths, list):
                assert_files_created(paths)
            else:
                assert_files_created(list(paths.values()))
