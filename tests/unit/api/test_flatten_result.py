"""Tests for FlattenResult class and its methods."""

import sys
import tempfile
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

import transmog as tm
from transmog.error import OutputError, ProcessingError
from transmog.types import RecoveryMode

from ...conftest import assert_files_created, count_files_in_dir


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
        table_names = list(result.tables.keys())
        assert any("employees" in name.lower() for name in table_names)
        assert "tags" in result.main[0]
        assert isinstance(result.main[0]["tags"], list)

    def test_all_tables_property(self, result):
        """Test accessing all tables including main."""
        all_tables = result.all_tables
        assert isinstance(all_tables, dict)
        assert len(all_tables) > len(result.tables)
        assert "company" in all_tables or "main" in all_tables

    def test_iteration(self, result):
        """Test iterating over result."""
        records = list(result)
        assert len(records) == 1
        assert records[0]["name"] == "Company"

    def test_length(self, result):
        """Test len() of result."""
        assert len(result) == 1

    def test_getitem(self, result):
        """Test dictionary-style access to tables."""
        main_table = result["main"]
        assert main_table == result.main

        for table_name in result.tables:
            table_data = result[table_name]
            assert table_data == result.tables[table_name]

    def test_keys_values_items(self, result):
        """Test dict-like methods."""
        keys = list(result.keys())
        values = list(result.values())
        items = list(result.items())

        assert len(keys) == len(values) == len(items)
        assert len(keys) > 1

        for name, data in items:
            assert isinstance(name, str)
            assert isinstance(data, list)

    def test_table_info(self, result):
        """Test table_info method."""
        info = result.table_info()
        assert isinstance(info, dict)

        for _table_name, table_info in info.items():
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

    def test_save_csv_single_table(self, simple_result, temp_file):
        """Test saving single table to CSV."""
        csv_file = str(temp_file).replace(".json", ".csv")
        paths = simple_result.save(csv_file, output_format="csv")

        assert isinstance(paths, list)
        assert len(paths) > 0
        assert_files_created(paths)

    def test_save_csv_multiple_tables(self, complex_result, output_dir):
        """Test saving multiple tables to CSV directory."""
        paths = complex_result.save(str(output_dir / "csv_output"), output_format="csv")

        assert isinstance(paths, dict)
        assert len(paths) > 1
        assert_files_created(list(paths.values()))

        csv_output_dir = output_dir / "csv_output"
        csv_files = count_files_in_dir(csv_output_dir, "*.csv")
        assert csv_files > 0

    def test_save_parquet_format(self, complex_result, output_dir):
        """Test saving to Parquet format."""
        paths = complex_result.save(
            str(output_dir / "parquet_output"), output_format="parquet"
        )

        assert isinstance(paths, dict)
        assert len(paths) > 0
        assert_files_created(list(paths.values()))

        parquet_output_dir = output_dir / "parquet_output"
        parquet_files = count_files_in_dir(parquet_output_dir, "*.parquet")
        assert parquet_files > 0

    def test_save_auto_format_detection(self, simple_result, output_dir):
        """Test automatic format detection from extension."""
        csv_path = output_dir / "auto_test.csv"
        paths = simple_result.save(str(csv_path))
        assert_files_created(paths)

        parquet_path = output_dir / "auto_test.parquet"
        paths = simple_result.save(str(parquet_path))
        assert_files_created(paths)

    def test_save_invalid_format(self, simple_result, temp_file):
        """Test saving with invalid format."""
        with pytest.raises(ValueError):
            simple_result.save(str(temp_file), output_format="invalid")

    def test_save_pathlib_path(self, simple_result, output_dir):
        """Test saving with pathlib.Path objects."""
        output_path = output_dir / "pathlib_test"
        paths = simple_result.save(output_path, output_format="csv")

        assert isinstance(paths, list)
        assert len(paths) > 0
        assert_files_created(paths)


class TestFlattenResultEdgeCases:
    """Test edge cases for FlattenResult operations."""

    def test_empty_result(self):
        """Test result with empty data."""
        result = tm.flatten([], name="empty")

        assert len(result.main) == 0
        assert isinstance(result.tables, dict)
        assert len(result) == 0

        records = list(result)
        assert len(records) == 0

    def test_result_with_no_child_tables(self, simple_data):
        """Test result with only main table."""
        result = tm.flatten(simple_data, name="simple")

        assert len(result.main) == 1

        with tempfile.TemporaryDirectory() as temp_dir:
            paths = result.save(str(Path(temp_dir) / "simple_output.csv"))
            if isinstance(paths, list):
                assert_files_created(paths)
            else:
                assert_files_created(list(paths.values()))

    def test_result_table_access_case_sensitivity(self):
        """Test table access is case sensitive."""
        data = {"id": 1, "name": "Test", "items": [{"id": 1, "value": "test"}]}
        result = tm.flatten(data, name="test")

        with pytest.raises(KeyError):
            _ = result["MAIN"]

        with pytest.raises(KeyError):
            _ = result["Main"]

    def test_result_iteration_multiple_times(self):
        """Test iterating over result multiple times."""
        data = [{"id": i, "value": f"item_{i}"} for i in range(10)]
        result = tm.flatten(data, name="items")

        first_iteration = list(result)
        second_iteration = list(result)
        assert first_iteration == second_iteration

    def test_result_dict_interface_completeness(self):
        """Test complete dict-like interface."""
        data = {"company": {"name": "Test", "employees": [{"name": "Alice"}]}}
        result = tm.flatten(data, name="company")

        keys = list(result.keys())
        assert len(keys) >= 1
        assert "main" in keys or result._result.entity_name in keys

        values = list(result.values())
        assert len(values) == len(keys)
        for value in values:
            assert isinstance(value, list)

        items = list(result.items())
        assert len(items) == len(keys)
        for name, data in items:
            assert isinstance(name, str)
            assert isinstance(data, list)

        assert "main" in result or result._result.entity_name in result

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

        with tempfile.TemporaryDirectory() as temp_dir:
            paths = result.save(str(Path(temp_dir) / "special_output.csv"))
            if isinstance(paths, list):
                assert_files_created(paths)
            else:
                assert_files_created(list(paths.values()))

    def test_result_with_unicode_field_names(self):
        """Test result with unicode field names."""
        data = {
            "名前": "Japanese name",
            "prénom": "French name",
            "имя": "Russian name",
            "🌟星": "Star emoji",
            "café": "Accented chars",
        }

        result = tm.flatten(data, name="unicode_fields")
        assert len(result.main) == 1

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "unicode.csv"
            saved_paths = result.save(str(output_path))
            assert isinstance(saved_paths, (str, dict, list))

    def test_result_large_table_count(self):
        """Test result with many tables."""
        data = {}
        for i in range(100):
            data[f"array_{i}"] = [{"id": j, "value": f"item_{j}"} for j in range(5)]

        result = tm.flatten(data, name="many_tables")

        info = result.table_info()
        assert isinstance(info, dict)
        assert len(info) >= 1

        count = 0
        for _record in result:
            count += 1
            if count > 100:
                break

    def test_result_memory_behavior(self):
        """Test memory behavior with large results."""
        large_data = [{"id": i, "data": "x" * 100} for i in range(1000)]
        result = tm.flatten(large_data, name="large")

        assert len(result.main) == 1000

        info = result.table_info()
        assert isinstance(info, dict)

        count = 0
        for _record in result:
            count += 1
            if count > 10:
                break
        assert count > 0

    def test_result_concurrent_access(self):
        """Test concurrent access to result data."""
        import threading
        import time

        data = [{"id": i, "value": f"item_{i}"} for i in range(100)]
        result = tm.flatten(data, name="concurrent")

        results = []
        errors = []

        def access_result():
            try:
                main_len = len(result.main)
                table_info = result.table_info()
                keys = list(result.keys())
                results.append((main_len, len(table_info), len(keys)))
                time.sleep(0.01)
            except Exception as e:
                errors.append(e)

        threads = []
        for _ in range(5):
            thread = threading.Thread(target=access_result)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0
        assert len(results) == 5

        first_result = results[0]
        for result_tuple in results[1:]:
            assert result_tuple == first_result

    def test_result_string_representations(self):
        """Test string representation methods."""
        data = {"company": {"name": "Test", "employees": [{"name": "Alice"}]}}
        result = tm.flatten(data, name="company")

        repr_str = repr(result)
        assert "FlattenResult" in repr_str
        assert isinstance(repr_str, str)
        assert len(repr_str) > 0

        str_str = str(result)
        assert isinstance(str_str, str)
        assert len(str_str) > 0

        empty_result = tm.flatten([], name="empty")
        empty_repr = repr(empty_result)
        assert "FlattenResult" in empty_repr
        assert isinstance(empty_repr, str)

    def test_result_with_null_and_none_values(self):
        """Test result handling of null and None values."""
        data = {
            "null_value": None,
            "nested": {
                "also_null": None,
                "empty_string": "",
                "zero": 0,
                "false": False,
            },
            "array_with_nulls": [
                {"id": 1, "value": None},
                {"id": 2, "value": "not_null"},
                None,
            ],
        }

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(data, name="nulls", config=config)
        assert isinstance(result, tm.FlattenResult)

        info = result.table_info()
        assert isinstance(info, dict)


class TestFlattenResultSaveErrors:
    """Test error handling in save operations."""

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows file permissions work differently from Unix"
    )
    def test_result_save_permission_errors(self):
        """Test save operations with permission errors."""
        data = {"id": 1, "name": "Test"}
        result = tm.flatten(data, name="test")

        with tempfile.TemporaryDirectory() as temp_dir:
            readonly_dir = Path(temp_dir) / "readonly"
            readonly_dir.mkdir()

            try:
                readonly_dir.chmod(0o444)
                readonly_file = readonly_dir / "output.csv"

                with pytest.raises(
                    (ProcessingError, OutputError, PermissionError, OSError)
                ):
                    result.save(str(readonly_file))
            except (OSError, NotImplementedError):
                pass
            finally:
                try:
                    readonly_dir.chmod(0o755)
                except (OSError, NotImplementedError):
                    pass

    def test_result_save_disk_full_simulation(self):
        """Test save operations when disk is full."""
        data = {"id": 1, "name": "Test"}
        result = tm.flatten(data, name="test")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "output.csv"

            with patch("builtins.open", mock_open()) as mock_file:
                mock_file.return_value.write.side_effect = OSError(
                    "No space left on device"
                )

                with pytest.raises((OutputError, ProcessingError, OSError)):
                    result.save(str(output_path))

    def test_result_serialization_edge_cases(self):
        """Test serialization edge cases."""
        data = {"company": {"name": "Test", "employees": [{"name": "Alice"}]}}
        result = tm.flatten(data, name="company")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "encoded.csv"

            try:
                saved_paths = result.save(str(output_path))

                if isinstance(saved_paths, str):
                    assert Path(saved_paths).exists()
                elif isinstance(saved_paths, dict):
                    for path in saved_paths.values():
                        assert Path(path).exists()
                elif isinstance(saved_paths, list):
                    for path in saved_paths:
                        assert Path(path).exists()

            except (ProcessingError, OutputError):
                pass
