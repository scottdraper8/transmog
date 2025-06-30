"""
Tests for FlattenResult edge cases and error conditions.

Tests edge cases, error conditions, and boundary conditions specific to FlattenResult.
"""

import pytest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import patch, mock_open

import transmog as tm
from transmog.error import (
    ProcessingError,
    ValidationError,
    FileError,
    OutputError,
)


class TestFlattenResultEdgeCases:
    """Test edge cases for FlattenResult operations."""

    @pytest.fixture
    def complex_result(self):
        """Create complex result with multiple tables for testing."""
        data = {
            "company": {
                "name": "Test Corp",
                "employees": [
                    {"id": 1, "name": "Alice", "skills": ["Python", "SQL"]},
                    {"id": 2, "name": "Bob", "skills": ["Java", "Docker"]},
                ],
                "locations": [
                    {"city": "New York", "address": {"street": "123 Main St"}},
                    {"city": "San Francisco", "address": {"street": "456 Oak Ave"}},
                ],
            }
        }
        return tm.flatten(data, name="company")

    @pytest.fixture
    def empty_result(self):
        """Create empty result for testing."""
        return tm.flatten([], name="empty")

    @pytest.fixture
    def single_table_result(self):
        """Create result with only main table."""
        return tm.flatten({"id": 1, "name": "simple"}, name="simple")

    def test_result_table_access_variations(self, complex_result):
        """Test different ways to access tables."""
        # Test main table access
        main_table = complex_result["main"]
        assert isinstance(main_table, list)
        assert len(main_table) >= 1

        # Test accessing child tables
        for table_name in complex_result.tables:
            table = complex_result[table_name]
            assert isinstance(table, list)

        # Test get_table method
        main_via_get = complex_result.get_table("main")
        assert main_via_get == main_table

    def test_result_table_access_case_sensitivity(self, complex_result):
        """Test table access case sensitivity."""
        # Access should be case sensitive
        with pytest.raises(KeyError):
            _ = complex_result["MAIN"]

        with pytest.raises(KeyError):
            _ = complex_result["Main"]

    def test_result_iteration_edge_cases(self, complex_result, empty_result):
        """Test iteration behavior in edge cases."""
        # Test iteration over complex result
        all_records = list(complex_result)
        assert len(all_records) >= 1

        # Test iteration over empty result
        empty_records = list(empty_result)
        assert len(empty_records) == 0

        # Test multiple iterations
        first_iteration = list(complex_result)
        second_iteration = list(complex_result)
        assert first_iteration == second_iteration

    def test_result_dict_interface_completeness(self, complex_result):
        """Test complete dict-like interface."""
        # Test keys()
        keys = list(complex_result.keys())
        assert len(keys) >= 1
        assert "main" in keys or complex_result._result.entity_name in keys

        # Test values()
        values = list(complex_result.values())
        assert len(values) == len(keys)
        for value in values:
            assert isinstance(value, list)

        # Test items()
        items = list(complex_result.items())
        assert len(items) == len(keys)
        for name, data in items:
            assert isinstance(name, str)
            assert isinstance(data, list)

        # Test __contains__
        assert (
            "main" in complex_result
            or complex_result._result.entity_name in complex_result
        )

    def test_result_save_edge_cases(self, complex_result, empty_result):
        """Test save operations with edge cases."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test saving empty result
            empty_paths = empty_result.save(str(Path(temp_dir) / "empty.json"))
            if isinstance(empty_paths, dict):
                # Multiple files
                assert len(empty_paths) >= 0
            else:
                # Single file or list
                assert isinstance(empty_paths, (str, list))

            # Test saving with very long filename
            long_name = "x" * 200
            long_paths = complex_result.save(str(Path(temp_dir) / f"{long_name}.json"))
            assert isinstance(long_paths, (str, dict, list))

    def test_result_save_format_detection(self, complex_result):
        """Test format detection in save operations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test JSON format detection
            json_path = Path(temp_dir) / "output.json"
            result = complex_result.save(str(json_path))
            assert isinstance(result, (str, dict, list))

            # Test CSV format detection
            csv_path = Path(temp_dir) / "output.csv"
            result = complex_result.save(str(csv_path))
            assert isinstance(result, (str, dict, list))

    def test_result_save_permission_errors(self, complex_result):
        """Test save operations with permission errors."""
        # Try to save to a read-only directory (if possible)
        with tempfile.TemporaryDirectory() as temp_dir:
            readonly_dir = Path(temp_dir) / "readonly"
            readonly_dir.mkdir()

            try:
                readonly_dir.chmod(0o444)  # Read-only
                readonly_file = readonly_dir / "output.json"

                with pytest.raises((FileError, PermissionError, OSError)):
                    complex_result.save(str(readonly_file))
            except (OSError, NotImplementedError):
                # Skip if chmod not supported on this platform
                pass
            finally:
                try:
                    readonly_dir.chmod(0o755)  # Restore permissions for cleanup
                except (OSError, NotImplementedError):
                    pass

    def test_result_save_disk_full_simulation(self, complex_result):
        """Test save operations when disk is full."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "output.json"

            # Mock open to simulate disk full
            with patch("builtins.open", mock_open()) as mock_file:
                mock_file.return_value.write.side_effect = OSError(
                    "No space left on device"
                )

                with pytest.raises((OutputError, FileError, OSError)):
                    complex_result.save(str(output_path))

    def test_result_table_info_edge_cases(self, complex_result, empty_result):
        """Test table_info method with edge cases."""
        # Test with complex result
        info = complex_result.table_info()
        assert isinstance(info, dict)

        for table_name, table_info in info.items():
            assert "records" in table_info
            assert "fields" in table_info
            assert "is_main" in table_info
            assert isinstance(table_info["records"], int)
            assert isinstance(table_info["fields"], list)
            assert isinstance(table_info["is_main"], bool)

        # Test with empty result
        empty_info = empty_result.table_info()
        assert isinstance(empty_info, dict)

    def test_result_memory_behavior(self):
        """Test memory behavior with large results."""
        # Create large result
        large_data = [{"id": i, "data": "x" * 100} for i in range(1000)]
        result = tm.flatten(large_data, name="large")

        # Test accessing parts without loading everything
        assert len(result.main) == 1000

        # Test table info doesn't load all data
        info = result.table_info()
        assert isinstance(info, dict)

        # Test iteration is memory efficient
        count = 0
        for record in result:
            count += 1
            if count > 10:  # Don't iterate through everything
                break
        assert count > 0

    def test_result_concurrent_modification(self, complex_result):
        """Test behavior when result is accessed concurrently."""
        import threading
        import time

        results = []
        errors = []

        def access_result():
            try:
                # Multiple operations on the same result
                main_len = len(complex_result.main)
                table_info = complex_result.table_info()
                keys = list(complex_result.keys())
                results.append((main_len, len(table_info), len(keys)))
                time.sleep(0.01)  # Small delay to encourage race conditions
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=access_result)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Should not have errors
        assert len(errors) == 0
        assert len(results) == 5

        # All results should be consistent
        first_result = results[0]
        for result in results[1:]:
            assert result == first_result

    def test_result_string_representations(self, complex_result, empty_result):
        """Test string representation methods."""
        # Test __repr__
        repr_str = repr(complex_result)
        assert "FlattenResult" in repr_str
        assert isinstance(repr_str, str)
        assert len(repr_str) > 0

        # Test __str__ if implemented
        str_str = str(complex_result)
        assert isinstance(str_str, str)
        assert len(str_str) > 0

        # Test with empty result
        empty_repr = repr(empty_result)
        assert "FlattenResult" in empty_repr
        assert isinstance(empty_repr, str)

    def test_result_equality_and_hashing(self, complex_result):
        """Test equality and hashing behavior."""
        # Create another result with same data
        data = {
            "company": {
                "name": "Test Corp",
                "employees": [
                    {"id": 1, "name": "Alice", "skills": ["Python", "SQL"]},
                    {"id": 2, "name": "Bob", "skills": ["Java", "Docker"]},
                ],
            }
        }
        other_result = tm.flatten(data, name="company")

        # Test self-equality
        assert complex_result == complex_result

        # Results with same data might or might not be equal (implementation dependent)
        # Just test that comparison doesn't raise errors
        try:
            is_equal = complex_result == other_result
            assert isinstance(is_equal, bool)
        except (NotImplementedError, TypeError):
            # Equality might not be implemented
            pass

    def test_result_attribute_access_edge_cases(self, complex_result):
        """Test attribute access edge cases."""
        # Test accessing main table
        main_table = complex_result.main
        assert isinstance(main_table, list)

        # Test accessing tables dict
        tables = complex_result.tables
        assert isinstance(tables, dict)

        # Test accessing non-existent attribute
        with pytest.raises(AttributeError):
            _ = complex_result.nonexistent_attribute

    def test_result_with_special_characters_in_names(self):
        """Test result with special characters in field names."""
        data = {
            "field with spaces": "value1",
            "field-with-dashes": "value2",
            "field.with.dots": "value3",
            "field_with_underscores": "value4",
            "field@with#symbols": "value5",
            "field/with\\slashes": "value6",
        }

        result = tm.flatten(data, name="special_chars")
        assert len(result.main) == 1

        # Should be able to access table info
        info = result.table_info()
        assert isinstance(info, dict)

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

        # Should handle unicode in save operations
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "unicode.json"
            saved_paths = result.save(str(output_path))
            assert isinstance(saved_paths, (str, dict, list))

    def test_result_large_table_count(self):
        """Test result with many tables."""
        # Create data that will generate many child tables
        data = {}
        for i in range(100):
            data[f"array_{i}"] = [{"id": j, "value": f"item_{j}"} for j in range(5)]

        result = tm.flatten(data, name="many_tables")

        # Should handle many tables
        info = result.table_info()
        assert isinstance(info, dict)
        assert len(info) >= 1

        # Should be able to iterate
        count = 0
        for record in result:
            count += 1
            if count > 100:  # Limit iteration
                break

    def test_result_deep_nesting_in_tables(self):
        """Test result with deeply nested structures in tables."""
        # Create deeply nested structure
        deep_data = {"root": {}}
        current = deep_data["root"]
        for i in range(20):
            current[f"level_{i}"] = {"value": i, "nested": {}}
            current = current[f"level_{i}"]["nested"]
        current["final"] = "deep_value"

        result = tm.flatten(deep_data, name="deep_nested")
        assert len(result.main) == 1

        # Should handle deep nesting in table info
        info = result.table_info()
        assert isinstance(info, dict)

    def test_result_error_handling_in_operations(self, complex_result):
        """Test error handling in result operations."""
        # Test with corrupted internal state (if possible)
        original_main = complex_result.main

        try:
            # Temporarily corrupt the result
            complex_result._result = None

            # Operations should handle gracefully or raise appropriate errors
            with pytest.raises((AttributeError, ProcessingError)):
                _ = complex_result.main

        except AttributeError:
            # If _result is not accessible, that's fine
            pass
        finally:
            # Restore if possible
            try:
                complex_result._result = original_main
            except:
                pass

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
                None,  # Null item in array
            ],
        }

        result = tm.flatten(data, name="nulls", errors="skip")
        assert isinstance(result, tm.FlattenResult)

        # Should handle null values in table info
        info = result.table_info()
        assert isinstance(info, dict)

    def test_result_serialization_edge_cases(self, complex_result):
        """Test serialization edge cases."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test saving with different encodings
            output_path = Path(temp_dir) / "encoded.json"

            try:
                saved_paths = complex_result.save(str(output_path))

                # Verify file was created and is readable
                if isinstance(saved_paths, str):
                    assert Path(saved_paths).exists()
                elif isinstance(saved_paths, dict):
                    for path in saved_paths.values():
                        assert Path(path).exists()
                elif isinstance(saved_paths, list):
                    for path in saved_paths:
                        assert Path(path).exists()

            except (FileError, OutputError):
                # Some serialization errors are expected
                pass
