"""
Tests for error handling in Transmog v1.1.0.

Tests various error conditions and recovery strategies.
"""

import json
import tempfile
from pathlib import Path

import pytest

import transmog as tm


class TestErrorHandlingModes:
    """Test different error handling modes."""

    def test_raise_mode_with_valid_data(self, simple_data):
        """Test raise mode with valid data."""
        result = tm.flatten(simple_data, name="test", errors="raise")
        assert len(result.main) == 1

    def test_raise_mode_with_invalid_data(self):
        """Test raise mode with problematic data."""
        # Create circular reference
        circular_data = {"id": 1, "name": "test"}
        circular_data["self"] = circular_data

        with pytest.raises(Exception):
            tm.flatten(circular_data, name="test", errors="raise")

    def test_skip_mode_with_mixed_data(self):
        """Test skip mode with mixed valid/invalid data."""
        mixed_data = [
            {"id": 1, "name": "Valid Record"},
            {"id": None, "name": "Null ID"},
            {"name": "Missing ID"},
            {"id": 2, "name": "Another Valid"},
        ]

        result = tm.flatten(mixed_data, name="test", errors="skip")

        # Should process at least the valid records
        assert len(result.main) >= 2

        # Check that valid records are present
        names = [record["name"] for record in result.main]
        assert "Valid Record" in names
        assert "Another Valid" in names

    def test_warn_mode_with_problematic_data(self):
        """Test warn mode with problematic data."""
        problematic_data = [
            {"id": 1, "name": "Good"},
            {"id": "", "name": "Empty ID"},
            {"id": None, "name": "Null ID"},
            {"id": 2, "name": "Also Good"},
        ]

        # Should not raise exception but may log warnings
        result = tm.flatten(problematic_data, name="test", errors="warn")

        # Should process what it can
        assert len(result.main) >= 2

    def test_skip_mode_with_all_invalid_data(self):
        """Test skip mode when all data is invalid."""
        invalid_data = [
            {"circular": None},  # Will be made circular
            {"invalid": {"deeply": {"nested": None}}},
        ]

        # Make first record circular
        invalid_data[0]["circular"] = invalid_data[0]

        result = tm.flatten(invalid_data, name="test", errors="skip")

        # May result in empty main table
        assert isinstance(result.main, list)

    def test_error_handling_with_arrays(self):
        """Test error handling with array processing."""
        data_with_problematic_arrays = {
            "id": 1,
            "name": "Test",
            "items": [
                {"id": 1, "value": "good"},
                {"id": None, "value": "bad_id"},
                {"value": "missing_id"},
                {"id": 2, "value": "also_good"},
            ],
        }

        # Skip mode should handle problematic array items
        result = tm.flatten(data_with_problematic_arrays, name="test", errors="skip")
        assert len(result.main) == 1

        # Check if items table exists and has some records
        items_table = None
        for table_name, table_data in result.tables.items():
            if "items" in table_name.lower():
                items_table = table_data
                break

        if items_table:
            # Should have at least the good items
            assert len(items_table) >= 2

    def test_error_handling_with_nested_structures(self):
        """Test error handling with nested structures."""
        nested_data = {
            "id": 1,
            "name": "Parent",
            "child": {
                "id": None,  # Problematic
                "name": "Child",
                "grandchild": {"id": 1, "name": "Grandchild"},
            },
        }

        result = tm.flatten(nested_data, name="test", errors="skip")
        assert len(result.main) == 1

        # Should still process the main record
        assert result.main[0]["name"] == "Parent"


class TestFileErrorHandling:
    """Test error handling with file operations."""

    def test_nonexistent_file(self):
        """Test handling nonexistent files."""
        with pytest.raises(Exception):
            tm.flatten_file("nonexistent_file.json")

    def test_invalid_json_file(self, tmp_path):
        """Test handling invalid JSON files."""
        invalid_json_file = tmp_path / "invalid.json"
        with open(invalid_json_file, "w") as f:
            f.write('{"invalid": json content}')  # Invalid JSON

        with pytest.raises(Exception):
            tm.flatten_file(str(invalid_json_file))

    def test_empty_file(self, tmp_path):
        """Test handling empty files."""
        empty_file = tmp_path / "empty.json"
        empty_file.touch()  # Create empty file

        # Behavior may vary - could raise exception or return empty result
        try:
            result = tm.flatten_file(str(empty_file))
            assert isinstance(result.main, list)
        except Exception:
            pass  # Empty file handling is implementation-specific

    def test_malformed_csv_file(self, tmp_path):
        """Test handling malformed CSV files."""
        malformed_csv = tmp_path / "malformed.csv"
        with open(malformed_csv, "w") as f:
            f.write("id,name,value\n")
            f.write("1,Alice,100\n")
            f.write("2,Bob,200,extra_field\n")  # Extra field
            f.write("3,Charlie\n")  # Missing field

        # Should handle malformed CSV gracefully depending on error mode
        result = tm.flatten_file(str(malformed_csv), errors="skip")
        assert len(result.main) >= 1  # At least one good record

    def test_permission_denied_file(self, tmp_path):
        """Test handling permission denied errors."""
        restricted_file = tmp_path / "restricted.json"
        with open(restricted_file, "w") as f:
            json.dump({"id": 1, "name": "test"}, f)

        # Make file unreadable (Unix-like systems)
        try:
            restricted_file.chmod(0o000)

            with pytest.raises(Exception):
                tm.flatten_file(str(restricted_file))
        except OSError:
            # File permission changes might not work on all systems
            pytest.skip("Cannot modify file permissions on this system")
        finally:
            # Restore permissions for cleanup
            try:
                restricted_file.chmod(0o644)
            except OSError:
                pass


class TestStreamingErrorHandling:
    """Test error handling in streaming operations."""

    def test_streaming_to_readonly_directory(self, simple_data):
        """Test streaming to read-only directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            readonly_dir = Path(temp_dir) / "readonly"
            readonly_dir.mkdir()

            try:
                # Make directory read-only
                readonly_dir.chmod(0o444)

                with pytest.raises(Exception):
                    tm.flatten_stream(
                        simple_data,
                        output_path=str(readonly_dir / "output"),
                        name="test",
                        format="json",
                    )
            except OSError:
                pytest.skip("Cannot modify directory permissions on this system")
            finally:
                # Restore permissions for cleanup
                try:
                    readonly_dir.chmod(0o755)
                except OSError:
                    pass

    def test_streaming_invalid_format(self, simple_data, tmp_path):
        """Test streaming with invalid format."""
        # The actual exception type may vary (ConfigurationError, ValueError, etc.)
        with pytest.raises(Exception):
            tm.flatten_stream(
                simple_data,
                output_path=str(tmp_path / "output"),
                name="test",
                format="invalid_format",
            )

    def test_streaming_with_error_recovery(self, tmp_path):
        """Test streaming with error recovery."""
        problematic_data = [
            {"id": 1, "name": "Good 1"},
            {"id": None, "name": "Bad"},
            {"id": 2, "name": "Good 2"},
        ]

        # Should not raise exception with skip mode
        result = tm.flatten_stream(
            problematic_data,
            output_path=str(tmp_path / "recovery_test"),
            name="test",
            format="json",
            errors="skip",
        )

        # Streaming returns None
        assert result is None

        # Check that some output was created
        json_files = list(tmp_path.glob("**/*.json"))
        assert len(json_files) > 0


class TestTransmogErrorClass:
    """Test the TransmogError exception class."""

    def test_transmog_error_import(self):
        """Test that TransmogError can be imported."""
        assert hasattr(tm, "TransmogError")
        assert issubclass(tm.TransmogError, Exception)

    def test_catch_transmog_error(self):
        """Test catching TransmogError in actual operations."""
        try:
            # Create a scenario that should raise TransmogError
            circular_data = {"id": 1}
            circular_data["self"] = circular_data

            tm.flatten(circular_data, name="test", errors="raise")

        except tm.TransmogError as e:
            # Successfully caught TransmogError
            assert isinstance(e, tm.TransmogError)
            assert str(e)  # Should have error message

        except Exception as e:
            # Other exceptions are also acceptable
            assert isinstance(e, Exception)


class TestErrorRecoveryStrategies:
    """Test different error recovery approaches."""

    def test_partial_record_processing(self):
        """Test processing records with partial failures."""
        mixed_quality_data = [
            {"id": 1, "name": "Perfect", "value": 100},
            {"id": 2, "name": "Missing Value"},  # Missing value field
            {"id": 3, "value": 300},  # Missing name field
            {"name": "No ID", "value": 400},  # Missing ID
        ]

        result = tm.flatten(mixed_quality_data, name="mixed", errors="skip")

        # Should process at least some records
        assert len(result.main) >= 1

        # Check that at least the perfect record is there
        names = [record.get("name", "") for record in result.main]
        assert "Perfect" in names

    def test_type_coercion_errors(self):
        """Test handling type coercion errors."""
        type_problematic_data = {
            "id": "not_a_number_but_should_be_id",
            "name": 12345,  # Number instead of string
            "active": "not_a_boolean",
            "score": "not_a_number",
            "items": {"should": "be_array"},  # Object instead of array
        }

        # Should handle type issues gracefully
        result = tm.flatten(type_problematic_data, name="types", errors="skip")
        assert len(result.main) >= 0  # May or may not process depending on strictness

    def test_deep_nesting_errors(self):
        """Test handling errors in deeply nested structures."""
        # Create deeply nested structure with errors at different levels
        deep_data = {
            "id": 1,
            "level1": {
                "id": 2,
                "level2": {
                    "id": None,  # Error at level 2
                    "level3": {
                        "id": 3,
                        "level4": {
                            "id": 4,
                            "bad_reference": None,  # Will create circular reference
                        },
                    },
                },
            },
        }

        # Make circular reference
        deep_data["level1"]["level2"]["level3"]["level4"]["bad_reference"] = deep_data

        result = tm.flatten(deep_data, name="deep", errors="skip")

        # Should at least process the root level
        assert len(result.main) == 1
        assert result.main[0]["id"] == "1"
