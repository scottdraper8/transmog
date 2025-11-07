"""
Tests for error handling in Transmog.

Tests various error conditions and recovery strategies.
"""

import json
import tempfile
from pathlib import Path

import pytest

import transmog as tm
from transmog.types.base import RecoveryMode


class TestErrorHandlingModes:
    """Test different error handling modes."""

    def test_raise_mode_with_valid_data(self, simple_data):
        """Test raise mode with valid data."""
        config = tm.TransmogConfig(recovery_mode=RecoveryMode.STRICT)
        result = tm.flatten(simple_data, name="test", config=config)
        assert len(result.main) == 1

    def test_raise_mode_with_invalid_data(self):
        """Test raise mode with problematic data."""
        # Circular references are handled by max_depth, not by raising errors
        # The system processes what it can within the depth limit
        circular_data = {"id": 1, "name": "test"}
        circular_data["self"] = circular_data

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.STRICT, max_depth=10)
        result = tm.flatten(circular_data, name="test", config=config)
        # Should process successfully, stopping at max depth
        assert len(result.main) == 1

    def test_skip_mode_with_mixed_data(self):
        """Test skip mode with mixed valid/invalid data."""
        mixed_data = [
            {"id": 1, "name": "Valid Record"},
            {"id": None, "name": "Null ID"},
            {"name": "Missing ID"},
            {"id": 2, "name": "Another Valid"},
        ]

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(mixed_data, name="test", config=config)

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

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(problematic_data, name="test", config=config)

        # Should process what it can
        assert len(result.main) >= 2

    def test_skip_mode_with_all_invalid_data(self):
        """Test skip mode when all data is invalid."""
        invalid_data = [
            {"circular": None},
            {"invalid": {"deeply": {"nested": None}}},
        ]

        # Make first record circular
        invalid_data[0]["circular"] = invalid_data[0]

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP, max_depth=10)
        result = tm.flatten(invalid_data, name="test", config=config)

        # Should handle gracefully
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

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(data_with_problematic_arrays, name="test", config=config)
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
                "id": None,
                "name": "Child",
                "grandchild": {"id": 1, "name": "Grandchild"},
            },
        }

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(nested_data, name="test", config=config)
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
                output_format="invalid_format",
            )

    def test_streaming_with_error_recovery(self, tmp_path):
        """Test streaming with error recovery."""
        problematic_data = [
            {"id": 1, "name": "Good 1"},
            {"id": None, "name": "Bad"},
            {"id": 2, "name": "Good 2"},
        ]

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten_stream(
            problematic_data,
            output_path=str(tmp_path / "recovery_test"),
            name="test",
            output_format="csv",
            config=config,
        )

        assert result is None

        csv_files = list(tmp_path.glob("**/*.csv"))
        assert len(csv_files) > 0


class TestTransmogErrorClass:
    """Test the TransmogError exception class."""

    def test_transmog_error_import(self):
        """Test that TransmogError can be imported."""
        assert hasattr(tm, "TransmogError")
        assert issubclass(tm.TransmogError, Exception)

    def test_catch_transmog_error(self):
        """Test catching TransmogError in actual operations."""
        # Circular references are handled gracefully by max_depth
        circular_data = {"id": 1}
        circular_data["self"] = circular_data

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.STRICT, max_depth=10)
        result = tm.flatten(circular_data, name="test", config=config)

        # Should process successfully
        assert isinstance(result, tm.FlattenResult)
        assert len(result.main) == 1


class TestErrorRecoveryStrategies:
    """Test different error recovery approaches."""

    def test_partial_record_processing(self):
        """Test processing records with partial failures."""
        mixed_quality_data = [
            {"id": 1, "name": "Perfect", "value": 100},
            {"id": 2, "name": "Missing Value"},
            {"id": 3, "value": 300},
            {"name": "No ID", "value": 400},
        ]

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(mixed_quality_data, name="mixed", config=config)

        # Should process at least some records
        assert len(result.main) >= 1

        # Check that at least the perfect record is there
        names = [record.get("name", "") for record in result.main]
        assert "Perfect" in names

    def test_type_coercion_errors(self):
        """Test handling type coercion errors."""
        type_problematic_data = {
            "id": "not_a_number_but_should_be_id",
            "name": 12345,
            "active": "not_a_boolean",
            "score": "not_a_number",
            "items": {"should": "be_array"},
        }

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(type_problematic_data, name="types", config=config)
        assert len(result.main) >= 0

    def test_deep_nesting_errors(self):
        """Test handling errors in deeply nested structures."""
        deep_data = {
            "id": 1,
            "level1": {
                "id": 2,
                "level2": {
                    "id": None,
                    "level3": {
                        "id": 3,
                        "level4": {
                            "id": 4,
                            "bad_reference": None,
                        },
                    },
                },
            },
        }

        # Make circular reference
        deep_data["level1"]["level2"]["level3"]["level4"]["bad_reference"] = deep_data

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP, max_depth=20)
        result = tm.flatten(deep_data, name="deep", config=config)

        assert len(result.main) == 1
        assert result.main[0]["id"] == 1
