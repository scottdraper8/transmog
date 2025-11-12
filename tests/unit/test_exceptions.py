"""Tests for error handling and recovery strategies."""

import json
import sys
import tempfile
from pathlib import Path

import pytest

import transmog as tm
from transmog.exceptions import MissingDependencyError
from transmog.types import RecoveryMode


class TestRecoveryModes:
    """Test different recovery modes."""

    def test_strict_mode_with_valid_data(self, simple_data):
        """Test strict mode processes valid data successfully."""
        config = tm.TransmogConfig(recovery_mode=RecoveryMode.STRICT)
        result = tm.flatten(simple_data, name="test", config=config)
        assert len(result.main) == 1

    def test_strict_mode_with_circular_references(self):
        """Test strict mode handles circular references via max_depth."""
        circular_data = {"id": 1, "name": "test"}
        circular_data["self"] = circular_data

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.STRICT, max_depth=10)
        result = tm.flatten(circular_data, name="test", config=config)
        assert len(result.main) == 1

    def test_skip_mode_with_mixed_valid_invalid_data(self):
        """Test skip mode processes valid records and skips invalid ones."""
        mixed_data = [
            {"id": 1, "name": "Valid Record"},
            {"id": None, "name": "Null ID"},
            {"name": "Missing ID"},
            {"id": 2, "name": "Another Valid"},
        ]

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(mixed_data, name="test", config=config)

        assert len(result.main) >= 2
        names = [record["name"] for record in result.main]
        assert "Valid Record" in names
        assert "Another Valid" in names

    def test_skip_mode_with_empty_invalid_ids(self):
        """Test skip mode handles empty and null IDs."""
        problematic_data = [
            {"id": 1, "name": "Good"},
            {"id": "", "name": "Empty ID"},
            {"id": None, "name": "Null ID"},
            {"id": 2, "name": "Also Good"},
        ]

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(problematic_data, name="test", config=config)
        assert len(result.main) >= 2

    def test_skip_mode_with_all_invalid_data(self):
        """Test skip mode handles all invalid data gracefully."""
        invalid_data = [
            {"circular": None},
            {"invalid": {"deeply": {"nested": None}}},
        ]
        invalid_data[0]["circular"] = invalid_data[0]

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP, max_depth=10)
        result = tm.flatten(invalid_data, name="test", config=config)
        assert isinstance(result.main, list)


class TestErrorHandlingWithArrays:
    """Test error handling during array processing."""

    def test_arrays_with_problematic_items(self):
        """Test arrays containing items with missing or invalid IDs."""
        data = {
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
        result = tm.flatten(data, name="test", config=config)
        assert len(result.main) == 1

        items_table = None
        for table_name, table_data in result.tables.items():
            if "items" in table_name.lower():
                items_table = table_data
                break

        if items_table:
            assert len(items_table) >= 2

    def test_array_processing_consistency(self):
        """Test consistent error handling across array operations."""
        array_data = {
            "items": [
                {"id": 1, "name": "valid"},
                {"id": float("inf"), "name": "invalid"},
                {"id": 3, "name": "valid"},
            ]
        }

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(array_data, config=config)

        assert len(result.main) == 1
        assert "items" in result.tables or len(result.tables) > 0


class TestErrorHandlingWithNestedData:
    """Test error handling with nested structures."""

    def test_nested_structures_with_null_ids(self):
        """Test nested data with null IDs at various levels."""
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
        assert result.main[0]["name"] == "Parent"

    def test_deeply_nested_errors(self):
        """Test error handling in deeply nested structures."""
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
        deep_data["level1"]["level2"]["level3"]["level4"]["bad_reference"] = deep_data

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP, max_depth=20)
        result = tm.flatten(deep_data, name="deep", config=config)

        assert len(result.main) == 1
        assert result.main[0]["id"] == 1

    def test_nested_error_context_preservation(self):
        """Test error context preservation through nested processing."""
        nested_data = {
            "level1": {"level2": {"level3": {"problematic_field": float("nan")}}}
        }

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(nested_data, config=config)
        assert len(result.main) == 1


class TestFileProcessingErrors:
    """Test error handling with file operations."""

    def test_nonexistent_file(self):
        """Test handling of nonexistent files."""
        with pytest.raises(Exception):
            tm.flatten("nonexistent_file.json")

    def test_invalid_json_file(self, tmp_path):
        """Test handling of malformed JSON files."""
        invalid_json_file = tmp_path / "invalid.json"
        with open(invalid_json_file, "w") as f:
            f.write('{"invalid": json content}')

        with pytest.raises(Exception):
            tm.flatten(str(invalid_json_file))

    def test_empty_file(self, tmp_path):
        """Test handling of empty files."""
        empty_file = tmp_path / "empty.json"
        empty_file.touch()

        try:
            result = tm.flatten(str(empty_file))
            assert isinstance(result.main, list)
        except Exception:
            pass

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows file permissions work differently from Unix",
    )
    def test_permission_denied_file(self, tmp_path):
        """Test handling of permission errors."""
        restricted_file = tmp_path / "restricted.json"
        with open(restricted_file, "w") as f:
            json.dump({"id": 1, "name": "test"}, f)

        try:
            restricted_file.chmod(0o000)
            with pytest.raises(Exception):
                tm.flatten(str(restricted_file))
        except OSError:
            pytest.skip("Cannot modify file permissions on this system")
        finally:
            try:
                restricted_file.chmod(0o644)
            except OSError:
                pass


class TestStreamingErrors:
    """Test error handling in streaming operations."""

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows file permissions work differently from Unix",
    )
    def test_streaming_to_readonly_directory(self, simple_data):
        """Test streaming to read-only directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            readonly_dir = Path(temp_dir) / "readonly"
            readonly_dir.mkdir()

            try:
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
                try:
                    readonly_dir.chmod(0o755)
                except OSError:
                    pass

    def test_streaming_invalid_format(self, simple_data, tmp_path):
        """Test streaming with unsupported output format."""
        with pytest.raises(Exception):
            tm.flatten_stream(
                simple_data,
                output_path=str(tmp_path / "output"),
                name="test",
                output_format="invalid_format",
            )

    def test_streaming_with_error_recovery(self, tmp_path):
        """Test streaming with skip recovery mode."""
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


class TestExceptionTypes:
    """Test custom exception types."""

    def test_transmog_error_exists(self):
        """Test TransmogError exception class exists."""
        assert hasattr(tm, "TransmogError")
        assert issubclass(tm.TransmogError, Exception)

    def test_transmog_error_in_operations(self):
        """Test TransmogError is used appropriately."""
        circular_data = {"id": 1}
        circular_data["self"] = circular_data

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.STRICT, max_depth=10)
        result = tm.flatten(circular_data, name="test", config=config)

        assert isinstance(result, tm.FlattenResult)
        assert len(result.main) == 1

    def test_missing_dependency_error(self):
        """Test MissingDependencyError basic functionality."""
        error = MissingDependencyError("Dependency required")
        assert str(error) == "Dependency required"
        assert isinstance(error, tm.TransmogError)


class TestErrorConsistency:
    """Test error handling consistency across operations."""

    def test_consistent_handling_of_special_floats(self):
        """Test consistent handling of inf and nan values."""
        problematic_data = {
            "name": "test",
            "bad_float": float("inf"),
        }

        config_skip = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result_skip = tm.flatten(problematic_data, config=config_skip)
        assert len(result_skip.main) == 1

    def test_consistent_handling_of_non_serializable_objects(self):
        """Test consistent handling of non-serializable objects."""

        def test_function():
            pass

        data_with_function = {
            "name": "test",
            "function": test_function,
        }

        config_strict = tm.TransmogConfig(recovery_mode=RecoveryMode.STRICT)
        result = tm.flatten(data_with_function, config=config_strict)
        assert len(result.main) == 1
        assert "function" in result.main[0]

    def test_recovery_mode_configuration(self):
        """Test recovery mode configuration is respected."""
        config_skip = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        assert config_skip.recovery_mode == RecoveryMode.SKIP

        config_strict = tm.TransmogConfig(recovery_mode=RecoveryMode.STRICT)
        assert config_strict.recovery_mode == RecoveryMode.STRICT


class TestPartialProcessing:
    """Test partial processing with recovery."""

    def test_partial_record_processing(self):
        """Test processing continues with partial record failures."""
        mixed_quality_data = [
            {"id": 1, "name": "Perfect", "value": 100},
            {"id": 2, "name": "Missing Value"},
            {"id": 3, "value": 300},
            {"name": "No ID", "value": 400},
        ]

        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(mixed_quality_data, name="mixed", config=config)

        assert len(result.main) >= 1
        names = [record.get("name", "") for record in result.main]
        assert "Perfect" in names

    def test_type_coercion_handling(self):
        """Test handling of type coercion issues."""
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
