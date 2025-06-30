"""
Tests for JSON writer functionality.

Tests JSON output, formatting, and writer interface implementation.
"""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pytest

from transmog.error import OutputError
from transmog.io.writer_interface import DataWriter
from transmog.io.writers.json import JsonWriter


class TestJsonWriter:
    """Test JSON writer implementation."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice", "age": 30, "active": True},
            {"id": 2, "name": "Bob", "age": 25, "active": False},
            {"id": 3, "name": "Charlie", "age": 35, "active": True},
        ]

    @pytest.fixture
    def complex_data(self):
        """Complex data with nested structures."""
        return [
            {
                "id": 1,
                "user": {"name": "Alice", "profile": {"age": 30, "city": "New York"}},
                "tags": ["python", "data"],
                "metadata": {"created": "2023-01-01", "updated": None},
            },
            {
                "id": 2,
                "user": {
                    "name": "Bob",
                    "profile": {"age": 25, "city": "San Francisco"},
                },
                "tags": ["javascript", "web"],
                "metadata": {"created": "2023-01-02", "updated": "2023-01-15"},
            },
        ]

    def test_json_writer_implements_interface(self):
        """Test that JsonWriter implements DataWriter interface."""
        writer = JsonWriter()
        assert isinstance(writer, DataWriter)

    def test_json_writer_basic_write(self, sample_data):
        """Test basic JSON writing functionality."""
        writer = JsonWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Write data
            writer.write(sample_data, tmp_path)

            # Verify file was created
            assert Path(tmp_path).exists()

            # Verify content
            with open(tmp_path) as f:
                written_data = json.load(f)

            assert written_data == sample_data

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_pretty_formatting(self, sample_data):
        """Test JSON writer with pretty formatting."""
        writer = JsonWriter(indent=2)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(sample_data, tmp_path)

            # Read raw content to check formatting
            with open(tmp_path) as f:
                content = f.read()

            # Should have indentation
            assert "  " in content  # 2-space indentation
            assert "\n" in content  # Newlines for formatting

            # Should still be valid JSON
            parsed_data = json.loads(content)
            assert parsed_data == sample_data

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_compact_formatting(self, sample_data):
        """Test JSON writer with compact formatting."""
        writer = JsonWriter(indent=None, separators=(",", ":"))

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(sample_data, tmp_path)

            # Read raw content
            with open(tmp_path) as f:
                content = f.read()

            # Should be compact (no extra spaces)
            assert ": " not in content  # No space after colon
            assert ", " not in content  # No space after comma

            # Should still be valid JSON
            parsed_data = json.loads(content)
            assert parsed_data == sample_data

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_complex_data(self, complex_data):
        """Test JSON writer with complex nested data."""
        writer = JsonWriter(indent=2)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(complex_data, tmp_path)

            # Verify content
            with open(tmp_path) as f:
                written_data = json.load(f)

            assert written_data == complex_data
            assert len(written_data) == 2
            assert written_data[0]["user"]["name"] == "Alice"
            assert written_data[1]["tags"] == ["javascript", "web"]

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_empty_data(self):
        """Test JSON writer with empty data."""
        writer = JsonWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write([], tmp_path)

            # Verify content
            with open(tmp_path) as f:
                written_data = json.load(f)

            assert written_data == []

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_single_record(self):
        """Test JSON writer with single record."""
        writer = JsonWriter()
        single_record = {"id": 1, "name": "Test"}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write([single_record], tmp_path)

            # Verify content
            with open(tmp_path) as f:
                written_data = json.load(f)

            assert written_data == [single_record]

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_unicode_data(self):
        """Test JSON writer with unicode data."""
        unicode_data = [
            {"name": "José", "city": "São Paulo", "emoji": "🌟"},
            {"name": "北京", "description": "Hello 世界"},
            {"name": "Москва", "note": "café naïve"},
        ]

        writer = JsonWriter(ensure_ascii=False)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(unicode_data, tmp_path)

            # Verify content
            with open(tmp_path, encoding="utf-8") as f:
                written_data = json.load(f)

            assert written_data == unicode_data
            assert written_data[0]["name"] == "José"
            assert written_data[1]["name"] == "北京"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_special_values(self):
        """Test JSON writer with special values."""
        special_data = [
            {"null_value": None, "empty_string": "", "zero": 0, "false": False},
            {"large_number": 999999999999, "float": 3.14159, "negative": -42},
        ]

        writer = JsonWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(special_data, tmp_path)

            # Verify content
            with open(tmp_path) as f:
                written_data = json.load(f)

            assert written_data == special_data
            assert written_data[0]["null_value"] is None
            assert written_data[0]["empty_string"] == ""
            assert written_data[0]["zero"] == 0
            assert written_data[0]["false"] is False

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_invalid_path(self, sample_data):
        """Test JSON writer with invalid file path."""
        writer = JsonWriter()
        invalid_path = "/invalid/path/that/does/not/exist.json"

        with pytest.raises((OutputError, OSError, FileNotFoundError)):
            writer.write(sample_data, invalid_path)

    def test_json_writer_permission_error(self, sample_data):
        """Test JSON writer with permission error."""
        writer = JsonWriter()

        # Try to write to a read-only directory
        with tempfile.TemporaryDirectory() as temp_dir:
            readonly_dir = Path(temp_dir) / "readonly"
            readonly_dir.mkdir()

            try:
                readonly_dir.chmod(0o444)  # Read-only
                readonly_file = readonly_dir / "output.json"

                with pytest.raises((OutputError, PermissionError, OSError)):
                    writer.write(sample_data, str(readonly_file))

            except (OSError, NotImplementedError):
                # Skip if chmod not supported on this platform
                pass
            finally:
                try:
                    readonly_dir.chmod(0o755)  # Restore permissions
                except (OSError, NotImplementedError):
                    pass

    def test_json_writer_large_data(self):
        """Test JSON writer with large dataset."""
        # Create large dataset
        large_data = [
            {"id": i, "value": f"item_{i}", "data": "x" * 100} for i in range(10000)
        ]

        writer = JsonWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(large_data, tmp_path)

            # Verify file was created and has correct size
            assert Path(tmp_path).exists()
            file_size = Path(tmp_path).stat().st_size
            assert file_size > 1000000  # Should be > 1MB

            # Verify first and last records
            with open(tmp_path) as f:
                written_data = json.load(f)

            assert len(written_data) == 10000
            assert written_data[0]["id"] == 0
            assert written_data[-1]["id"] == 9999

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_custom_encoder(self):
        """Test JSON writer with custom encoder."""
        from datetime import datetime

        # Data with datetime objects
        data_with_datetime = [
            {"id": 1, "created": datetime(2023, 1, 1, 12, 0, 0)},
            {"id": 2, "created": datetime(2023, 1, 2, 13, 30, 0)},
        ]

        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return super().default(obj)

        writer = JsonWriter(cls=DateTimeEncoder)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(data_with_datetime, tmp_path)

            # Verify content
            with open(tmp_path) as f:
                content = f.read()
                assert "2023-01-01T12:00:00" in content
                assert "2023-01-02T13:30:00" in content

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_file_like_object(self, sample_data):
        """Test JSON writer with file-like object."""
        import io

        writer = JsonWriter()
        output_buffer = io.StringIO()

        # Write to buffer
        writer.write(sample_data, output_buffer)

        # Get content
        content = output_buffer.getvalue()
        parsed_data = json.loads(content)

        assert parsed_data == sample_data

    def test_json_writer_append_mode(self, sample_data):
        """Test JSON writer behavior with existing files."""
        writer = JsonWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Write initial data
            writer.write(sample_data[:2], tmp_path)

            # Write again (should overwrite, not append)
            writer.write(sample_data, tmp_path)

            # Verify final content
            with open(tmp_path) as f:
                written_data = json.load(f)

            assert written_data == sample_data
            assert len(written_data) == 3  # Not 5 (2 + 3)

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_configuration_options(self, sample_data):
        """Test JSON writer with various configuration options."""
        configs = [
            {"indent": 4, "sort_keys": True},
            {"indent": None, "separators": (",", ":")},
            {"ensure_ascii": False, "indent": 2},
            {"skipkeys": True, "indent": 2},
        ]

        for config in configs:
            writer = JsonWriter(**config)

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as tmp:
                tmp_path = tmp.name

            try:
                writer.write(sample_data, tmp_path)

                # Verify file was created and is valid JSON
                assert Path(tmp_path).exists()

                with open(tmp_path) as f:
                    written_data = json.load(f)

                assert written_data == sample_data

            finally:
                Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_error_handling(self):
        """Test JSON writer error handling with non-serializable data."""
        # Data with non-serializable objects
        non_serializable_data = [
            {"id": 1, "object": object()},  # Non-serializable object
            {"id": 2, "name": "valid"},
        ]

        writer = JsonWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            with pytest.raises((OutputError, TypeError, ValueError)):
                writer.write(non_serializable_data, tmp_path)

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_memory_efficiency(self):
        """Test JSON writer memory efficiency with large data."""
        import gc

        # Create moderately large dataset
        data = [{"id": i, "data": f"item_{i}" * 100} for i in range(5000)]

        writer = JsonWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Force garbage collection before
            gc.collect()

            # Write data
            writer.write(data, tmp_path)

            # Force garbage collection after
            gc.collect()

            # Verify file was created
            assert Path(tmp_path).exists()

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_json_writer_thread_safety(self, sample_data):
        """Test JSON writer thread safety."""
        import threading
        import time

        writer = JsonWriter()
        results = []
        errors = []

        def write_in_thread(thread_id):
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=f"_{thread_id}.json", delete=False
                ) as tmp:
                    tmp_path = tmp.name

                # Modify data slightly for each thread
                thread_data = [
                    {**record, "thread_id": thread_id} for record in sample_data
                ]

                writer.write(thread_data, tmp_path)

                # Verify written data
                with open(tmp_path) as f:
                    written_data = json.load(f)

                results.append((thread_id, len(written_data)))

                # Cleanup
                Path(tmp_path).unlink(missing_ok=True)

            except Exception as e:
                errors.append((thread_id, e))

        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=write_in_thread, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Should have no errors
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 5

        # All threads should have written the same number of records
        for thread_id, record_count in results:
            assert record_count == len(sample_data)
