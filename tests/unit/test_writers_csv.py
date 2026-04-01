"""
Tests for CSV writer functionality.

Tests CSV output, formatting, and writer interface implementation.
"""

import csv
import sys
import tempfile
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from transmog.exceptions import ConfigurationError, OutputError
from transmog.writers import CsvWriter, DataWriter
from transmog.writers.csv import CsvStreamingWriter, _sanitize_csv_value


class TestCsvWriter:
    """Test CSV writer implementation."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": "1", "name": "Alice", "age": "30", "active": "True"},
            {"id": "2", "name": "Bob", "age": "25", "active": "False"},
            {"id": "3", "name": "Charlie", "age": "35", "active": "True"},
        ]

    @pytest.fixture
    def mixed_data(self):
        """Mixed data types for testing."""
        return [
            {"id": 1, "name": "Alice", "score": 95.5, "active": True},
            {"id": 2, "name": "Bob", "score": 87.2, "active": False},
            {"id": 3, "name": "Charlie", "score": 92.0, "active": True},
        ]

    @pytest.fixture
    def sparse_data(self):
        """Sparse data with missing fields."""
        return [
            {"id": "1", "name": "Alice", "email": "alice@example.com"},
            {"id": "2", "name": "Bob", "phone": "555-1234"},
            {"id": "3", "email": "charlie@example.com", "phone": "555-5678"},
        ]

    def test_csv_writer_implements_interface(self):
        """Test that CsvWriter implements DataWriter interface."""
        writer = CsvWriter()
        assert isinstance(writer, DataWriter)

    def test_csv_writer_basic_write(self, sample_data):
        """Test basic CSV writing functionality."""
        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Write data
            writer.write(sample_data, tmp_path)

            # Verify file was created
            assert Path(tmp_path).exists()

            # Verify content
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            assert len(written_data) == len(sample_data)
            assert written_data[0]["name"] == "Alice"
            assert written_data[1]["name"] == "Bob"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_header_generation(self, sample_data):
        """Test CSV header generation."""
        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(sample_data, tmp_path)

            # Read raw content to check header
            with open(tmp_path) as f:
                lines = f.readlines()

            # First line should be header
            header_line = lines[0].strip()
            expected_fields = ["id", "name", "age", "active"]

            for field in expected_fields:
                assert field in header_line

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_mixed_types(self, mixed_data):
        """Test CSV writer with mixed data types."""
        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(mixed_data, tmp_path)

            # Verify content
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            assert len(written_data) == len(mixed_data)
            # CSV data should be strings
            assert written_data[0]["id"] == "1"
            assert written_data[0]["score"] == "95.5"
            assert written_data[0]["active"] == "True"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_sparse_data(self, sparse_data):
        """Test CSV writer with sparse data (missing fields)."""
        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(sparse_data, tmp_path)

            # Verify content
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            assert len(written_data) == len(sparse_data)

            # Check that all fields are present in header
            fieldnames = reader.fieldnames
            expected_fields = {"id", "name", "email", "phone"}
            assert set(fieldnames) == expected_fields

            # Missing fields should be empty strings
            assert written_data[0]["phone"] == ""
            assert written_data[1]["email"] == ""
            assert written_data[2]["name"] == ""

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_empty_data(self):
        """Test CSV writer with empty data."""
        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write([], tmp_path)

            # Verify file was created but is empty or has only header
            assert Path(tmp_path).exists()

            with open(tmp_path) as f:
                content = f.read().strip()

            # Should be empty or just contain empty header
            assert len(content) == 0 or content == ""

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_custom_delimiter(self, sample_data):
        """Test CSV writer with custom delimiter."""
        writer = CsvWriter(delimiter=";")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(sample_data, tmp_path)

            # Read raw content to check delimiter
            with open(tmp_path) as f:
                content = f.read()

            # Should use semicolon delimiter
            assert ";" in content
            assert content.count(";") > content.count(",")

            # Verify content can be read back
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f, delimiter=";")
                written_data = list(reader)

            assert len(written_data) == len(sample_data)

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_custom_quotechar(self, sample_data):
        """Test CSV writer with custom quote character."""
        writer = CsvWriter(quotechar="'")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(sample_data, tmp_path)

            # Verify content can be read back
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f, quotechar="'")
                written_data = list(reader)

            assert len(written_data) == len(sample_data)
            assert written_data[0]["name"] == "Alice"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_special_characters(self):
        """Test CSV writer with special characters."""
        special_data = [
            {"id": "1", "name": "Alice, Bob", "note": "Has comma"},
            {"id": "2", "name": "Charlie", "note": 'Has "quotes"'},
            {"id": "3", "name": "Dave\nMultiline", "note": "Has\nnewlines"},
            {"id": "4", "name": "Eve", "note": "Has\ttabs"},
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(special_data, tmp_path)

            # Verify content can be read back correctly
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            assert len(written_data) == len(special_data)
            assert written_data[0]["name"] == "Alice, Bob"
            assert written_data[1]["note"] == 'Has "quotes"'
            assert written_data[2]["name"] == "Dave\nMultiline"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_unicode_data(self):
        """Test CSV writer with unicode data."""
        unicode_data = [
            {"id": "1", "name": "José", "city": "São Paulo", "emoji": "🌟"},
            {"id": "2", "name": "北京", "description": "Hello 世界"},
            {"id": "3", "name": "Москва", "note": "café naïve"},
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(unicode_data, tmp_path)

            # Verify content
            with open(tmp_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            assert len(written_data) == len(unicode_data)
            assert written_data[0]["name"] == "José"
            assert written_data[1]["name"] == "北京"
            assert written_data[2]["name"] == "Москва"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_null_values(self):
        """Test CSV writer with null/None values."""
        null_data = [
            {"id": "1", "name": "Alice", "email": None, "phone": ""},
            {"id": "2", "name": None, "email": "bob@example.com", "phone": None},
            {"id": None, "name": "Charlie", "email": "", "phone": "555-1234"},
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(null_data, tmp_path)

            # Verify content
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            assert len(written_data) == len(null_data)
            # None values should be converted to empty strings
            assert written_data[0]["email"] == ""
            assert written_data[1]["name"] == ""
            assert written_data[2]["id"] == ""

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    @pytest.mark.parametrize("size", [5000, 10000], ids=["5k", "10k"])
    def test_csv_writer_large_dataset_integrity(self, size):
        """Test CSV writer preserves data integrity with large datasets."""
        data = [{"id": str(i), "value": f"item_{i}"} for i in range(size)]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(data, tmp_path)

            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            assert len(written_data) == size
            assert written_data[0]["id"] == "0"
            assert written_data[-1]["id"] == str(size - 1)

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_field_order(self):
        """Test that CSV writer maintains consistent field order."""
        data = [
            {"name": "Alice", "id": 1, "active": True, "age": 30},
            {"name": "Bob", "id": 2, "active": False, "age": 25},
            {"name": "Charlie", "id": 3, "active": True, "age": 35},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            writer = CsvWriter()
            writer.write(data, tmp.name)
            tmp_path = tmp.name

        try:
            with open(tmp_path, encoding="utf-8") as f:
                content = f.read()
                lines = content.strip().split("\n")
                header_line = lines[0]

                # All field names present in header
                header_fields = set(header_line.split(","))
                assert header_fields == {"active", "age", "id", "name"}

                # Check data integrity
                assert "Alice" in content
                assert "Bob" in content
                assert "Charlie" in content
        finally:
            Path(tmp_path).unlink()

    def test_csv_writer_no_header(self, sample_data):
        """Test CSV writer without header row."""
        writer = CsvWriter(include_header=False)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(sample_data, tmp_path)

            # Check that first line is data, not header
            with open(tmp_path) as f:
                first_line = f.readline().strip()

            # Should be data values, not field names
            assert "1" in first_line  # ID value
            assert "Alice" in first_line  # Name value
            assert "id" not in first_line  # Not field name

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows allows creation of paths like /invalid/path",
    )
    def test_csv_writer_invalid_path(self, sample_data):
        """Test CSV writer with invalid file path."""
        writer = CsvWriter()
        invalid_path = "/invalid/path/that/does/not/exist.csv"

        with pytest.raises(OutputError):
            writer.write(sample_data, invalid_path)

    def test_csv_writer_file_like_object(self):
        """Test CSV writer with file-like object."""
        data = [
            {"id": 1, "name": "Alice", "age": 30, "active": True},
            {"id": 2, "name": "Bob", "age": 25, "active": False},
            {"id": 3, "name": "Charlie", "age": 35, "active": True},
        ]

        output_buffer = StringIO()
        writer = CsvWriter()
        writer.write(data, output_buffer)

        content = output_buffer.getvalue()

        # All field names present in header
        header_line = content.split("\n")[0].strip().split("\r")[0]
        header_fields = set(header_line.split(","))
        assert header_fields == {"active", "age", "id", "name"}
        assert "Alice" in content
        assert "Bob" in content
        assert "Charlie" in content

    def test_csv_writer_append_mode(self, sample_data):
        """Test CSV writer behavior with existing files."""
        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Write initial data
            writer.write(sample_data[:2], tmp_path)

            # Write again (should overwrite, not append)
            writer.write(sample_data, tmp_path)

            # Verify final content
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            assert len(written_data) == 3  # Not 5 (2 + 3)

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    @pytest.mark.parametrize(
        "quoting",
        [csv.QUOTE_MINIMAL, csv.QUOTE_ALL, csv.QUOTE_NONNUMERIC],
    )
    def test_csv_writer_quoting_options(self, sample_data, quoting):
        """Test CSV writer with different quoting options."""
        writer = CsvWriter(quoting=quoting)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(sample_data, tmp_path)

            assert Path(tmp_path).exists()

            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f, quoting=quoting)
                written_data = list(reader)

            assert len(written_data) == len(sample_data)
            assert written_data[0]["name"] == "Alice"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_escapechar(self):
        """Test CSV writer with escape character."""
        escape_data = [
            {"id": "1", "name": "Alice", "note": "simple text"},
            {"id": "2", "name": "Bob", "note": "also simple"},
        ]

        writer = CsvWriter(quoting=csv.QUOTE_NONE, escapechar="\\")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(escape_data, tmp_path)

            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f, quoting=csv.QUOTE_NONE, escapechar="\\")
                written_data = list(reader)

            assert len(written_data) == 2
            assert written_data[0]["name"] == "Alice"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_thread_safety(self, sample_data):
        """Test CSV writer thread safety with data integrity checks."""
        import threading

        writer = CsvWriter()
        results = []
        errors = []

        def write_in_thread(thread_id):
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=f"_{thread_id}.csv", delete=False
                ) as tmp:
                    tmp_path = tmp.name

                thread_data = [
                    {**record, "thread_id": str(thread_id)} for record in sample_data
                ]

                writer.write(thread_data, tmp_path)

                with open(tmp_path, newline="") as f:
                    reader = csv.DictReader(f)
                    written_data = list(reader)

                results.append((thread_id, written_data))

                Path(tmp_path).unlink(missing_ok=True)

            except Exception as e:
                errors.append((thread_id, e))

        threads = []
        for i in range(3):
            thread = threading.Thread(target=write_in_thread, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 3

        for thread_id, written_data in results:
            assert len(written_data) == len(sample_data)
            # Verify each thread's data contains the correct thread_id
            assert all(row["thread_id"] == str(thread_id) for row in written_data)

    def test_csv_injection_prevention_formulas(self):
        """Test CSV injection prevention for formula injection attacks."""
        injection_data = [
            {"id": "1", "name": "Alice", "note": "=1+1"},
            {"id": "2", "name": "Bob", "note": "+2+2"},
            {"id": "3", "name": "Charlie", "note": "-3-3"},
            {"id": "4", "name": "Dave", "note": "@SUM(A1:A10)"},
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(injection_data, tmp_path)

            # Read back and verify sanitization
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            # All dangerous values should be prefixed with single quote
            assert written_data[0]["note"] == "'=1+1"
            assert written_data[1]["note"] == "'+2+2"
            assert written_data[2]["note"] == "'-3-3"
            assert written_data[3]["note"] == "'@SUM(A1:A10)"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_injection_prevention_commands(self):
        """Test CSV injection prevention for command injection attacks."""
        injection_data = [
            {"id": "1", "cmd": "|calc"},
            {"id": "2", "cmd": "\t/usr/bin/whoami"},
            {"id": "3", "cmd": "\rmalicious"},
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(injection_data, tmp_path)

            # Read back and verify sanitization
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            # All dangerous values should be prefixed with single quote
            assert written_data[0]["cmd"] == "'|calc"
            assert written_data[1]["cmd"] == "'\t/usr/bin/whoami"
            assert written_data[2]["cmd"] == "'\rmalicious"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_injection_prevention_safe_values(self):
        """Test that safe values are not modified by injection prevention."""
        safe_data = [
            {"id": "1", "name": "Alice", "note": "Normal text"},
            {"id": "2", "name": "Bob", "note": "123456"},
            {"id": "3", "name": "Charlie", "note": "test@example.com"},
            {"id": "4", "name": "Dave", "note": ""},
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(safe_data, tmp_path)

            # Read back and verify no changes
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            # Safe values should not be modified
            assert written_data[0]["note"] == "Normal text"
            assert written_data[1]["note"] == "123456"
            assert written_data[2]["note"] == "test@example.com"
            assert written_data[3]["note"] == ""

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_injection_prevention_mixed_types(self):
        """Test CSV injection prevention with mixed data types."""
        mixed_data = [
            {"id": 1, "formula": "=1+1", "number": 42, "safe": "text"},
            {"id": 2, "formula": "+A1", "number": 3.14, "safe": None},
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(mixed_data, tmp_path)

            # Read back and verify sanitization
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            # String formulas should be sanitized
            assert written_data[0]["formula"] == "'=1+1"
            assert written_data[1]["formula"] == "'+A1"
            # Numbers should remain unchanged
            assert written_data[0]["number"] == "42"
            assert written_data[1]["number"] == "3.14"
            # Safe text should remain unchanged
            assert written_data[0]["safe"] == "text"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_injection_prevention_complex_formulas(self):
        """Test CSV injection prevention for complex formula attacks."""
        complex_data = [
            {"attack": "=cmd|'/c calc'!A0"},
            {"attack": '=HYPERLINK("http://evil.com","click")'},
            {"attack": "@cmd|'/c notepad'"},
            {"attack": '+1+1+IMPORTXML("http://evil.com")'},
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(complex_data, tmp_path)

            # Read back and verify all are sanitized
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            # All should be prefixed with single quote
            for row in written_data:
                assert row["attack"].startswith("'")

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_injection_prevention_whitespace_bypass(self):
        """Test CSV injection prevention for whitespace bypass attacks."""
        bypass_data = [
            {"payload": " =cmd"},
            {"payload": "  =1+1"},
            {"payload": " +malicious"},
            {"payload": "  @SUM(A1)"},
            {"payload": " -formula"},
            {"payload": "  |command"},
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(bypass_data, tmp_path)

            # Read back and verify sanitization
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            # All should be prefixed with single quote (including leading spaces)
            assert written_data[0]["payload"] == "' =cmd"
            assert written_data[1]["payload"] == "'  =1+1"
            assert written_data[2]["payload"] == "' +malicious"
            assert written_data[3]["payload"] == "'  @SUM(A1)"
            assert written_data[4]["payload"] == "' -formula"
            assert written_data[5]["payload"] == "'  |command"

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    @pytest.mark.parametrize(
        "char",
        ["=", "+", "-", "@", "|", "\t", "\r"],
        ids=["equals", "plus", "minus", "at", "pipe", "tab", "cr"],
    )
    def test_sanitize_csv_value_dangerous_chars_parametrized(self, char: str):
        """Each dangerous leading character triggers single-quote prefix."""
        payload = f"{char}payload"
        result = _sanitize_csv_value(payload)
        assert result == f"'{char}payload"

    @pytest.mark.parametrize(
        ("label", "value"),
        [
            ("fullwidth_equals", "\uff1d1+1"),
            ("fullwidth_plus", "\uff0b1"),
            ("unicode_minus", "\u2212value"),
            ("zwsp_prefix_equals", "\u200b=1+1"),
            ("bom_prefix_equals", "\ufeff=1+1"),
        ],
        ids=lambda v: v if isinstance(v, str) and len(v) < 30 else None,
    )
    def test_sanitize_csv_value_unicode_bypass_not_caught(self, label: str, value: str):
        """Fullwidth and zero-width Unicode bypasses are not currently sanitized."""
        result = _sanitize_csv_value(value)
        assert result == value, f"{label}: expected value to pass through unsanitized"

    @pytest.mark.parametrize(
        ("label", "value"),
        [
            ("nbsp_equals", "\u00a0=cmd"),
            ("em_space_plus", "\u2003+attack"),
            ("ideographic_space_at", "\u3000@SUM(A1)"),
            ("newline_minus", "\n-formula"),
        ],
        ids=lambda v: v if isinstance(v, str) and len(v) < 30 else None,
    )
    def test_sanitize_csv_value_unicode_whitespace_caught(self, label: str, value: str):
        """Unicode whitespace recognized by str.lstrip() is caught."""
        result = _sanitize_csv_value(value)
        assert result.startswith("'"), (
            f"{label}: expected sanitization for whitespace-prefixed dangerous char"
        )

    @pytest.mark.parametrize(
        "value",
        [
            "hello=world",
            "user@domain.com",
            "a|b",
            "mid-word",
            "col\there",
            "line\rhere",
        ],
        ids=[
            "embedded_equals",
            "embedded_at",
            "embedded_pipe",
            "embedded_minus",
            "embedded_tab",
            "embedded_cr",
        ],
    )
    def test_sanitize_csv_value_embedded_dangerous_chars_safe(self, value: str):
        """Dangerous characters in non-leading positions must not trigger sanitization."""
        result = _sanitize_csv_value(value)
        assert result == value

    @pytest.mark.parametrize(
        "value",
        [42, 3.14, True, None, [1, 2], {"a": 1}],
        ids=["int", "float", "bool", "none", "list", "dict"],
    )
    def test_sanitize_csv_value_non_string_passthrough(self, value: Any):
        """Non-string types pass through unchanged via identity check."""
        result = _sanitize_csv_value(value)
        assert result is value

    def test_csv_writer_compression_raises_configuration_error(self):
        """Test that compression option raises ConfigurationError, not OutputError."""
        from transmog.exceptions import ConfigurationError

        writer = CsvWriter()
        data = [{"id": "1", "name": "Alice"}]

        with pytest.raises(ConfigurationError):
            writer.write(data, "output.csv", compression="gzip")

    def test_csv_writer_memory_error_propagates(self, sample_data):
        """Test that MemoryError is not caught by the writer."""
        from unittest.mock import patch

        writer = CsvWriter()

        with patch("builtins.open", side_effect=MemoryError("out of memory")):
            with pytest.raises(MemoryError):
                writer.write(sample_data, "/tmp/test.csv")

    def test_csv_writer_oserror_wrapped_in_output_error(self, sample_data):
        """Test that OSError is wrapped in OutputError."""
        from unittest.mock import patch

        writer = CsvWriter()

        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(OutputError, match="Failed to write CSV file"):
                writer.write(sample_data, "/tmp/test.csv")


class TestCsvStreamingWriterBasics:
    """Test basic CsvStreamingWriter functionality with part files."""

    def test_write_produces_file(self, tmp_path):
        """Writing records and closing produces a consolidated file."""
        dest = str(tmp_path)

        with CsvStreamingWriter(
            destination=dest, entity_name="test", batch_size=100
        ) as writer:
            writer.write_main_records([{"id": "1", "name": "Alice"}])

        expected = tmp_path / "test.csv"
        assert expected.exists()

        with open(expected, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_close_returns_paths(self, tmp_path):
        """close() returns a list of Path objects for written files."""
        dest = str(tmp_path)

        writer = CsvStreamingWriter(
            destination=dest, entity_name="test", batch_size=100
        )
        writer.write_main_records([{"id": "1", "name": "Alice"}])
        paths = writer.close()

        assert len(paths) == 1
        assert paths[0] == Path(tmp_path / "test.csv")

    def test_multiple_batches_produce_multiple_parts(self, tmp_path):
        """Exceeding batch_size causes multiple part files when consolidate=False."""
        dest = str(tmp_path)

        with CsvStreamingWriter(
            destination=dest,
            entity_name="test",
            batch_size=2,
            consolidate=False,
        ) as writer:
            writer.write_main_records([{"id": "1"}, {"id": "2"}])
            writer.write_main_records([{"id": "3"}])

        part_0 = tmp_path / "test_part_0000.csv"
        part_1 = tmp_path / "test_part_0001.csv"

        assert part_0.exists()
        assert part_1.exists()

        with open(part_0, newline="") as f:
            rows_0 = list(csv.DictReader(f))
        with open(part_1, newline="") as f:
            rows_1 = list(csv.DictReader(f))

        assert len(rows_0) == 2
        assert len(rows_1) == 1

    def test_multiple_batches_consolidated(self, tmp_path):
        """Multiple batches produce a single consolidated file by default."""
        dest = str(tmp_path)

        with CsvStreamingWriter(
            destination=dest,
            entity_name="test",
            batch_size=2,
        ) as writer:
            writer.write_main_records([{"id": "1"}, {"id": "2"}])
            writer.write_main_records([{"id": "3"}])

        consolidated = tmp_path / "test.csv"
        assert consolidated.exists()

        with open(consolidated, newline="") as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 3

    def test_empty_write_produces_no_files(self, tmp_path):
        """Writing empty records does not produce any part files."""
        dest = str(tmp_path)

        with CsvStreamingWriter(
            destination=dest, entity_name="test", batch_size=100
        ) as writer:
            writer.write_main_records([])

        csv_files = list(tmp_path.glob("*.csv"))
        assert len(csv_files) == 0

    def test_custom_delimiter(self, tmp_path):
        """Output files use the configured delimiter."""
        dest = str(tmp_path)

        with CsvStreamingWriter(
            destination=dest, entity_name="test", delimiter=";", batch_size=100
        ) as writer:
            writer.write_main_records([{"id": "1", "name": "Alice"}])

        output_file = tmp_path / "test.csv"
        content = output_file.read_text()

        assert ";" in content
        assert content.count(";") > content.count(",")

    def test_no_header(self, tmp_path):
        """Output files omit headers when include_header is False."""
        dest = str(tmp_path)

        with CsvStreamingWriter(
            destination=dest, entity_name="test", include_header=False, batch_size=100
        ) as writer:
            writer.write_main_records([{"id": "1", "name": "Alice"}])

        output_file = tmp_path / "test.csv"
        content = output_file.read_text().strip()
        lines = content.split("\n")

        # Only one line (data), no header
        assert len(lines) == 1
        assert "id" not in lines[0] or "1" in lines[0]

    def test_child_records_produce_separate_files(self, tmp_path):
        """Child records are written to files named after the child table."""
        dest = str(tmp_path)

        with CsvStreamingWriter(
            destination=dest, entity_name="parent", batch_size=100
        ) as writer:
            writer.write_main_records([{"id": "1", "name": "Alice"}])
            writer.write_child_records("addresses", [{"parent_id": "1", "city": "NYC"}])

        main_file = tmp_path / "parent.csv"
        child_file = tmp_path / "addresses.csv"

        assert main_file.exists()
        assert child_file.exists()

        with open(child_file, newline="") as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 1
        assert rows[0]["city"] == "NYC"

    def test_file_like_object_raises_configuration_error(self):
        """Passing a file-like object as destination raises ConfigurationError."""
        buffer = StringIO()

        with pytest.raises(ConfigurationError, match="directory path"):
            CsvStreamingWriter(destination=buffer, entity_name="test")

    def test_writer_has_expected_attributes(self, tmp_path):
        """Writer exposes buffers, part_counts, base_schemas, schema_log, all_part_paths."""
        dest = str(tmp_path)
        writer = CsvStreamingWriter(destination=dest, entity_name="test")

        assert hasattr(writer, "buffers")
        assert hasattr(writer, "part_counts")
        assert hasattr(writer, "base_schemas")
        assert hasattr(writer, "schema_log")
        assert hasattr(writer, "all_part_paths")

        assert isinstance(writer.buffers, dict)
        assert isinstance(writer.part_counts, dict)
        assert isinstance(writer.base_schemas, dict)
        assert isinstance(writer.schema_log, dict)
        assert isinstance(writer.all_part_paths, list)

        writer.close()

    def test_context_manager_basic(self, tmp_path):
        """Context manager writes and closes cleanly."""
        dest = str(tmp_path)

        with CsvStreamingWriter(
            destination=dest, entity_name="test", batch_size=100
        ) as writer:
            writer.write_main_records(
                [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
            )

        assert writer._closed is True

        output_file = tmp_path / "test.csv"
        assert output_file.exists()

        with open(output_file, newline="") as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 2


class TestCsvStreamingWriterExceptionCleanup:
    """Test resource cleanup behavior of CsvStreamingWriter on exceptions."""

    def test_context_manager_closes_on_write_exception(self, tmp_path):
        """Context manager calls close() when writerows raises mid-batch."""
        dest = str(tmp_path)

        call_count = 0
        original_writerows = csv.DictWriter.writerows

        def failing_writerows(self_writer, rowdicts):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise OSError("simulated disk failure")
            return original_writerows(self_writer, rowdicts)

        with patch.object(csv.DictWriter, "writerows", failing_writerows):
            with pytest.raises(OSError, match="simulated disk failure"):
                with CsvStreamingWriter(
                    destination=dest,
                    entity_name="test",
                    batch_size=2,
                    consolidate=False,
                ) as writer:
                    # First batch flushes (call_count=1, succeeds)
                    writer.write_main_records(
                        [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
                    )
                    # Second batch flushes (call_count=2, fails)
                    writer.write_main_records(
                        [{"id": "3", "name": "Charlie"}, {"id": "4", "name": "Dave"}]
                    )

        # First part file was written before the failure
        assert (tmp_path / "test_part_0000.csv").exists()

    def test_close_after_exception_is_idempotent(self, tmp_path):
        """Calling close() after context manager exit is safe and idempotent."""
        dest = str(tmp_path)

        with CsvStreamingWriter(
            destination=dest, entity_name="test", batch_size=100
        ) as writer:
            writer.write_main_records([{"id": "1", "name": "Alice"}])

        assert writer._closed is True

        # Second close is a no-op
        writer.close()
        assert writer._closed is True

    def test_close_without_context_manager(self, tmp_path):
        """Explicit close() works without context manager."""
        dest = str(tmp_path)

        writer = CsvStreamingWriter(
            destination=dest, entity_name="test", batch_size=100
        )
        writer.write_main_records([{"id": "1", "name": "Alice"}])

        assert not getattr(writer, "_closed", False)

        paths = writer.close()
        assert writer._closed is True
        assert len(paths) == 1

        # Idempotent: second close returns empty list
        paths2 = writer.close()
        assert paths2 == []

    def test_compression_raises_configuration_error(self):
        """Passing compression option raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="compression"):
            CsvStreamingWriter(
                destination="/tmp/test", entity_name="test", compression="gzip"
            )


class TestCsvCoercion:
    """Test schema coercion for CSV part files."""

    def test_coercion_unifies_csv_columns(self, tmp_path):
        """Test that coerce_schema rewrites minority CSV part files."""
        import json
        import warnings

        dest = str(tmp_path)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with CsvStreamingWriter(
                destination=dest,
                entity_name="test",
                batch_size=2,
                coerce_schema=True,
                consolidate=False,
            ) as writer:
                writer.write_main_records(
                    [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
                )
                writer.write_main_records(
                    [
                        {"id": "3", "name": "Charlie", "extra": "val"},
                        {"id": "4", "name": "Dave", "extra": "val2"},
                    ]
                )

        # Part 0 should have been rewritten with the "extra" column
        with open(tmp_path / "test_part_0000.csv", newline="") as f:
            rows0 = list(csv.DictReader(f))
        with open(tmp_path / "test_part_0001.csv", newline="") as f:
            rows1 = list(csv.DictReader(f))

        assert len(rows0) == 2
        assert len(rows1) == 2
        assert "extra" in rows0[0]
        assert "extra" in rows1[0]
        # Coerced column should be empty string in part 0
        assert rows0[0]["extra"] == ""
        assert rows1[0]["extra"] == "val"

        log = json.loads((tmp_path / "_schema_log.json").read_text())
        assert "coerced_to" in log["tables"]["main"]["parts"][0]
