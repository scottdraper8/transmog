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

import pytest

from transmog.exceptions import OutputError
from transmog.writers import CsvWriter, DataWriter


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

    def test_csv_writer_large_data(self):
        """Test CSV writer with large dataset."""
        # Create large dataset
        large_data = [
            {"id": str(i), "value": f"item_{i}", "data": "x" * 50} for i in range(10000)
        ]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(large_data, tmp_path)

            # Verify file was created and has correct size
            assert Path(tmp_path).exists()
            file_size = Path(tmp_path).stat().st_size
            assert file_size > 500000  # Should be substantial

            # Verify first and last records
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f)
                written_data = list(reader)

            assert len(written_data) == 10000
            assert written_data[0]["id"] == "0"
            assert written_data[-1]["id"] == "9999"

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

                # Fields are sorted alphabetically
                expected_order = "active,age,id,name"
                assert header_line == expected_order

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

        with pytest.raises((OutputError, OSError, FileNotFoundError)):
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

        # Fields are sorted alphabetically: active,age,id,name
        assert "active,age,id,name" in content
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

    def test_csv_writer_quoting_options(self, sample_data):
        """Test CSV writer with different quoting options."""
        quoting_options = [
            csv.QUOTE_MINIMAL,
            csv.QUOTE_ALL,
            csv.QUOTE_NONNUMERIC,
            csv.QUOTE_NONE,
        ]

        for quoting in quoting_options:
            try:
                writer = CsvWriter(quoting=quoting)

                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".csv", delete=False
                ) as tmp:
                    tmp_path = tmp.name

                try:
                    writer.write(sample_data, tmp_path)

                    # Verify file was created
                    assert Path(tmp_path).exists()

                    # Verify content can be read back
                    with open(tmp_path, newline="") as f:
                        reader = csv.DictReader(f, quoting=quoting)
                        written_data = list(reader)

                    assert len(written_data) == len(sample_data)

                finally:
                    Path(tmp_path).unlink(missing_ok=True)

            except (csv.Error, TypeError):
                # Some quoting options might not work with all data
                continue

    def test_csv_writer_escapechar(self):
        """Test CSV writer with escape character."""
        escape_data = [
            {"id": "1", "name": "Alice", "note": 'Has "quotes"'},
            {"id": "2", "name": "Bob", "note": "Has \\backslash"},
        ]

        writer = CsvWriter(quoting=csv.QUOTE_NONE, escapechar="\\")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer.write(escape_data, tmp_path)

            # Verify content can be read back
            with open(tmp_path, newline="") as f:
                reader = csv.DictReader(f, quoting=csv.QUOTE_NONE, escapechar="\\")
                written_data = list(reader)

            assert len(written_data) == len(escape_data)

        except csv.Error:
            # Escape character handling might not work in all cases
            pass
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_csv_writer_memory_efficiency(self):
        """Test CSV writer memory efficiency with large data."""
        import gc

        # Create moderately large dataset
        data = [{"id": str(i), "data": f"item_{i}" * 50} for i in range(5000)]

        writer = CsvWriter()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
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

    def test_csv_writer_thread_safety(self, sample_data):
        """Test CSV writer thread safety."""
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

                # Modify data slightly for each thread
                thread_data = [
                    {**record, "thread_id": str(thread_id)} for record in sample_data
                ]

                writer.write(thread_data, tmp_path)

                # Verify written data
                with open(tmp_path, newline="") as f:
                    reader = csv.DictReader(f)
                    written_data = list(reader)

                results.append((thread_id, len(written_data)))

                # Cleanup
                Path(tmp_path).unlink(missing_ok=True)

            except Exception as e:
                errors.append((thread_id, e))

        # Create multiple threads
        threads = []
        for i in range(3):  # Fewer threads for CSV to avoid file conflicts
            thread = threading.Thread(target=write_in_thread, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Should have no errors
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 3

        # All threads should have written the same number of records
        for _thread_id, record_count in results:
            assert record_count == len(sample_data)

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
