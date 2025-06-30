"""
Tests for CSV reading functionality.

Tests CSV file reading, parsing, and data iteration.
"""

import csv
import tempfile
from pathlib import Path

import pytest

from transmog.io.readers.csv import CSVReader


class TestCSVReader:
    """Test the CSVReader class."""

    def test_read_simple_csv_file(self, csv_file):
        """Test reading a simple CSV file."""
        reader = CSVReader()
        result = reader.read_all(csv_file)

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 3  # 3 rows in csv_file fixture

    def test_read_csv_with_headers(self, csv_file):
        """Test reading CSV with headers."""
        reader = CSVReader()
        data = reader.read_all(csv_file)

        # Should return list of dictionaries with headers as keys
        assert isinstance(data, list)
        assert isinstance(data[0], dict)
        assert "id" in data[0]
        assert "name" in data[0]
        assert "value" in data[0]
        assert "active" in data[0]

    def test_read_csv_data_types(self, csv_file):
        """Test CSV data type handling."""
        reader = CSVReader()
        data = reader.read_all(csv_file)

        # CSV data is typically read as strings
        first_row = data[0]
        assert first_row["id"] == "1"
        assert first_row["name"] == "Alice"
        assert first_row["value"] == "100"
        assert first_row["active"] == "true"

    def test_read_empty_csv_file(self):
        """Test reading empty CSV file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("")
            empty_file = f.name

        try:
            reader = CSVReader()
            from transmog.error import ParsingError

            # Empty CSV files should raise a parsing error
            with pytest.raises(ParsingError):
                reader.read_all(empty_file)
        finally:
            Path(empty_file).unlink()

    def test_read_csv_with_only_headers(self):
        """Test reading CSV with only headers."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n")
            headers_only_file = f.name

        try:
            reader = CSVReader()
            data = reader.read_all(headers_only_file)

            assert isinstance(data, list)
            assert len(data) == 0  # No data rows
        finally:
            Path(headers_only_file).unlink()

    def test_read_csv_with_missing_values(self):
        """Test reading CSV with missing values."""
        csv_content = """id,name,value,active
1,Alice,100,true
2,,200,false
3,Charlie,,true
4,Diana,400,
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            missing_file = f.name

        try:
            reader = CSVReader()
            data = reader.read_all(missing_file)

            assert len(data) == 4
            assert (
                data[1]["name"] is None or data[1]["name"] == ""
            )  # None or empty string for missing name
            assert (
                data[2]["value"] is None or data[2]["value"] == ""
            )  # None or empty string for missing value
            assert (
                data[3]["active"] is None or data[3]["active"] == ""
            )  # None or empty string for missing active
        finally:
            Path(missing_file).unlink()

    def test_read_nonexistent_file(self):
        """Test reading nonexistent file."""
        reader = CSVReader()

        from transmog.error import FileError

        with pytest.raises(FileError):
            reader.read_all("nonexistent_file.csv")


class TestCSVReaderOptions:
    """Test CSVReader with various options."""

    def test_read_with_custom_delimiter(self):
        """Test reading CSV with custom delimiter."""
        csv_content = "id|name|value\n1|Alice|100\n2|Bob|200"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            pipe_file = f.name

        try:
            reader = CSVReader(delimiter="|")
            data = reader.read_all(pipe_file)

            assert len(data) == 2
            assert data[0]["name"] == "Alice"
            assert data[1]["name"] == "Bob"
        finally:
            Path(pipe_file).unlink()

    def test_read_with_custom_quotechar(self):
        """Test reading CSV with custom quote character."""
        csv_content = (
            "id,name,description\n1,'Alice','A person'\n2,'Bob','Another person'"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            quote_file = f.name

        try:
            reader = CSVReader(quote_char="'")
            data = reader.read_all(quote_file)

            assert len(data) == 2
            assert data[0]["name"] == "Alice"
            assert data[0]["description"] == "A person"
        finally:
            Path(quote_file).unlink()

    def test_read_with_encoding(self):
        """Test reading CSV with specific encoding."""
        unicode_content = "id,name,description\n1,Alice,Café owner\n2,José,Naïve person"

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(unicode_content)
            unicode_file = f.name

        try:
            reader = CSVReader(encoding="utf-8")
            data = reader.read_all(unicode_file)

            assert len(data) == 2
            assert data[0]["description"] == "Café owner"
            assert data[1]["name"] == "José"
        finally:
            Path(unicode_file).unlink()

    def test_read_without_headers(self):
        """Test reading CSV without headers."""
        csv_content = "1,Alice,100\n2,Bob,200\n3,Charlie,300"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            no_headers_file = f.name

        try:
            reader = CSVReader(has_header=False)
            data = reader.read_all(no_headers_file)

            # Should return list of lists or use generic column names
            assert isinstance(data, list)
            assert len(data) == 3
        finally:
            Path(no_headers_file).unlink()


class TestCSVReaderIntegration:
    """Test CSVReader integration with other components."""

    def test_reader_with_transmog_flatten(self, csv_file):
        """Test using CSVReader with transmog flatten."""
        reader = CSVReader()
        data = reader.read_all(csv_file)

        # Should be able to flatten the read data
        import transmog as tm

        result = tm.flatten(data, name="from_csv_reader")

        assert len(result.main) == 3  # 3 rows from CSV
        assert result.main[0]["name"] == "Alice"

    def test_reader_with_large_csv(self):
        """Test CSVReader with large CSV files."""
        # Create a large CSV file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n")
            for i in range(10000):
                f.write(f"{i},Name_{i},{i * 10}\n")
            large_file = f.name

        try:
            reader = CSVReader()
            data = reader.read_all(large_file)

            assert isinstance(data, list)
            assert len(data) == 10000
            assert data[0]["name"] == "Name_0"
            assert data[-1]["name"] == "Name_9999"
        finally:
            Path(large_file).unlink()

    def test_reader_error_handling_integration(self):
        """Test CSVReader error handling."""
        # Create CSV with problematic content
        problematic_content = """id,name,value
1,Alice,100
2,"Bob with, comma",200
3,Charlie "with quotes",300
4,Diana,
5,,500"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(problematic_content)
            problematic_file = f.name

        try:
            reader = CSVReader()
            data = reader.read_all(problematic_file)

            # Should handle problematic content gracefully
            assert isinstance(data, list)
            assert len(data) >= 3  # Should read at least some rows
        finally:
            Path(problematic_file).unlink()


class TestCSVReaderEdgeCases:
    """Test edge cases for CSVReader."""

    def test_read_csv_with_special_characters(self):
        """Test reading CSV with special characters."""
        special_content = '''id,name,description
1,"Alice, Jr.","Person with comma"
2,"Bob ""The Builder""","Person with quotes"
3,"Charlie
Multiline","Person with newline"
4,Café Owner,Unicode characters'''

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(special_content)
            special_file = f.name

        try:
            reader = CSVReader(encoding="utf-8")
            data = reader.read_all(special_file)

            assert len(data) >= 3
            # Check handling of special cases
            assert "Alice, Jr." in data[0]["name"]
            assert "Bob" in data[1]["name"]
            assert "Café" in data[3]["name"]
        finally:
            Path(special_file).unlink()

    def test_read_csv_with_very_long_fields(self):
        """Test reading CSV with very long field values."""
        long_value = "x" * 10000  # 10KB string
        long_content = (
            f"id,name,description\n1,Alice,{long_value}\n2,Bob,Short description"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(long_content)
            long_file = f.name

        try:
            reader = CSVReader()
            data = reader.read_all(long_file)

            assert len(data) == 2
            assert len(data[0]["description"]) == 10000
            assert data[1]["description"] == "Short description"
        finally:
            Path(long_file).unlink()

    def test_read_csv_with_many_columns(self):
        """Test reading CSV with many columns."""
        # Create CSV with 100 columns
        headers = [f"col_{i}" for i in range(100)]
        header_line = ",".join(headers)

        values = [str(i) for i in range(100)]
        value_line = ",".join(values)

        many_cols_content = f"{header_line}\n{value_line}"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(many_cols_content)
            many_cols_file = f.name

        try:
            reader = CSVReader()
            data = reader.read_all(many_cols_file)

            assert len(data) == 1
            assert len(data[0]) == 100  # 100 columns
            assert data[0]["col_0"] == "0"
            assert data[0]["col_99"] == "99"
        finally:
            Path(many_cols_file).unlink()

    def test_read_malformed_csv(self):
        """Test reading malformed CSV."""
        malformed_content = """id,name,value
1,Alice,100
2,Bob,200,extra_field
3,Charlie
4,Diana,400,another_extra,and_another"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(malformed_content)
            malformed_file = f.name

        try:
            reader = CSVReader()

            # Should either handle gracefully or raise appropriate error
            try:
                data = reader.read_all(malformed_file)
                # If it succeeds, should have some data
                assert isinstance(data, list)
            except (csv.Error, ValueError):
                # CSV parsing errors are acceptable for malformed data
                pass
        finally:
            Path(malformed_file).unlink()

    def test_read_csv_with_bom(self):
        """Test reading CSV with Byte Order Mark (BOM)."""
        csv_content = "id,name,value\n1,Alice,100\n2,Bob,200"

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
            # Write BOM + content
            f.write(b"\xef\xbb\xbf")  # UTF-8 BOM
            f.write(csv_content.encode("utf-8"))
            bom_file = f.name

        try:
            reader = CSVReader(encoding="utf-8-sig")  # Handles BOM
            data = reader.read_all(bom_file)

            assert len(data) == 2
            assert data[0]["name"] == "Alice"
            # First column name should not have BOM characters
            assert "id" in data[0]
        finally:
            Path(bom_file).unlink()
