"""
Tests for Parquet writer in Transmog.

Tests Parquet file writing functionality, formats, and edge cases.
"""

import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from transmog.exceptions import OutputError
from transmog.writers import ParquetWriter


class TestParquetWriter:
    """Test the ParquetWriter class."""

    def test_parquet_writer_basic(self, tmp_path):
        """Test basic Parquet writing functionality."""
        data = [
            {"id": "1", "name": "Alice", "age": "25"},
            {"id": "2", "name": "Bob", "age": "30"},
            {"id": "3", "name": "Charlie", "age": "35"},
        ]

        output_file = tmp_path / "output.parquet"
        writer = ParquetWriter()
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 3
        assert set(table.schema.names) == {"id", "name", "age"}
        assert table.column("name").to_pylist() == ["Alice", "Bob", "Charlie"]

    def test_parquet_writer_empty_data(self, tmp_path):
        """Test writing empty data to Parquet produces no file."""
        output_file = tmp_path / "empty.parquet"
        writer = ParquetWriter()
        writer.write([], str(output_file))

        # Empty data produces no output file
        assert not output_file.exists()

    def test_parquet_writer_mixed_types(self, tmp_path):
        """Test writing mixed data types to Parquet."""
        data = [
            {"id": 1, "name": "Alice", "score": 95.5, "active": True},
            {"id": 2, "name": "Bob", "score": 87.2, "active": False},
        ]

        output_file = tmp_path / "mixed.parquet"
        writer = ParquetWriter()
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 2
        assert table.column("name").to_pylist() == ["Alice", "Bob"]

    def test_parquet_writer_unicode_data(self, tmp_path):
        """Test writing Unicode data to Parquet preserves characters."""
        data = [
            {"id": "1", "name": "José", "city": "São Paulo"},
            {"id": "2", "name": "张三", "city": "北京"},
        ]

        output_file = tmp_path / "unicode.parquet"
        writer = ParquetWriter()
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 2
        assert table.column("name").to_pylist() == ["José", "张三"]
        assert table.column("city").to_pylist() == ["São Paulo", "北京"]

    def test_parquet_writer_large_dataset(self, tmp_path):
        """Test writing large dataset to Parquet."""
        data = [
            {"id": str(i), "name": f"User_{i}", "email": f"user{i}@example.com"}
            for i in range(1000)
        ]

        output_file = tmp_path / "large.parquet"
        writer = ParquetWriter()
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 1000
        assert table.column("id").to_pylist()[0] == "0"
        assert table.column("id").to_pylist()[-1] == "999"

    def test_parquet_writer_sparse_data(self, tmp_path):
        """Test writing sparse data (missing fields) fills nulls."""
        data = [
            {"id": "1", "name": "Alice", "email": "alice@example.com"},
            {"id": "2", "name": "Bob"},
            {"id": "3", "email": "charlie@example.com"},
        ]

        output_file = tmp_path / "sparse.parquet"
        writer = ParquetWriter()
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 3
        assert set(table.schema.names) == {"id", "name", "email"}
        # Missing fields should be null
        names = table.column("name").to_pylist()
        assert names[0] == "Alice"
        assert names[2] is None

    def test_parquet_writer_null_values(self, tmp_path):
        """Test writing null values to Parquet."""
        data = [
            {"id": "1", "name": "Alice", "optional_field": "value1"},
            {"id": "2", "name": "Bob", "optional_field": None},
            {"id": "3", "name": "Charlie", "optional_field": ""},
        ]

        output_file = tmp_path / "nulls.parquet"
        writer = ParquetWriter()
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 3
        values = table.column("optional_field").to_pylist()
        assert values[0] == "value1"
        assert values[1] is None
        assert values[2] == ""


class TestParquetWriterOptions:
    """Test ParquetWriter with various options."""

    @pytest.mark.parametrize("compression", ["snappy", "gzip", "brotli", None])
    def test_parquet_writer_compression(self, tmp_path, compression):
        """Test Parquet writer with different compression options."""
        data = [
            {"id": "1", "name": "Alice", "data": "x" * 100},
            {"id": "2", "name": "Bob", "data": "y" * 100},
            {"id": "3", "name": "Charlie", "data": "z" * 100},
        ]

        output_file = tmp_path / f"compressed_{compression}.parquet"
        writer = ParquetWriter(compression=compression)
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 3
        assert table.column("name").to_pylist() == ["Alice", "Bob", "Charlie"]


class TestParquetWriterErrorHandling:
    """Test ParquetWriter error handling."""

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows allows creation of paths like /nonexistent/directory",
    )
    def test_parquet_writer_invalid_path(self):
        """Test writing to invalid path."""
        data = [{"id": "1", "name": "Alice"}]
        invalid_path = "/nonexistent/directory/file.parquet"

        writer = ParquetWriter()
        with pytest.raises(OutputError):
            writer.write(data, invalid_path)

    def test_parquet_writer_invalid_data_type(self, tmp_path):
        """Test writing invalid data types."""
        invalid_data = "not a list"
        output_file = tmp_path / "invalid.parquet"

        writer = ParquetWriter()
        with pytest.raises((OutputError, TypeError, ValueError, AttributeError)):
            writer.write(invalid_data, str(output_file))

    def test_parquet_writer_complex_nested_data(self, tmp_path):
        """Test writing complex nested data serializes dicts/lists to JSON strings."""
        data = [
            {"id": "1", "name": "Alice", "nested": {"key": "value"}, "array": [1, 2, 3]}
        ]

        output_file = tmp_path / "nested.parquet"
        writer = ParquetWriter()
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 1
        assert "id" in table.schema.names
        assert "name" in table.schema.names


class TestParquetWriterIntegration:
    """Test ParquetWriter integration with other components."""

    def test_parquet_writer_with_transmog_result(self, tmp_path):
        """Test ParquetWriter with transmog flatten result."""
        import transmog as tm

        test_data = {
            "id": 1,
            "name": "Test Company",
            "employees": [
                {"id": 1, "name": "Alice", "role": "Developer"},
                {"id": 2, "name": "Bob", "role": "Designer"},
            ],
        }

        result = tm.flatten(test_data, name="company")
        output_path = tmp_path / "output"
        paths = result.save(str(output_path), output_format="parquet")

        assert isinstance(paths, dict)
        for _table_name, path in paths.items():
            assert Path(path).exists()
            assert Path(path).suffix == ".parquet"
            table = pq.read_table(path)
            assert table.num_rows > 0


class TestParquetWriterEdgeCases:
    """Test edge cases for ParquetWriter."""

    def test_parquet_writer_many_columns(self, tmp_path):
        """Test writing data with many columns."""
        data = [{f"col_{i}": f"value_{i}_{j}" for i in range(100)} for j in range(10)]

        output_file = tmp_path / "wide.parquet"
        writer = ParquetWriter()
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 10
        assert len(table.schema.names) == 100

    def test_parquet_writer_special_characters_in_data(self, tmp_path):
        """Test writing data with special characters are preserved."""
        data = [
            {"id": "1", "text": "Line 1\nLine 2\tTabbed"},
            {"id": "2", "text": 'Quote: "Hello"'},
            {"id": "3", "text": "Unicode: 🚀 emoji"},
        ]

        output_file = tmp_path / "special.parquet"
        writer = ParquetWriter()
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 3
        texts = table.column("text").to_pylist()
        assert texts[0] == "Line 1\nLine 2\tTabbed"
        assert texts[1] == 'Quote: "Hello"'
        assert texts[2] == "Unicode: 🚀 emoji"
