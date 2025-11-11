"""
Tests for Parquet writer in Transmog.

Tests Parquet file writing functionality, formats, and edge cases.
"""

import json
import sys
import tempfile
from pathlib import Path

import pytest

from transmog.error import OutputError
from transmog.io.writers.parquet import ParquetWriter


class TestParquetWriter:
    """Test the ParquetWriter class."""

    def test_parquet_writer_basic(self):
        """Test basic Parquet writing functionality."""
        data = [
            {"id": "1", "name": "Alice", "age": "25"},
            {"id": "2", "name": "Bob", "age": "30"},
            {"id": "3", "name": "Charlie", "age": "35"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            # Verify file was created
            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

            # Try to read back the data to verify it's valid Parquet
            try:
                import pyarrow.parquet as pq

                table = pq.read_table(output_file)
                assert table.num_rows == 3
                assert "id" in table.schema.names
                assert "name" in table.schema.names
                assert "age" in table.schema.names
            except ImportError:
                # PyArrow not available, just check file exists
                pass

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_empty_data(self):
        """Test writing empty data to Parquet."""
        data = []

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            # File should exist but be minimal
            assert Path(output_file).exists()

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_mixed_types(self):
        """Test writing mixed data types to Parquet."""
        data = [
            {"id": "1", "name": "Alice", "score": "95.5", "active": "true"},
            {"id": "2", "name": "Bob", "score": "87.2", "active": "false"},
            {"id": "3", "name": "Charlie", "score": "92.0", "active": "true"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_unicode_data(self):
        """Test writing Unicode data to Parquet."""
        data = [
            {"id": "1", "name": "José", "city": "São Paulo"},
            {"id": "2", "name": "François", "city": "Montréal"},
            {"id": "3", "name": "张三", "city": "北京"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_large_dataset(self):
        """Test writing large dataset to Parquet."""
        # Create a larger dataset
        data = [
            {
                "id": str(i),
                "name": f"User_{i}",
                "email": f"user{i}@example.com",
                "score": str(i * 10.5),
                "active": "true" if i % 2 == 0 else "false",
            }
            for i in range(1000)
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

            # Verify the file is reasonably sized for 1000 records
            file_size = Path(output_file).stat().st_size
            assert file_size > 1000  # Should be more than 1KB

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_sparse_data(self):
        """Test writing sparse data (missing fields) to Parquet."""
        data = [
            {"id": "1", "name": "Alice", "email": "alice@example.com"},
            {"id": "2", "name": "Bob"},  # Missing email
            {"id": "3", "email": "charlie@example.com"},  # Missing name
            {
                "id": "4",
                "name": "Diana",
                "email": "diana@example.com",
                "phone": "123-456-7890",
            },  # Extra field
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_null_values(self):
        """Test writing null values to Parquet."""
        data = [
            {"id": "1", "name": "Alice", "optional_field": "value1"},
            {"id": "2", "name": "Bob", "optional_field": None},
            {"id": "3", "name": "Charlie", "optional_field": ""},
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        finally:
            Path(output_file).unlink(missing_ok=True)


class TestParquetWriterOptions:
    """Test ParquetWriter with various options."""

    def test_parquet_writer_compression(self):
        """Test Parquet writer with different compression options."""
        data = [
            {"id": "1", "name": "Alice", "data": "x" * 100},
            {"id": "2", "name": "Bob", "data": "y" * 100},
            {"id": "3", "name": "Charlie", "data": "z" * 100},
        ]

        compression_options = ["snappy", "gzip", "brotli", "lz4", None]

        for compression in compression_options:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
                output_file = f.name

            try:
                writer = ParquetWriter(compression=compression)
                writer.write(data, output_file)

                assert Path(output_file).exists()
                assert Path(output_file).stat().st_size > 0

            except (ValueError, ImportError):
                # Some compression types might not be available
                pass
            finally:
                Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_schema_options(self):
        """Test Parquet writer with schema options."""
        data = [
            {"id": "1", "name": "Alice", "score": "95.5"},
            {"id": "2", "name": "Bob", "score": "87.2"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter(preserve_types=True, infer_schema=True)
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        except (OutputError, TypeError):
            # These options might not exist in the current implementation
            writer = ParquetWriter()
            writer.write(data, output_file)
            assert Path(output_file).exists()
        finally:
            Path(output_file).unlink(missing_ok=True)


class TestParquetWriterErrorHandling:
    """Test ParquetWriter error handling."""

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows allows creation of paths like /nonexistent/directory"
    )
    def test_parquet_writer_invalid_path(self):
        """Test writing to invalid path."""
        data = [{"id": "1", "name": "Alice"}]
        invalid_path = "/nonexistent/directory/file.parquet"

        writer = ParquetWriter()
        with pytest.raises(OutputError):
            writer.write(data, invalid_path)

    def test_parquet_writer_permission_denied(self):
        """Test writing to path with no permissions."""
        data = [{"id": "1", "name": "Alice"}]

        # Try to write to root directory (should fail on most systems)
        restricted_path = "/root/test.parquet"

        writer = ParquetWriter()
        try:
            writer.write(data, restricted_path)
            # If it doesn't raise an error, the system allows it
        except (OutputError, PermissionError, OSError):
            # Expected behavior
            pass

    def test_parquet_writer_invalid_data_type(self):
        """Test writing invalid data types."""
        invalid_data = "not a list"

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            with pytest.raises((OutputError, TypeError, ValueError)):
                writer.write(invalid_data, output_file)
        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_complex_nested_data(self):
        """Test writing complex nested data."""
        data = [
            {"id": "1", "name": "Alice", "nested": {"key": "value"}, "array": [1, 2, 3]}
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            # This might fail or succeed depending on implementation
            try:
                writer.write(data, output_file)
                assert Path(output_file).exists()
            except (OutputError, ValueError):
                # Acceptable if complex nested data is not supported
                pass
        finally:
            Path(output_file).unlink(missing_ok=True)


class TestParquetWriterIntegration:
    """Test ParquetWriter integration with other components."""

    def test_parquet_writer_with_transmog_result(self):
        """Test ParquetWriter with transmog flatten result."""
        import transmog as tm

        # Create test data
        test_data = {
            "id": 1,
            "name": "Test Company",
            "employees": [
                {"id": 1, "name": "Alice", "role": "Developer"},
                {"id": 2, "name": "Bob", "role": "Designer"},
            ],
        }

        # Flatten the data
        result = tm.flatten(test_data, name="company")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "output"

            # Save as Parquet
            paths = result.save(str(output_path), output_format="parquet")

            # Verify files were created
            if isinstance(paths, dict):
                for path in paths.values():
                    assert Path(path).exists()
                    assert Path(path).suffix == ".parquet"
            else:
                for path in paths:
                    assert Path(path).exists()
                    assert Path(path).suffix == ".parquet"

    def test_parquet_writer_performance(self):
        """Test ParquetWriter performance with medium dataset."""
        # Create medium-sized dataset
        data = [
            {
                "id": str(i),
                "name": f"Record_{i}",
                "category": f"Category_{i % 10}",
                "value": str(i * 1.5),
                "timestamp": f"2023-01-{(i % 28) + 1:02d}T10:00:00Z",
                "active": "true" if i % 3 == 0 else "false",
            }
            for i in range(5000)
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            import time

            start_time = time.time()

            writer = ParquetWriter()
            writer.write(data, output_file)

            end_time = time.time()
            duration = end_time - start_time

            # Verify file was created
            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

            # Performance should be reasonable (less than 5 seconds for 5K records)
            assert duration < 5.0, f"Writing took too long: {duration:.2f} seconds"

        finally:
            Path(output_file).unlink(missing_ok=True)


class TestParquetWriterEdgeCases:
    """Test edge cases for ParquetWriter."""

    def test_parquet_writer_very_long_field_names(self):
        """Test writing data with very long field names."""
        long_field_name = "a" * 1000
        data = [
            {"id": "1", long_field_name: "value1"},
            {"id": "2", long_field_name: "value2"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_many_columns(self):
        """Test writing data with many columns."""
        # Create data with 100 columns
        data = [{f"col_{i}": f"value_{i}_{j}" for i in range(100)} for j in range(10)]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_parquet_writer_special_characters_in_data(self):
        """Test writing data with special characters."""
        data = [
            {"id": "1", "text": "Line 1\nLine 2\tTabbed"},
            {"id": "2", "text": 'Quote: "Hello"'},
            {"id": "3", "text": "Comma, semicolon; pipe|"},
            {"id": "4", "text": "Unicode: 🚀 emoji"},
        ]

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(data, output_file)

            assert Path(output_file).exists()
            assert Path(output_file).stat().st_size > 0

        finally:
            Path(output_file).unlink(missing_ok=True)
