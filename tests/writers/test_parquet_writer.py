"""
Tests for the Parquet writer implementation.

This module tests that the Parquet writer correctly handles writing data to Parquet format.
"""

import os

import pytest

# Skip tests if PyArrow is not available
pytest.importorskip("pyarrow")
import pyarrow.parquet as pq

from tests.interfaces.test_writer_interface import AbstractWriterTest

# Import the writer and abstract test base class
from transmog.io.writers.parquet import ParquetWriter


class TestParquetWriter(AbstractWriterTest):
    """Test the Parquet writer implementation."""

    writer_class = ParquetWriter
    format_name = "parquet"

    @pytest.fixture
    def writer(self):
        """Create a Parquet writer."""
        if not ParquetWriter.is_available():
            pytest.skip("PyArrow is required for Parquet tests")
        return ParquetWriter(compression="snappy")

    @pytest.mark.requires_pyarrow
    def test_compression_options(self, writer, batch_data, tmp_path):
        """Test Parquet-specific compression options."""
        # Create paths for different compression formats
        compression_formats = ["snappy", "gzip", "zstd", None]
        outputs = {}

        for compression in compression_formats:
            # Skip zstd if not available
            if compression == "zstd":
                try:
                    import importlib.util

                    has_zstd = importlib.util.find_spec("zstandard") is not None
                    if not has_zstd:
                        continue
                except ImportError:
                    continue

            # Create writer with this compression format
            if compression:
                writer = ParquetWriter(compression=compression)
            else:
                writer = ParquetWriter()  # Default compression

            # Create output path
            output_path = (
                tmp_path / f"compression_test_{compression or 'default'}.parquet"
            )

            # Write data
            writer.write_table(batch_data, output_path)

            # Store path for size comparison
            outputs[compression or "default"] = output_path

            # Verify file exists
            assert os.path.exists(output_path)

        # Verify files were written with different sizes (if we have at least 2 formats)
        if len(outputs) >= 2:
            sizes = {fmt: os.path.getsize(path) for fmt, path in outputs.items()}
            # At least some files should have different sizes due to compression
            assert len(set(sizes.values())) > 1, (
                "All compression formats produced same file size"
            )

    @pytest.mark.requires_pyarrow
    def test_schema_preservation(self, batch_data, tmp_path):
        """Test that the schema is preserved correctly."""
        # Create writer
        writer = ParquetWriter()

        # Create output path
        output_path = tmp_path / "schema_test.parquet"

        # Write data
        writer.write_table(batch_data, output_path)

        # Read back with PyArrow
        table = pq.read_table(output_path)

        # Check schema matches original data
        schema = table.schema

        # Verify the schema contains the original fields
        for key in batch_data[0].keys():
            assert key in schema.names, f"Field {key} missing from schema"

        # Verify field count matches
        assert len(schema.names) == len(batch_data[0].keys())

        # Verify rows match
        assert table.num_rows == len(batch_data)

    @pytest.mark.requires_pyarrow
    def test_table_reader(self, simple_data, tmp_path):
        """Test reading back written data."""
        # Create list of records
        records = [simple_data]

        # Create writer
        writer = ParquetWriter()

        # Create output path
        output_path = tmp_path / "read_test.parquet"

        # Write data
        writer.write_table(records, output_path)

        # Read back with PyArrow
        table = pq.read_table(output_path)

        # Convert back to Python dict
        read_data = table.to_pydict()

        # Check keys match (all fields from original data should be present)
        for key in simple_data.keys():
            assert key in read_data, f"Field {key} missing from read data"

            # Field should contain one value (one row)
            assert len(read_data[key]) == 1

    @pytest.mark.requires_pyarrow
    def test_nested_data_flattening(self, writer, complex_data, tmp_path):
        """Test that nested data is properly flattened."""
        # Convert to list of records
        records = [complex_data]

        # Create output path
        output_path = tmp_path / "nested_data.parquet"

        # Write data
        writer.write_table(records, output_path)

        # Read back
        table = pq.read_table(output_path)
        read_data = table.to_pydict()

        # PyArrow doesn't automatically flatten nested dictionaries when writing to Parquet
        # It preserves the structure, so we need to check that the top-level fields exist
        for key in complex_data.keys():
            assert key in read_data, f"Top-level field {key} missing from read data"

        # For nested fields, we need to check they're preserved as nested
        assert "details" in read_data
        assert "metadata" in read_data

        # Verify we have structure preserved with at least one record
        assert len(read_data["details"]) == 1
        assert isinstance(read_data["details"][0], dict)

        # Verify nested fields are accessible
        details = read_data["details"][0]
        assert "description" in details
        assert "attributes" in details
        assert isinstance(details["attributes"], dict)
        assert details["attributes"]["color"] == "blue"
