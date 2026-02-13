"""Tests for writer factory functionality."""

import csv

import pytest

from transmog.exceptions import ConfigurationError
from transmog.writers import CsvWriter, ParquetWriter, create_writer
from transmog.writers.orc import ORC_AVAILABLE

if ORC_AVAILABLE:
    from transmog.writers.orc import OrcWriter


class TestWriterFactory:
    """Test writer factory functions."""

    def test_create_csv_writer_returns_correct_type(self):
        """Test creating CSV writer returns CsvWriter."""
        writer = create_writer("csv")
        assert isinstance(writer, CsvWriter)

    def test_create_parquet_writer_returns_correct_type(self):
        """Test creating Parquet writer returns ParquetWriter."""
        writer = create_writer("parquet")
        assert isinstance(writer, ParquetWriter)

    @pytest.mark.skipif(not ORC_AVAILABLE, reason="PyArrow not available")
    def test_create_orc_writer_returns_correct_type(self):
        """Test creating ORC writer returns OrcWriter."""
        writer = create_writer("orc")
        assert isinstance(writer, OrcWriter)

    def test_create_writer_case_insensitive(self):
        """Test writer creation is case insensitive."""
        writer_lower = create_writer("csv")
        writer_upper = create_writer("CSV")
        writer_mixed = create_writer("Csv")

        assert type(writer_lower) is type(writer_upper) is type(writer_mixed)

    def test_create_writer_with_options_applies_them(self, tmp_path):
        """Test creating writer with custom options stores them."""
        writer = create_writer("csv", delimiter=";")

        output_file = tmp_path / "test.csv"
        writer.write([{"a": 1, "b": 2}], str(output_file))

        with open(output_file) as f:
            content = f.read()
        assert ";" in content

    def test_create_writer_unsupported_format(self):
        """Test creating writer for unsupported format."""
        with pytest.raises(ConfigurationError, match="Unsupported format"):
            create_writer("unsupported_format")

    def test_create_writer_empty_format(self):
        """Test creating writer with empty format."""
        with pytest.raises(ConfigurationError):
            create_writer("")

    def test_factory_returns_new_instances(self):
        """Test factory returns new instances each time."""
        writer1 = create_writer("csv")
        writer2 = create_writer("csv")
        assert writer1 is not writer2


class TestWriterFactoryIntegration:
    """Test writer factory integration with actual writing."""

    def test_csv_writer_integration(self, tmp_path):
        """Test factory-created CSV writer writes correct data with options."""
        writer = create_writer("csv", delimiter=";")

        data = [
            {"id": 1, "name": "Test 1", "value": 100},
            {"id": 2, "name": "Test 2", "value": 200},
        ]

        output_file = tmp_path / "factory_test.csv"
        writer.write(data, str(output_file))

        with open(output_file, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "Test 1"

    def test_parquet_writer_integration(self, tmp_path):
        """Test factory-created Parquet writer writes correct data."""
        import pyarrow.parquet as pq

        writer = create_writer("parquet")

        data = [
            {"id": 1, "name": "Test 1", "value": 100},
            {"id": 2, "name": "Test 2", "value": 200},
        ]

        output_file = tmp_path / "factory_test.parquet"
        writer.write(data, str(output_file))

        table = pq.read_table(str(output_file))
        assert table.num_rows == 2
        assert table.column("name").to_pylist() == ["Test 1", "Test 2"]

    @pytest.mark.skipif(not ORC_AVAILABLE, reason="PyArrow not available")
    def test_orc_writer_integration(self, tmp_path):
        """Test factory-created ORC writer writes correct data."""
        import pyarrow.orc as orc

        writer = create_writer("orc")

        data = [
            {"id": 1, "name": "Test 1", "value": 100},
            {"id": 2, "name": "Test 2", "value": 200},
        ]

        output_file = tmp_path / "factory_test.orc"
        writer.write(data, str(output_file))

        table = orc.read_table(str(output_file))
        assert table.num_rows == 2
        assert table.column("name").to_pylist() == ["Test 1", "Test 2"]
