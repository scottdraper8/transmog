"""Tests for writer factory functions."""

import csv
import io

import pytest

from transmog.exceptions import ConfigurationError
from transmog.writers import (
    CsvStreamingWriter,
    CsvWriter,
    create_streaming_writer,
    create_writer,
)
from transmog.writers.orc import ORC_AVAILABLE


class TestCreateWriter:
    """Test create_writer()."""

    def test_create_writer_case_insensitive(self):
        """Format names are case insensitive."""
        writer_lower = create_writer("csv")
        writer_upper = create_writer("CSV")
        writer_mixed = create_writer("Csv")

        assert type(writer_lower) is type(writer_upper) is type(writer_mixed)
        assert isinstance(writer_lower, CsvWriter)

    def test_create_writer_with_options_applies_them(self, tmp_path):
        """Custom writer options are applied to output."""
        writer = create_writer("csv", delimiter=";")

        output_file = tmp_path / "test.csv"
        writer.write([{"a": 1, "b": 2}], str(output_file))

        with open(output_file, newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle, delimiter=";"))

        assert rows == [{"a": "1", "b": "2"}]

    def test_create_writer_unsupported_format(self):
        """Unknown formats raise ConfigurationError."""
        with pytest.raises(ConfigurationError, match="Unsupported format"):
            create_writer("unsupported_format")

    def test_create_writer_empty_format(self):
        """Empty format name raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="Unsupported format"):
            create_writer("")


class TestCreateStreamingWriter:
    """Test create_streaming_writer()."""

    def test_creates_csv_streaming_writer(self, tmp_path):
        """Factory returns a CSV streaming writer that emits parts."""
        writer = create_streaming_writer(
            "csv",
            destination=str(tmp_path),
            entity_name="items",
            batch_size=2,
            consolidate=False,
        )
        assert isinstance(writer, CsvStreamingWriter)

        with writer:
            writer.write_main_records([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
            writer.write_main_records([{"id": 3, "name": "c"}, {"id": 4, "name": "d"}])

        part_0 = tmp_path / "items_part_0000.csv"
        part_1 = tmp_path / "items_part_0001.csv"
        assert part_0.exists()
        assert part_1.exists()

        with open(part_0, newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        assert [row["id"] for row in rows] == ["1", "2"]

    def test_consolidate_default_merges_parts(self, tmp_path):
        """Default consolidate=True writes a single file per table."""
        writer = create_streaming_writer(
            "csv",
            destination=str(tmp_path),
            entity_name="items",
            batch_size=2,
        )
        with writer:
            writer.write_main_records([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
            writer.write_main_records([{"id": 3, "name": "c"}, {"id": 4, "name": "d"}])

        consolidated = tmp_path / "items.csv"
        assert consolidated.exists()
        assert not (tmp_path / "items_part_0000.csv").exists()

        with open(consolidated, newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        assert [row["id"] for row in rows] == ["1", "2", "3", "4"]

    def test_file_like_destination_rejected(self):
        """Streaming writers require a directory path."""
        with pytest.raises(ConfigurationError, match="directory path"):
            create_streaming_writer("csv", destination=io.StringIO())

    def test_unsupported_format(self, tmp_path):
        """Unknown streaming formats raise ConfigurationError."""
        with pytest.raises(ConfigurationError, match="Unsupported format"):
            create_streaming_writer("nope", destination=str(tmp_path))


class TestWriterFactoryIntegration:
    """Test factory-created writers against real files."""

    def test_csv_writer_writes_rows(self, tmp_path):
        """Factory-created CSV writer writes the given rows."""
        writer = create_writer("csv", delimiter=";")
        output_file = tmp_path / "factory_test.csv"
        writer.write(
            [
                {"id": 1, "name": "Test 1", "value": 100},
                {"id": 2, "name": "Test 2", "value": 200},
            ],
            str(output_file),
        )

        with open(output_file, encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter=";"))

        assert len(rows) == 2
        assert rows[0]["name"] == "Test 1"
        assert rows[1]["value"] == "200"

    def test_parquet_writer_writes_rows(self, tmp_path):
        """Factory-created Parquet writer writes the given rows."""
        import pyarrow.parquet as pq

        writer = create_writer("parquet")
        output_file = tmp_path / "factory_test.parquet"
        writer.write(
            [
                {"id": 1, "name": "Test 1", "value": 100},
                {"id": 2, "name": "Test 2", "value": 200},
            ],
            str(output_file),
        )

        table = pq.read_table(str(output_file))
        assert table.num_rows == 2
        assert table.column("name").to_pylist() == ["Test 1", "Test 2"]

    @pytest.mark.skipif(not ORC_AVAILABLE, reason="PyArrow ORC not available")
    def test_orc_writer_writes_rows(self, tmp_path):
        """Factory-created ORC writer writes the given rows."""
        import pyarrow.orc as orc

        writer = create_writer("orc")
        output_file = tmp_path / "factory_test.orc"
        writer.write(
            [
                {"id": 1, "name": "Test 1", "value": 100},
                {"id": 2, "name": "Test 2", "value": 200},
            ],
            str(output_file),
        )

        table = orc.read_table(str(output_file))
        assert table.num_rows == 2
        assert table.column("name").to_pylist() == ["Test 1", "Test 2"]
