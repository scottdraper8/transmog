"""Tests for writer factory functionality."""

import threading
import time

import pytest

from transmog.exceptions import ConfigurationError
from transmog.writers import create_writer


class TestWriterFactory:
    """Test writer factory functions."""

    def test_create_csv_writer(self):
        """Test creating CSV writer."""
        writer = create_writer("csv")
        assert writer is not None
        assert hasattr(writer, "write")

    def test_create_parquet_writer(self):
        """Test creating Parquet writer."""
        writer = create_writer("parquet")
        assert writer is not None
        assert hasattr(writer, "write")

    def test_create_orc_writer(self):
        """Test creating ORC writer."""
        writer = create_writer("orc")
        assert writer is not None
        assert hasattr(writer, "write")

    def test_create_writer_case_insensitive(self):
        """Test writer creation is case insensitive."""
        writer_lower = create_writer("csv")
        writer_upper = create_writer("CSV")
        writer_mixed = create_writer("Csv")

        assert writer_lower is not None
        assert writer_upper is not None
        assert writer_mixed is not None
        assert isinstance(writer_lower, type(writer_upper))

    def test_create_writer_with_options(self):
        """Test creating writer with custom options."""
        csv_writer = create_writer("csv", delimiter=";")
        assert csv_writer is not None

        parquet_writer = create_writer("parquet", compression="snappy")
        assert parquet_writer is not None

        orc_writer = create_writer("orc", compression="zstd")
        assert orc_writer is not None

    def test_create_writer_unsupported_format(self):
        """Test creating writer for unsupported format."""
        with pytest.raises(ConfigurationError) as exc_info:
            create_writer("unsupported_format")

        assert "Unsupported format" in str(exc_info.value)

    def test_create_writer_empty_format(self):
        """Test creating writer with empty format."""
        with pytest.raises(ConfigurationError):
            create_writer("")

    def test_factory_returns_new_instances(self):
        """Test factory returns new instances."""
        writer1 = create_writer("csv")
        writer2 = create_writer("csv")
        assert writer1 is not writer2


class TestWriterFactoryIntegration:
    """Test writer factory integration with actual writing."""

    def test_csv_writer_integration(self, tmp_path):
        """Test factory-created CSV writer integration."""
        writer = create_writer("csv", delimiter=";")

        data = [
            {"id": 1, "name": "Test 1", "value": 100},
            {"id": 2, "name": "Test 2", "value": 200},
        ]

        output_file = tmp_path / "factory_test.csv"
        writer.write(data, str(output_file))

        assert output_file.exists()

        import csv

        with open(output_file, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "Test 1"

    def test_parquet_writer_integration(self, tmp_path):
        """Test factory-created Parquet writer integration."""
        pytest.importorskip("pyarrow")

        writer = create_writer("parquet")

        data = [
            {"id": 1, "name": "Test 1", "value": 100},
            {"id": 2, "name": "Test 2", "value": 200},
        ]

        output_file = tmp_path / "factory_test.parquet"
        writer.write(data, str(output_file))

        assert output_file.exists()

        import pyarrow.parquet as pq

        table = pq.read_table(str(output_file))

        assert table.num_rows == 2
        assert "name" in table.schema.names
        name_column = table.column("name").to_pylist()
        assert name_column[0] == "Test 1"

    def test_orc_writer_integration(self, tmp_path):
        """Test factory-created ORC writer integration."""
        pytest.importorskip("pyarrow")

        writer = create_writer("orc")

        data = [
            {"id": 1, "name": "Test 1", "value": 100},
            {"id": 2, "name": "Test 2", "value": 200},
        ]

        output_file = tmp_path / "factory_test.orc"
        writer.write(data, str(output_file))

        assert output_file.exists()

        import pyarrow.orc as orc

        table = orc.read_table(str(output_file))

        assert table.num_rows == 2
        assert "name" in table.schema.names
        name_column = table.column("name").to_pylist()
        assert name_column[0] == "Test 1"


class TestWriterFactoryThreadSafety:
    """Test writer factory thread safety."""

    def test_factory_thread_safety(self):
        """Test factory is thread-safe."""
        results = []
        errors = []

        def create_writers(thread_id):
            try:
                for i in range(10):
                    writer = create_writer("csv")
                    results.append((thread_id, i, type(writer).__name__))
                    time.sleep(0.001)
            except Exception as e:
                errors.append((thread_id, e))

        threads = []
        for i in range(5):
            thread = threading.Thread(target=create_writers, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0, f"Thread errors: {errors}"
        assert len(results) == 50

    def test_factory_handles_many_writers(self):
        """Test factory handles creating many writers."""
        writers = []
        for i in range(100):
            format_type = ["csv", "parquet", "orc"][i % 3]
            writer = create_writer(format_type)
            writers.append(writer)

        assert len(writers) == 100

        for writer in writers:
            assert hasattr(writer, "write")
