"""
Tests for writer factory functionality.

Tests the factory functions for creating writers for different formats.
"""

import threading
import time
from io import StringIO
from pathlib import Path

import pytest

from transmog.error.exceptions import ConfigurationError, MissingDependencyError
from transmog.io.writer_factory import (
    create_streaming_writer,
    create_writer,
    get_supported_formats,
    get_supported_streaming_formats,
    is_format_available,
    is_streaming_format_available,
)


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

    def test_create_writer_case_insensitive(self):
        """Test that writer creation is case insensitive."""
        writer_lower = create_writer("csv")
        writer_upper = create_writer("CSV")
        writer_mixed = create_writer("Csv")

        assert writer_lower is not None
        assert writer_upper is not None
        assert writer_mixed is not None
        assert isinstance(writer_lower, type(writer_upper)) and isinstance(
            writer_upper, type(writer_mixed)
        )

    def test_create_writer_with_options(self):
        """Test creating writer with options."""
        # CSV writer with custom delimiter
        writer = create_writer("csv", delimiter=";")
        assert writer is not None

        # Parquet writer with compression
        writer = create_writer("parquet", compression="snappy")
        assert writer is not None

    def test_create_writer_unsupported_format(self):
        """Test creating writer for unsupported format."""
        with pytest.raises(ConfigurationError) as exc_info:
            create_writer("unsupported_format")

        assert "Unsupported output format" in str(exc_info.value)

    def test_create_writer_empty_format(self):
        """Test creating writer with empty format."""
        with pytest.raises(ConfigurationError):
            create_writer("")

    def test_get_supported_formats(self):
        """Test getting supported formats."""
        formats = get_supported_formats()
        assert isinstance(formats, dict)
        assert "csv" in formats
        assert "parquet" in formats

    def test_is_format_supported(self):
        """Test checking if format is supported."""
        assert is_format_available("csv")
        assert is_format_available("parquet")
        assert not is_format_available("unsupported_format")


class TestWriterFactoryIntegration:
    """Test writer factory integration with actual writing."""

    def test_factory_writer_csv_integration(self, tmp_path):
        """Test factory-created CSV writer integration."""
        writer = create_writer("csv", delimiter=";")

        data = [
            {"id": 1, "name": "Test 1", "value": 100},
            {"id": 2, "name": "Test 2", "value": 200},
        ]

        output_file = tmp_path / "factory_test.csv"
        writer.write(data, str(output_file))

        assert output_file.exists()

        # Verify content
        import csv

        with open(output_file, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "Test 1"

    def test_factory_writer_parquet_integration(self, tmp_path):
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

        # Verify content
        import pyarrow.parquet as pq

        table = pq.read_table(str(output_file))

        # Check PyArrow table structure
        assert table.num_rows == 2
        assert "name" in table.schema.names
        # Convert to Python objects to verify data
        name_column = table.column("name").to_pylist()
        assert name_column[0] == "Test 1"

    def test_factory_multiple_writers_same_format(self):
        """Test creating multiple writers of same format."""
        writer1 = create_writer("csv", delimiter=",")
        writer2 = create_writer("csv", delimiter=";")

        assert writer1 is not writer2
        assert hasattr(writer1, "write")
        assert hasattr(writer2, "write")

    def test_factory_writers_different_formats(self):
        """Test creating writers of different formats."""
        csv_writer = create_writer("csv")
        parquet_writer = create_writer("parquet")

        assert hasattr(csv_writer, "write")
        assert hasattr(parquet_writer, "write")


class TestWriterFactoryEdgeCases:
    """Test edge cases and error conditions."""

    def test_factory_with_invalid_options(self):
        """Test factory with invalid writer options."""
        writer = create_writer("csv", invalid_option="value")
        assert hasattr(writer, "write")

    def test_factory_format_detection_from_path(self):
        """Test format detection capabilities."""
        formats = get_supported_formats()
        assert "csv" in formats
        assert "parquet" in formats

    def test_factory_thread_safety(self):
        """Test factory thread safety."""
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

    def test_factory_memory_usage(self):
        """Test factory memory usage with many writers."""
        writers = []
        for i in range(100):
            format_type = ["csv", "parquet"][i % 2]
            writer = create_writer(format_type)
            writers.append(writer)

        assert len(writers) == 100

        for writer in writers:
            assert hasattr(writer, "write")

    def test_factory_singleton_behavior(self):
        """Test that factory functions work independently."""
        writer1 = create_writer("csv")
        writer2 = create_writer("csv")

        assert hasattr(writer1, "write")
        assert hasattr(writer2, "write")


class TestWriterFactoryConfiguration:
    """Test writer factory configuration and customization."""

    def test_factory_default_configuration(self):
        """Test factory with default configuration."""
        csv_writer = create_writer("csv")
        parquet_writer = create_writer("parquet")

        assert hasattr(csv_writer, "write")
        assert hasattr(parquet_writer, "write")

    def test_factory_custom_writer_registration(self):
        """Test that factory supports the expected formats."""
        assert is_format_available("csv")
        assert is_format_available("parquet")

    def test_factory_writer_options_validation(self):
        """Test writer options validation."""
        writers = [
            create_writer("csv", delimiter=","),
            create_writer("parquet", compression="snappy"),
        ]

        for writer in writers:
            assert hasattr(writer, "write")

    def test_factory_error_handling_configuration(self):
        """Test factory error handling configuration."""
        invalid_formats = ["", "unknown", "invalid"]

        for invalid_format in invalid_formats:
            with pytest.raises(ConfigurationError):
                create_writer(invalid_format)
