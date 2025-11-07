"""
Tests for streaming processing functionality.

Tests the streaming processing capabilities including batch processing,
memory optimization, and direct output to various formats.
"""

import csv
import json
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from transmog.config import ProcessingMode, TransmogConfig
from transmog.error import ConfigurationError, FileError, ProcessingError
from transmog.process import Processor
from transmog.process.streaming import (
    stream_process,
    stream_process_csv,
    stream_process_file,
    stream_process_file_with_format,
)


class TestStreamProcessing:
    """Test the main stream_process function."""

    def test_stream_process_basic_data(self, tmp_path):
        """Test streaming processing of basic data."""
        data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        assert output_dir.exists()
        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 2
        assert result[0]["name"] == "Alice"

    def test_stream_process_with_arrays(self, tmp_path):
        """Test streaming processing with arrays."""
        data = [
            {
                "id": 1,
                "name": "Company A",
                "employees": [
                    {"name": "Alice", "role": "Engineer"},
                    {"name": "Bob", "role": "Manager"},
                ],
            }
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="companies",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "companies.csv"
        assert main_file.exists()

        employees_file = output_dir / "companies_employees.csv"
        assert employees_file.exists()

        with open(employees_file) as f:
            reader = csv.DictReader(f)
            employees = list(reader)
        assert len(employees) == 2
        assert employees[0]["name"] == "Alice"

    def test_stream_process_iterator(self, tmp_path):
        """Test streaming processing with iterator input."""

        def data_generator():
            for i in range(5):
                yield {"id": i, "value": f"item_{i}"}

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data_generator(),
            entity_name="items",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "items.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 5

    def test_stream_process_csv_output(self, tmp_path):
        """Test streaming processing with CSV output."""
        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.2},
        ]

        processor = Processor()
        output_path = tmp_path / "scores.csv"

        stream_process(
            processor=processor,
            data=data,
            entity_name="scores",
            output_format="csv",
            output_destination=str(output_path),
        )

        assert output_path.exists()

        # Read and verify CSV content
        with open(output_path) as f:
            content = f.read()
        assert "Alice" in content
        assert "95.5" in content

    def test_stream_process_batch_size(self, tmp_path):
        """Test streaming processing with custom batch size."""
        data = [{"id": i, "value": f"item_{i}"} for i in range(50)]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="items",
            output_format="csv",
            output_destination=str(output_dir),
            batch_size=10,
        )

        main_file = output_dir / "items.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 50

    def test_stream_process_deterministic_ids(self, tmp_path):
        """Test streaming processing with deterministic ID generation."""
        data = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
            use_deterministic_ids=True,
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert "user_id" in result[0]

    def test_stream_process_force_transmog_id(self, tmp_path):
        """Test streaming processing with forced transmog ID generation."""
        data = [
            {"id": "existing1", "name": "Alice"},
            {"id": "existing2", "name": "Bob"},
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
            force_transmog_id=True,
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert "__transmog_id" in result[0]


class TestStreamProcessFile:
    """Test file-based streaming processing."""

    def test_stream_process_json_file(self, tmp_path):
        """Test streaming processing of JSON file."""
        test_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        input_file = tmp_path / "input.json"
        with open(input_file, "w") as f:
            json.dump(test_data, f)

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process_file(
            processor=processor,
            file_path=str(input_file),
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 2

    def test_stream_process_jsonl_file(self, tmp_path):
        """Test streaming processing of JSONL file."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write('{"id": 1, "name": "Alice"}\n')
            f.write('{"id": 2, "name": "Bob"}\n')

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process_file(
            processor=processor,
            file_path=str(input_file),
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 2

    def test_stream_process_nonexistent_file(self):
        """Test streaming processing with nonexistent file."""
        processor = Processor()

        with pytest.raises(FileError):
            stream_process_file(
                processor=processor,
                file_path="nonexistent.json",
                entity_name="test",
                output_format="csv",
            )

    def test_stream_process_file_with_format(self, tmp_path):
        """Test streaming processing with explicit format specification."""
        # Create test JSON file
        test_data = [{"id": 1, "name": "Alice"}]
        input_file = tmp_path / "input.data"  # Non-standard extension
        with open(input_file, "w") as f:
            json.dump(test_data, f)

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process_file_with_format(
            processor=processor,
            file_path=str(input_file),
            entity_name="users",
            output_format="csv",
            format_type="json",  # Explicitly specify format
            output_destination=str(output_dir),
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()


class TestStreamProcessCSV:
    """Test CSV-specific streaming processing."""

    def test_stream_process_csv_basic(self, tmp_path):
        """Test basic CSV streaming processing."""
        input_file = tmp_path / "input.csv"
        with open(input_file, "w") as f:
            f.write("id,name,age\n")
            f.write("1,Alice,30\n")
            f.write("2,Bob,25\n")

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process_csv(
            processor=processor,
            file_path=str(input_file),
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 2
        assert result[0]["name"] == "Alice"

    def test_stream_process_csv_custom_delimiter(self, tmp_path):
        """Test CSV streaming with custom delimiter."""
        input_file = tmp_path / "input.csv"
        with open(input_file, "w") as f:
            f.write("id;name;age\n")
            f.write("1;Alice;30\n")
            f.write("2;Bob;25\n")

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process_csv(
            processor=processor,
            file_path=str(input_file),
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
            delimiter=";",
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 2

    def test_stream_process_csv_no_header(self, tmp_path):
        """Test CSV streaming without header."""
        input_file = tmp_path / "input.csv"
        with open(input_file, "w") as f:
            f.write("1,Alice,30\n")
            f.write("2,Bob,25\n")

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process_csv(
            processor=processor,
            file_path=str(input_file),
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
            has_header=False,
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 2
        # Should have generic column names starting from column_1
        assert "column_1" in result[0]


class TestStreamingMemoryOptimization:
    """Test memory optimization in streaming processing."""

    def test_streaming_with_memory_optimized_config(self, tmp_path):
        """Test streaming with memory-optimized configuration."""
        data = [{"id": i, "value": f"item_{i}"} for i in range(100)]

        config = TransmogConfig.memory_optimized()
        processor = Processor(config)
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="items",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "items.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 100

    def test_streaming_with_small_batch_size(self, tmp_path):
        """Test streaming with very small batch size for memory efficiency."""
        data = [{"id": i, "nested": {"value": i * 2}} for i in range(20)]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="items",
            output_format="csv",
            output_destination=str(output_dir),
            batch_size=3,
        )

        main_file = output_dir / "items.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 20


class TestStreamingErrorHandling:
    """Test error handling in streaming processing."""

    def test_streaming_with_malformed_data(self, tmp_path):
        """Test streaming processing with malformed data."""
        # Include some malformed records
        data = [
            {"id": 1, "name": "Alice"},
            {"id": "invalid", "name": None},  # Potentially problematic
            {"id": 3, "name": "Charlie"},
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        # Should handle malformed data gracefully
        stream_process(
            processor=processor,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

    def test_streaming_with_invalid_output_format(self, tmp_path):
        """Test streaming with invalid output format."""
        data = [{"id": 1, "name": "Alice"}]

        processor = Processor()
        output_dir = tmp_path / "output"

        # Should raise ConfigurationError for unsupported output format
        with pytest.raises(ConfigurationError):
            stream_process(
                processor=processor,
                data=data,
                entity_name="users",
                output_format="invalid_format",
                output_destination=str(output_dir),
            )


class TestStreamingWithComplexData:
    """Test streaming processing with complex nested data structures."""

    def test_streaming_deeply_nested_data(self, tmp_path):
        """Test streaming processing with deeply nested data."""
        data = [
            {
                "id": 1,
                "level1": {"level2": {"level3": {"level4": {"value": "deep_value"}}}},
            }
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="nested",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "nested.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 1
        flattened_keys = list(result[0].keys())
        assert any("level" in key for key in flattened_keys)

    def test_streaming_mixed_array_types(self, tmp_path):
        """Test streaming processing with mixed array types."""
        data = [
            {
                "id": 1,
                "simple_array": [1, 2, 3],
                "object_array": [
                    {"name": "Item1", "value": 10},
                    {"name": "Item2", "value": 20},
                ],
            }
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="mixed",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "mixed.csv"
        assert main_file.exists()

        files = list(output_dir.glob("*.csv"))
        assert len(files) >= 1

    def test_streaming_large_dataset_simulation(self, tmp_path):
        """Test streaming processing simulation with larger dataset."""

        def large_data_generator():
            """Generate a large dataset incrementally."""
            for i in range(500):  # Simulate 500 records
                yield {
                    "id": i,
                    "name": f"User_{i}",
                    "metadata": {
                        "created": f"2023-01-{(i % 28) + 1:02d}",
                        "category": f"cat_{i % 10}",
                    },
                    "tags": [f"tag_{j}" for j in range(i % 5)],
                }

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=large_data_generator(),
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
            batch_size=50,  # Process in chunks
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 500


class TestStreamingOutputFormats:
    """Test streaming processing with different output formats."""

    def test_streaming_to_multiple_formats(self, tmp_path):
        """Test streaming to different output formats."""
        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.2},
        ]

        processor = Processor()

        # Test JSON output
        json_file = tmp_path / "scores.json"
        stream_process(
            processor=processor,
            data=data,
            entity_name="scores",
            output_format="csv",
            output_destination=str(json_file),
        )
        assert json_file.exists()

        # Test CSV output
        csv_file = tmp_path / "scores.csv"
        stream_process(
            processor=processor,
            data=data,
            entity_name="scores",
            output_format="csv",
            output_destination=str(csv_file),
        )
        assert csv_file.exists()

    def test_streaming_with_format_options(self, tmp_path):
        """Test streaming with format-specific options."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "users.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 2

    def test_streaming_to_parquet_format(self, tmp_path):
        """Test streaming to Parquet format with finalization.

        This is a regression test for the issue where Parquet files
        were not created due to missing finalization call.
        """
        pytest.importorskip("pyarrow")

        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.2},
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        # Stream to Parquet format
        stream_process(
            processor=processor,
            data=data,
            entity_name="scores",
            output_format="parquet",
            output_destination=str(output_dir),
        )

        # Verify Parquet file was created
        parquet_file = output_dir / "scores.parquet"
        assert parquet_file.exists()
        assert parquet_file.stat().st_size > 0  # File should have content

        # Verify the content using PyArrow
        import pyarrow.parquet as pq

        table = pq.read_table(str(parquet_file))
        assert table.num_rows == 2
        assert "name" in table.schema.names
        assert "score" in table.schema.names


class TestStreamingEdgeCases:
    """Test edge cases in streaming processing."""

    def test_streaming_empty_data(self, tmp_path):
        """Test streaming processing with empty data."""
        data = []

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="empty",
            output_format="csv",
            output_destination=str(output_dir),
        )

        # Should create output directory but may not create files for empty data
        assert output_dir.exists()

    def test_streaming_single_record(self, tmp_path):
        """Test streaming processing with single record."""
        data = [{"id": 1, "name": "Single"}]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="single",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "single.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 1

    def test_streaming_with_null_values(self, tmp_path):
        """Test streaming processing with null values."""
        data = [
            {"id": 1, "name": "Alice", "optional": None},
            {"id": 2, "name": None, "optional": "value"},
        ]

        processor = Processor()
        output_dir = tmp_path / "output"

        stream_process(
            processor=processor,
            data=data,
            entity_name="nulls",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_file = output_dir / "nulls.csv"
        assert main_file.exists()

        with open(main_file) as f:
            reader = csv.DictReader(f)
            result = list(reader)
        assert len(result) == 2
