"""
Tests for streaming processing functionality.

Tests the streaming processing capabilities including batch processing,
memory optimization, and direct output to various formats.
"""

import csv
import glob
import json
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from transmog.config import TransmogConfig
from transmog.exceptions import ConfigurationError, ValidationError
from transmog.iterators import (
    get_json_file_iterator,
    get_jsonl_file_iterator,
)
from transmog.streaming import stream_process


def _read_csv_parts(directory: Path, entity_name: str) -> list[dict[str, Any]]:
    """Read all CSV part files and return combined records."""
    parts = sorted(directory.glob(f"{entity_name}_part_*.csv"))
    records = []
    for part in parts:
        with open(part) as f:
            reader = csv.DictReader(f)
            records.extend(reader)
    return records


class TestStreamProcessing:
    """Test the main stream_process function."""

    def test_stream_process_basic_data(self, tmp_path):
        """Test streaming processing of basic data."""
        data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        assert output_dir.exists()
        result = _read_csv_parts(output_dir, "users")
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

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="companies",
            output_format="csv",
            output_destination=str(output_dir),
        )

        main_parts = sorted(output_dir.glob("companies_part_*.csv"))
        assert len(main_parts) >= 1

        child_parts = sorted(output_dir.glob("companies_employees_part_*.csv"))
        assert len(child_parts) >= 1

        employees = _read_csv_parts(output_dir, "companies_employees")
        assert len(employees) == 2
        assert employees[0]["name"] == "Alice"

    def test_stream_process_iterator(self, tmp_path):
        """Test streaming processing with iterator input."""

        def data_generator():
            for i in range(5):
                yield {"id": i, "value": f"item_{i}"}

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data_generator(),
            entity_name="items",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "items")
        assert len(result) == 5

    def test_stream_process_csv_output(self, tmp_path):
        """Test streaming processing with CSV output."""
        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.2},
        ]

        config = TransmogConfig()
        output_dir = tmp_path / "scores"

        stream_process(
            config=config,
            data=data,
            entity_name="scores",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "scores")
        assert len(result) == 2
        assert any("Alice" in r.get("name", "") for r in result)

    def test_stream_process_batch_size(self, tmp_path):
        """Test streaming processing with custom batch size."""
        data = [{"id": i, "value": f"item_{i}"} for i in range(50)]

        config = TransmogConfig(batch_size=10)
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="items",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "items")
        assert len(result) == 50

    def test_stream_process_deterministic_ids(self, tmp_path):
        """Test streaming processing with deterministic ID generation."""
        data = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ]

        config = TransmogConfig(id_generation="natural", id_field="user_id")
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "users")
        assert "user_id" in result[0]

    def test_stream_process_with_id_field(self, tmp_path):
        """Test streaming processing with ID field."""
        data = [
            {"name": "Alice"},
            {"name": "Bob"},
        ]

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "users")
        assert "_id" in result[0]


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

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        data_iterator = get_json_file_iterator(str(input_file))
        stream_process(
            config=config,
            data=data_iterator,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "users")
        assert len(result) == 2

    def test_stream_process_jsonl_file(self, tmp_path):
        """Test streaming processing of JSONL file."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write('{"id": 1, "name": "Alice"}\n')
            f.write('{"id": 2, "name": "Bob"}\n')

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        data_iterator = get_jsonl_file_iterator(str(input_file))
        stream_process(
            config=config,
            data=data_iterator,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "users")
        assert len(result) == 2

    def test_stream_process_nonexistent_file(self):
        """Test streaming processing with nonexistent file."""
        with pytest.raises(ValidationError):
            list(get_json_file_iterator("nonexistent.json"))


class TestStreamingMemoryOptimization:
    """Test memory optimization in streaming processing."""

    def test_streaming_with_memory_optimized_config(self, tmp_path):
        """Test streaming with memory-optimized configuration."""
        data = [{"id": i, "value": f"item_{i}"} for i in range(100)]

        config = TransmogConfig(batch_size=100)
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="items",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "items")
        assert len(result) == 100

    def test_streaming_with_small_batch_size(self, tmp_path):
        """Test streaming with very small batch size for memory efficiency."""
        data = [{"id": i, "nested": {"value": i * 2}} for i in range(20)]

        config = TransmogConfig(batch_size=3)
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="items",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "items")
        assert len(result) == 20


class TestStreamingErrorHandling:
    """Test error handling in streaming processing."""

    def test_streaming_with_malformed_data(self, tmp_path):
        """Test streaming processing with malformed data."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": "invalid", "name": None},
            {"id": 3, "name": "Charlie"},
        ]

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        parts = sorted(output_dir.glob("users_part_*.csv"))
        assert len(parts) >= 1

    def test_streaming_with_invalid_output_format(self, tmp_path):
        """Test streaming with invalid output format."""
        data = [{"id": 1, "name": "Alice"}]

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        with pytest.raises(ConfigurationError, match="Unsupported format"):
            stream_process(
                config=config,
                data=data,
                entity_name="users",
                output_format="invalid_format",
                output_destination=str(output_dir),
            )

    def test_streaming_rejects_csv_compression(self, tmp_path):
        """Test that CSV streaming rejects compression options."""
        data = [{"id": 1, "name": "Alice"}]

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        with pytest.raises(ConfigurationError, match="compression"):
            stream_process(
                config=config,
                data=data,
                entity_name="users",
                output_format="csv",
                output_destination=str(output_dir),
                compression="gzip",
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

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="nested",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "nested")
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

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="mixed",
            output_format="csv",
            output_destination=str(output_dir),
        )

        parts = sorted(output_dir.glob("mixed_part_*.csv"))
        assert len(parts) >= 1

        all_csv_files = list(output_dir.glob("*.csv"))
        assert len(all_csv_files) >= 1

    def test_streaming_large_dataset_simulation(self, tmp_path):
        """Test streaming processing simulation with larger dataset."""

        def large_data_generator():
            for i in range(500):
                yield {
                    "id": i,
                    "name": f"User_{i}",
                    "metadata": {
                        "created": f"2023-01-{(i % 28) + 1:02d}",
                        "category": f"cat_{i % 10}",
                    },
                    "tags": [f"tag_{j}" for j in range(i % 5)],
                }

        config = TransmogConfig(batch_size=50)
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=large_data_generator(),
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "users")
        assert len(result) == 500


class TestStreamingOutputFormats:
    """Test streaming processing with different output formats."""

    def test_streaming_csv_output_content(self, tmp_path):
        """Test that streaming CSV output contains correct data."""
        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.2},
        ]

        config = TransmogConfig()
        output_dir = tmp_path / "csv_output"

        stream_process(
            config=config,
            data=data,
            entity_name="scores",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "scores")
        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[1]["name"] == "Bob"

    def test_streaming_with_csv_delimiter_option(self, tmp_path):
        """Test streaming with CSV delimiter format option."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="users",
            output_format="csv",
            output_destination=str(output_dir),
            delimiter="\t",
        )

        parts = sorted(output_dir.glob("users_part_*.csv"))
        assert len(parts) >= 1

        with open(parts[0]) as f:
            content = f.read()
        assert "\t" in content

    def test_streaming_to_parquet_format(self, tmp_path):
        """Test streaming to Parquet format with part files."""
        pytest.importorskip("pyarrow")

        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.2},
        ]

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="scores",
            output_format="parquet",
            output_destination=str(output_dir),
        )

        parts = sorted(output_dir.glob("scores_part_*.parquet"))
        assert len(parts) >= 1
        assert parts[0].stat().st_size > 0

        import pyarrow.parquet as pq

        table = pq.read_table(str(parts[0]))
        assert table.num_rows == 2
        assert "name" in table.schema.names
        assert "score" in table.schema.names


class TestStreamingEdgeCases:
    """Test edge cases in streaming processing."""

    def test_streaming_empty_data(self, tmp_path):
        """Test streaming processing with empty data."""
        data = []

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="empty",
            output_format="csv",
            output_destination=str(output_dir),
        )

        assert output_dir.exists()

    def test_streaming_single_record(self, tmp_path):
        """Test streaming processing with single record."""
        data = [{"id": 1, "name": "Single"}]

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="single",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "single")
        assert len(result) == 1

    def test_streaming_with_null_values(self, tmp_path):
        """Test streaming processing with null values."""
        data = [
            {"id": 1, "name": "Alice", "optional": None},
            {"id": 2, "name": None, "optional": "value"},
        ]

        config = TransmogConfig()
        output_dir = tmp_path / "output"

        stream_process(
            config=config,
            data=data,
            entity_name="nulls",
            output_format="csv",
            output_destination=str(output_dir),
        )

        result = _read_csv_parts(output_dir, "nulls")
        assert len(result) == 2


class TestStreamProcessingAvro:
    """Test streaming processing with Avro format."""

    @pytest.mark.skipif(
        not pytest.importorskip("fastavro", reason="fastavro not available"),
        reason="fastavro required",
    )
    def test_stream_to_avro(self, tmp_path):
        """Test streaming output to Avro format."""
        import fastavro

        data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 87},
            {"id": 3, "name": "Charlie", "score": 92},
        ]

        config = TransmogConfig(batch_size=2)
        output_dir = tmp_path / "avro_output"
        output_dir.mkdir()

        stream_process(
            config=config,
            data=data,
            entity_name="users",
            output_format="avro",
            output_destination=str(output_dir),
        )

        parts = sorted(output_dir.glob("users_part_*.avro"))
        assert len(parts) >= 1

        all_records = []
        for part in parts:
            with open(part, "rb") as f:
                reader = fastavro.reader(f)
                all_records.extend(reader)

        assert len(all_records) == 3
        names = {r["name"] for r in all_records}
        assert names == {"Alice", "Bob", "Charlie"}
