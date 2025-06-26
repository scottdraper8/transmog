"""
Tests for streaming process functionality.

This module tests the streaming process functions for memory-efficient data processing.
"""

import glob
import json
import os
import tempfile
from typing import Any

import pytest

from transmog import Processor, TransmogConfig
from transmog.process.streaming import (
    _get_streaming_params,
    stream_process,
    stream_process_csv,
    stream_process_file,
    stream_process_file_with_format,
)


class TestStreamingProcessFunctions:
    """Tests for streaming process functions."""

    @pytest.fixture
    def processor(self):
        """Create a processor for testing."""
        config = TransmogConfig.default().with_metadata(force_transmog_id=True)
        return Processor(config=config)

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return [
            {"id": f"record{i}", "value": i, "nested": {"field": f"value{i}"}}
            for i in range(10)
        ]

    @pytest.fixture
    def test_data(self) -> list[dict[str, Any]]:
        """Create test data for streaming tests."""
        return [
            {
                "id": i,
                "name": f"Record {i}",
                "metadata": {"created": "2023-01-01"},
                "items": [
                    {"id": f"item-{i}-{j}", "value": j * 10} for j in range(1, 3)
                ],
            }
            for i in range(5)
        ]

    @pytest.fixture
    def json_file_path(self, test_data) -> str:
        """Create a temporary JSON file with test data."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
            temp_file.write(json.dumps(test_data).encode("utf-8"))
            return temp_file.name

    @pytest.fixture
    def jsonl_file_path(self, test_data) -> str:
        """Create a temporary JSONL file with test data."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as temp_file:
            for record in test_data:
                temp_file.write((json.dumps(record) + "\n").encode("utf-8"))
            return temp_file.name

    @pytest.fixture
    def csv_file_path(self) -> str:
        """Create a temporary CSV file."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
            temp_file.write(b"id,name,value\n1,Test 1,100\n2,Test 2,200\n3,Test 3,300")
            return temp_file.name

    def test_get_streaming_params(self, processor):
        """Test that streaming parameters are correctly generated."""
        # Test with default values
        params = _get_streaming_params(processor)
        assert "cast_to_string" in params
        assert "separator" in params
        assert "deeply_nested_threshold" in params

        # Test with extract_time - parameter name changed from transmog_time
        extract_time = "2023-01-01"
        params = _get_streaming_params(processor, extract_time=extract_time)
        assert "transmog_time" in params
        assert params["transmog_time"] == extract_time

        # Test with deterministic IDs
        params = _get_streaming_params(processor, use_deterministic_ids=True)
        assert params["use_deterministic_ids"] is True

        # Test with force_transmog_id - this parameter is now in the processor config
        # so we don't need to test passing it directly

    def test_stream_process(self, processor, sample_data, tmp_path):
        """Test stream_process function."""
        output_dir = tmp_path / "output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = output_dir / "test.json"

        # Stream process the data
        stream_process(
            processor=processor,
            data=sample_data,
            entity_name="test",
            output_format="json",
            output_destination=str(output_dir),
            batch_size=5,
        )

        # Verify output file exists
        assert output_file.exists()

        # Verify content
        with open(output_file) as f:
            content = json.load(f)
            assert isinstance(content, list)
            assert len(content) == len(sample_data)

    def test_stream_process_file(self, processor, sample_data, tmp_path):
        """Test stream_process_file function."""
        # Create input file
        input_file = tmp_path / "input.json"
        with open(input_file, "w") as f:
            json.dump(sample_data, f)

        # Create output directory
        output_dir = tmp_path / "output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = output_dir / "test.json"

        # Stream process the file
        stream_process_file(
            processor=processor,
            file_path=str(input_file),
            entity_name="test",
            output_format="json",
            output_destination=str(output_dir),
        )

        # Verify output file exists
        assert output_file.exists()

        # Verify content
        with open(output_file) as f:
            content = json.load(f)
            assert isinstance(content, list)
            assert len(content) == len(sample_data)

    def test_stream_process_csv(self, processor, tmp_path):
        """Test stream_process_csv function."""
        # Create CSV input file
        input_file = tmp_path / "input.csv"
        with open(input_file, "w") as f:
            f.write("id,value,nested_field\n")
            for i in range(5):
                f.write(f"record{i},{i},value{i}\n")

        # Create output directory
        output_dir = tmp_path / "output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = output_dir / "test.json"

        # Stream process the CSV file
        stream_process_csv(
            processor=processor,
            file_path=str(input_file),
            entity_name="test",
            output_format="json",
            output_destination=str(output_dir),
            delimiter=",",
            has_header=True,
        )

        # Verify output file exists
        assert output_file.exists()

        # Verify content
        with open(output_file) as f:
            content = json.load(f)
            assert isinstance(content, list)
            assert len(content) == 5

    def test_stream_process_file_with_format(self, processor, sample_data, tmp_path):
        """Test stream_process_file_with_format function."""
        # Create input file
        input_file = tmp_path / "input.json"
        with open(input_file, "w") as f:
            json.dump(sample_data, f)

        # Create output directory
        output_dir = tmp_path / "output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = output_dir / "test.json"

        # Stream process the file with explicit format

        stream_process_file_with_format(
            processor=processor,
            file_path=str(input_file),
            entity_name="test",
            output_format="json",
            format_type="json",
            output_destination=str(output_dir),
        )

        # Verify output file exists
        assert output_file.exists()

        # Verify content
        with open(output_file) as f:
            content = json.load(f)
            assert isinstance(content, list)
            assert len(content) == len(sample_data)

    def test_stream_process_to_csv(self, processor, sample_data, tmp_path):
        """Test stream processing to CSV format."""
        output_file = tmp_path / "output.csv"

        # Stream process to CSV
        stream_process(
            processor=processor,
            data=sample_data,
            entity_name="test",
            output_format="csv",
            output_destination=str(output_file),
            batch_size=5,
            force_transmog_id=True,  # Add this parameter to match refactored code
        )

        # Verify output file exists
        assert output_file.exists()

        # Verify content
        with open(output_file) as f:
            content = f.read()
            assert "id" in content
            assert "value" in content
            # CSV should have a header row plus data rows
            assert len(content.splitlines()) > len(sample_data)
