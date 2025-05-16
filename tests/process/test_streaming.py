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
        return Processor(TransmogConfig.default())

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
        assert "abbreviate_field_names" in params

        # Test with extract_time
        extract_time = "2023-01-01"
        params = _get_streaming_params(processor, extract_time=extract_time)
        assert "extract_time" in params
        assert params["extract_time"] == extract_time

        # Test with deterministic IDs
        params = _get_streaming_params(processor, use_deterministic_ids=True)
        assert params["use_deterministic_ids"] is True

    def test_stream_process(self, processor, test_data, tmpdir):
        """Test the main stream_process function."""
        # Output directory (stream_process creates a directory)
        output_path = os.path.join(tmpdir, "output")
        os.makedirs(output_path, exist_ok=True)

        # Stream process the data, passing the directory path
        stream_process(
            processor=processor,
            data=test_data,
            entity_name="test_entity",
            output_format="json",
            output_destination=output_path,
            batch_size=2,
        )

        # Find all JSON files in the directory
        json_files = glob.glob(os.path.join(output_path, "*.json"))
        assert len(json_files) > 0, "No JSON files found in output directory"

        # Read the main table file
        main_file = os.path.join(output_path, "test_entity.json")
        assert os.path.exists(main_file), "Main entity file not found"

        # Read the main entity data
        with open(main_file) as f:
            main_data = json.load(f)

        # Verify structure - array of records
        assert isinstance(main_data, list)
        assert len(main_data) == 5  # 5 main records

        # Check for items table - at least one items table should exist
        items_files = [f for f in json_files if "items" in os.path.basename(f)]
        assert len(items_files) > 0, "No items-related tables found"

    def test_stream_process_file(self, processor, json_file_path, tmpdir):
        """Test processing a file with streaming."""
        # Output directory
        output_path = os.path.join(tmpdir, "output_file")
        os.makedirs(output_path, exist_ok=True)

        try:
            # Stream process the file
            stream_process_file(
                processor=processor,
                file_path=json_file_path,
                entity_name="test_entity",
                output_format="json",
                output_destination=output_path,
            )

            # Verify files were created
            json_files = glob.glob(os.path.join(output_path, "*.json"))
            assert len(json_files) > 0, "No JSON files found"

            # Verify the main entity file exists and contains data
            main_file = os.path.join(output_path, "test_entity.json")
            assert os.path.exists(main_file), "Main entity file not found"

            with open(main_file) as f:
                main_data = json.load(f)
                assert isinstance(main_data, list)
                assert len(main_data) > 0
        finally:
            # Clean up
            if os.path.exists(json_file_path):
                os.unlink(json_file_path)

    def test_stream_process_csv(self, processor, csv_file_path, tmpdir):
        """Test processing a CSV file with streaming."""
        # Output directory
        output_path = os.path.join(tmpdir, "output_csv")
        os.makedirs(output_path, exist_ok=True)

        try:
            # Stream process the CSV file
            stream_process_csv(
                processor=processor,
                file_path=csv_file_path,
                entity_name="test_entity",
                output_format="json",
                output_destination=output_path,
                has_header=True,
            )

            # Verify files were created
            json_files = glob.glob(os.path.join(output_path, "*.json"))
            assert len(json_files) > 0, "No JSON files found"

            # Verify the main entity file contains the 3 CSV rows
            main_file = os.path.join(output_path, "test_entity.json")
            assert os.path.exists(main_file), "Main entity file not found"

            with open(main_file) as f:
                main_data = json.load(f)
                assert isinstance(main_data, list)
                assert len(main_data) == 3  # 3 CSV rows
        finally:
            # Clean up
            if os.path.exists(csv_file_path):
                os.unlink(csv_file_path)

    def test_stream_process_file_with_format(self, processor, jsonl_file_path, tmpdir):
        """Test processing a file with a specific format."""
        # Output directory
        output_path = os.path.join(tmpdir, "output_format")
        os.makedirs(output_path, exist_ok=True)

        try:
            # Check function signature first
            import inspect

            sig = inspect.signature(stream_process_file_with_format)
            params = list(sig.parameters.keys())

            # Create appropriate args based on the function signature
            kwargs = {
                "processor": processor,
                "file_path": jsonl_file_path,
                "entity_name": "test_entity",
                "output_format": "json",
                "output_destination": output_path,
            }

            # Handle format_type vs input_format parameter
            if "format_type" in params:
                kwargs["format_type"] = "jsonl"
            if "input_format" in params:
                kwargs["input_format"] = "jsonl"

            # Stream process the file with appropriate parameters
            stream_process_file_with_format(**kwargs)

            # Verify files were created
            json_files = glob.glob(os.path.join(output_path, "*.json"))
            assert len(json_files) > 0, "No JSON files found"

            # Verify the main entity file exists
            main_file = os.path.join(output_path, "test_entity.json")
            assert os.path.exists(main_file), "Main entity file not found"
        finally:
            # Clean up
            if os.path.exists(jsonl_file_path):
                os.unlink(jsonl_file_path)

    def test_stream_process_to_csv(self, processor, test_data, tmpdir):
        """Test streaming process to CSV output."""
        # Output directory
        output_dir = os.path.join(tmpdir, "csv_output")
        os.makedirs(output_dir, exist_ok=True)

        try:
            # Stream process to CSV format
            stream_process(
                processor=processor,
                data=test_data,
                entity_name="test_entity",
                output_format="csv",
                output_destination=output_dir,
                batch_size=2,
            )

            # Verify CSV files were created
            csv_files = glob.glob(os.path.join(output_dir, "*.csv"))
            # Debug output if no files found
            if not csv_files:
                print(f"Output directory: {output_dir}")
                print(f"Directory contents: {os.listdir(output_dir)}")

            assert len(csv_files) > 0, "No CSV files found"

            # Verify the main table CSV file exists - the entity name is used as the filename
            main_file = os.path.join(output_dir, "test_entity.csv")

            # Debug output if main file not found
            if not os.path.exists(main_file):
                print(f"Main file not found: {main_file}")
                print(f"Available files: {csv_files}")

            assert os.path.exists(main_file), (
                f"Main entity CSV file not found. Files in directory: {csv_files}"
            )

            # Verify content (headers and rows)
            with open(main_file) as f:
                lines = f.readlines()
                header = lines[0].strip() if lines else ""
                data_rows = lines[1:] if len(lines) > 1 else []

            # Verify CSV structure
            assert "id" in header, "Header missing 'id' field"
            assert "name" in header, "Header missing 'name' field"
            assert len(data_rows) == 5, "Expected 5 data records"
        finally:
            # Clean up - remove temporary test files if needed
            if os.path.exists(output_dir):
                for file in os.listdir(output_dir):
                    try:
                        os.unlink(os.path.join(output_dir, file))
                    except Exception as e:
                        # Log the error but continue cleanup
                        print(f"Warning: Could not remove file {file}: {e}")
