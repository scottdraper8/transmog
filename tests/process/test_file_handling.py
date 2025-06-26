"""
Tests for file handling module.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from transmog import Processor, TransmogConfig
from transmog.error import FileError, ParsingError
from transmog.process.file_handling import (
    detect_input_format,
    process_chunked,
    process_csv,
    process_file,
    process_file_to_format,
)
from transmog.process.result import ProcessingResult
from transmog.process.utils import handle_file_error


@pytest.fixture
def processor():
    """Create a real processor for testing."""
    config = TransmogConfig.default().with_metadata(force_transmog_id=True)
    return Processor(config=config)


@pytest.fixture
def sample_json_file():
    """Create a temporary JSON file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        json.dump(
            [
                {"id": "record1", "name": "Test Record 1"},
                {"id": "record2", "name": "Test Record 2"},
            ],
            f,
        )
        return f.name


@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        f.write(b"id,name\nrecord1,Test Record 1\nrecord2,Test Record 2\n")
        return f.name


def test_process_file(processor, sample_json_file):
    """Test process_file function with a JSON file."""
    # Process the file
    result = process_file(
        processor=processor,
        file_path=sample_json_file,
        entity_name="test",
    )

    # Verify result
    assert result is not None
    assert result.entity_name == "test"

    # Check main table
    main_table = result.get_main_table()
    assert len(main_table) == 2
    assert main_table[0]["id"] == "record1"
    assert main_table[1]["id"] == "record2"

    # Clean up
    os.unlink(sample_json_file)


def test_process_file_csv(processor, sample_csv_file):
    """Test process_file function with a CSV file."""
    # Process the CSV file
    result = process_file(
        processor=processor,
        file_path=sample_csv_file,
        entity_name="test",
    )

    # Verify result
    assert result is not None
    assert result.entity_name == "test"

    # Check main table
    main_table = result.get_main_table()
    assert len(main_table) == 2
    assert main_table[0]["id"] == "record1"
    assert main_table[1]["id"] == "record2"

    # Clean up
    os.unlink(sample_csv_file)


def test_detect_input_format():
    """Test detect_input_format function."""
    assert detect_input_format("file.json") == "json"
    assert detect_input_format("file.jsonl") == "jsonl"
    assert detect_input_format("file.ndjson") == "jsonl"
    assert detect_input_format("file.csv") == "csv"
    assert detect_input_format("file.txt") == "json"  # Default


def test_process_file_to_format(processor, sample_json_file, tmp_path):
    """Test process_file_to_format function."""


@pytest.fixture
def json_test_data():
    """Create test data for JSON file tests."""
    return [
        {"id": f"record{i}", "value": i, "nested": {"field": f"value{i}"}}
        for i in range(5)
    ]


@pytest.fixture
def json_file(json_test_data, tmp_path):
    """Create a JSON file with test data."""
    file_path = tmp_path / "test.json"
    with open(file_path, "w") as f:
        json.dump(json_test_data, f)
    return str(file_path)


@pytest.fixture
def jsonl_file(json_test_data):
    """Create a temporary JSONL file for testing."""
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    with os.fdopen(fd, "w") as f:
        for item in json_test_data:
            f.write(json.dumps(item) + "\n")
    yield path
    os.unlink(path)


@pytest.fixture
def csv_file():
    """Create a temporary CSV file for testing."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "w") as f:
        f.write("id,name\n")
        f.write("1,Test 1\n")
        f.write("2,Test 2\n")
    yield path
    os.unlink(path)


def test_handle_file_error():
    """Test file error handling."""
    # Test FileError passthrough
    file_error = FileError("Test file error")
    with pytest.raises(FileError) as exc_info:
        try:
            # Need to raise the error first for handle_file_error to re-raise it
            raise file_error
        except Exception as e:
            handle_file_error("test.json", e)
    assert "Test file error" in str(exc_info.value)

    # Test JSONDecodeError conversion
    json_error = json.JSONDecodeError("Test decode error", "", 0)
    with pytest.raises(ParsingError) as exc_info:
        try:
            # Need to raise the error first for handle_file_error to re-raise it
            raise json_error
        except Exception as e:
            handle_file_error("test.json", e)
    assert "Invalid JSON" in str(exc_info.value)

    # Test generic exception conversion
    generic_error = Exception("Test exception")
    with pytest.raises(FileError) as exc_info:
        try:
            # Need to raise the error first for handle_file_error to re-raise it
            raise generic_error
        except Exception as e:
            handle_file_error("test.json", e)
    assert "Error reading" in str(exc_info.value)


@patch("transmog.process.file_handling.os.path.getsize")
@patch("transmog.process.file_handling.process_file")
@patch("transmog.process.file_handling.os.makedirs")
def test_process_file_to_format_small_file(
    mock_makedirs, mock_process_file, mock_getsize, processor, json_file, tmp_path
):
    """Test process_file_to_format with a small file."""
    # Mock file size to be small (under threshold)
    mock_getsize.return_value = 1024

    # Set up mock result
    mock_result = MagicMock(spec=ProcessingResult)
    mock_process_file.return_value = mock_result

    # Set up output directory
    output_dir = os.path.join(tmp_path, "output")

    # Call the function
    result = process_file_to_format(
        processor=processor,
        file_path=json_file,
        entity_name="test_entity",
        output_format="json",
        output_path=output_dir,
    )

    # Verify process_file was called (not streaming)
    mock_process_file.assert_called_once_with(processor, json_file, "test_entity", None)
    mock_makedirs.assert_called_once_with(output_dir, exist_ok=True)
    mock_result.write.assert_called_once()
    assert result == mock_result


@patch("transmog.process.file_handling.os.path.getsize")
@patch("transmog.process.streaming.stream_process_file_with_format")
@patch("transmog.process.file_handling.os.makedirs")
def test_process_file_to_format_large_file(
    mock_makedirs, mock_stream_process, mock_getsize, processor, json_file, tmp_path
):
    """Test process_file_to_format with a large file."""
    # Mock file size to be large (above threshold)
    mock_getsize.return_value = 100 * 1024 * 1024 + 1  # Just over 100MB

    # Set up output directory
    output_dir = os.path.join(tmp_path, "output")

    # Call the function
    result = process_file_to_format(
        processor=processor,
        file_path=json_file,
        entity_name="test_entity",
        output_format="json",
        output_path=output_dir,
    )

    # Verify stream_process_file_with_format was called (not regular process)
    mock_stream_process.assert_called_once()
    assert isinstance(result, ProcessingResult)
    assert result.main_table == []
    assert "streaming" in result.source_info
    assert result.source_info["streaming"] is True


@patch("transmog.process.file_handling.CSVStrategy")
def test_process_csv(MockCSVStrategy, processor, csv_file):
    """Test process_csv function."""
    # Set up mock strategy
    mock_strategy = MagicMock()
    MockCSVStrategy.return_value = mock_strategy
    mock_strategy.process.return_value = ProcessingResult([], {}, "test_entity")

    # Call the function
    result = process_csv(
        processor=processor,
        file_path=csv_file,
        entity_name="test_entity",
        delimiter=",",
        has_header=True,
    )

    # Verify CSVStrategy was created and used
    MockCSVStrategy.assert_called_once()
    mock_strategy.process.assert_called_once()
    assert isinstance(result, ProcessingResult)
