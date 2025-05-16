"""
Tests for file handling module.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from transmog.error import FileError, ParsingError
from transmog.process.file_handling import (
    detect_input_format,
    process_chunked,
    process_csv,
    process_file,
    process_file_to_format,
)
from transmog.process.utils import handle_file_error


@pytest.fixture
def mock_processor():
    """Create a mock processor for testing."""
    processor = MagicMock()
    processor.config.csv.delimiter = ","
    processor.config.csv.quote_char = '"'
    processor.config.csv.null_values = ["", "NULL", "null"]
    processor.config.csv.sanitize_column_names = True
    processor.config.csv.infer_types = True

    # Setup for process_chunked test
    processor.config.processing.batch_size = 10
    processor._process_batch.return_value = MagicMock(main_table=[], child_tables={})

    return processor


@pytest.fixture
def json_test_data():
    """Create test JSON data."""
    return [
        {"id": 1, "name": "Test 1"},
        {"id": 2, "name": "Test 2"},
    ]


@pytest.fixture
def json_file(json_test_data):
    """Create a temporary JSON file for testing."""
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(json_test_data, f)
    yield path
    os.unlink(path)


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


def test_detect_input_format():
    """Test input format detection."""
    assert detect_input_format("file.json") == "json"
    assert detect_input_format("file.jsonl") == "jsonl"
    assert detect_input_format("file.ndjson") == "jsonl"
    assert detect_input_format("file.csv") == "csv"
    # Default for unknown extension
    assert detect_input_format("file.txt") == "json"


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


@patch("transmog.process.file_handling.FileStrategy")
def test_process_file(MockFileStrategy, mock_processor, json_file):
    """Test process_file function."""
    # Setup the mock
    mock_strategy = MagicMock()
    MockFileStrategy.return_value = mock_strategy
    mock_result = MagicMock()
    mock_strategy.process.return_value = mock_result

    # Call the function
    result = process_file(mock_processor, json_file, "test_entity")

    # Verify the strategy was created and used correctly
    MockFileStrategy.assert_called_once_with(mock_processor.config)
    mock_strategy.process.assert_called_once_with(
        json_file, entity_name="test_entity", extract_time=None
    )
    assert result == mock_result


@patch("transmog.process.file_handling.CSVStrategy")
def test_process_file_csv(MockCSVStrategy, mock_processor, csv_file):
    """Test process_file function with CSV file."""
    # Setup the mock
    mock_strategy = MagicMock()
    MockCSVStrategy.return_value = mock_strategy
    mock_result = MagicMock()
    mock_strategy.process.return_value = mock_result

    # Call the function
    result = process_file(mock_processor, csv_file, "test_entity")

    # Verify the strategy was created and used correctly
    MockCSVStrategy.assert_called_once_with(mock_processor.config)
    mock_strategy.process.assert_called_once_with(
        csv_file, entity_name="test_entity", extract_time=None
    )
    assert result == mock_result


@patch("transmog.process.file_handling.os.path.getsize")
@patch("transmog.process.file_handling.process_file")
@patch("transmog.process.file_handling.os.makedirs")
def test_process_file_to_format_small_file(
    mock_makedirs, mock_process_file, mock_getsize, mock_processor, json_file
):
    """Test process_file_to_format with a small file."""
    # Setup mocks
    mock_getsize.return_value = 1000  # 1KB, smaller than threshold
    mock_result = MagicMock()
    mock_process_file.return_value = mock_result

    # Call function
    result = process_file_to_format(
        mock_processor, json_file, "test_entity", "json", "/tmp/output"
    )

    # Verify function behavior
    mock_process_file.assert_called_once_with(
        mock_processor, json_file, "test_entity", None
    )
    mock_makedirs.assert_called_once_with("/tmp/output", exist_ok=True)
    mock_result.write.assert_called_once()
    assert result == mock_result


@patch("transmog.process.file_handling.os.path.getsize")
@patch("transmog.process.streaming.stream_process_file_with_format")
@patch("transmog.process.file_handling.os.makedirs")
def test_process_file_to_format_large_file(
    mock_makedirs, mock_stream, mock_getsize, mock_processor, json_file
):
    """Test process_file_to_format with a large file."""
    # Setup mocks
    mock_getsize.return_value = 200 * 1024 * 1024  # 200MB, larger than threshold

    # Create a mock result for the ProcessingResult created in the function
    from transmog.process.result import ProcessingResult

    ProcessingResult([], {}, "test_entity")

    # Call function
    result = process_file_to_format(
        mock_processor, json_file, "test_entity", "json", "/tmp/output"
    )

    # Verify streaming was used for large file
    mock_stream.assert_called_once()
    mock_makedirs.assert_called_once_with("/tmp/output", exist_ok=True)

    # Check the result is a ProcessingResult with empty tables
    assert isinstance(result, ProcessingResult)
    assert result.main_table == []


@patch("transmog.process.file_handling.CSVStrategy")
def test_process_csv(MockCSVStrategy, mock_processor, csv_file):
    """Test process_csv function."""
    # Setup the mock
    mock_strategy = MagicMock()
    MockCSVStrategy.return_value = mock_strategy
    mock_result = MagicMock()
    mock_strategy.process.return_value = mock_result

    # Call the function
    result = process_csv(
        mock_processor,
        csv_file,
        "test_entity",
        delimiter=",",
        has_header=True,
        sanitize_column_names=True,
    )

    # Verify the strategy was created and used correctly
    MockCSVStrategy.assert_called_once_with(mock_processor.config)
    mock_strategy.process.assert_called_once()
    assert result == mock_result


def test_process_chunked():
    """Test process_chunked function with real implementation."""
    # Create a processor with default configuration
    from transmog import Processor, TransmogConfig

    processor = Processor(TransmogConfig.default())

    # Create simple test data
    test_data = [{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]

    # Process the data using process_chunked
    result = process_chunked(processor, test_data, "test_entity", chunk_size=1)

    # Verify results - note that integers may be converted to strings
    # due to cast_to_string=True in default config
    assert len(result.main_table) == 2

    # Compare either as string or integer depending on what's returned
    first_id = result.main_table[0]["id"]
    assert str(first_id) == "1" or first_id == 1

    second_id = result.main_table[1]["id"]
    assert str(second_id) == "2" or second_id == 2

    assert result.entity_name == "test_entity"
