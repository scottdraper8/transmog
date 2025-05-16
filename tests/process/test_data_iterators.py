"""
Tests for the data iterators module.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock

import pytest

from transmog.error import FileError, ParsingError, ValidationError
from transmog.process.data_iterators import (
    get_csv_file_iterator,
    get_data_iterator,
    get_json_data_iterator,
    get_json_file_iterator,
    get_jsonl_data_iterator,
    get_jsonl_file_iterator,
)


@pytest.fixture
def mock_processor():
    """Create a mock processor for testing."""
    processor = MagicMock()
    processor.config.csv.delimiter = ","
    processor.config.csv.quote_char = '"'
    processor.config.csv.sanitize_column_names = True
    processor.config.csv.infer_types = True
    processor.config.csv.null_values = ["", "NULL", "null"]

    # Create a mock recovery strategy
    recovery_strategy = MagicMock()
    recovery_strategy.is_strict.return_value = False
    processor.config.error_handling.recovery_strategy = recovery_strategy

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


def test_get_data_iterator_with_dict(mock_processor):
    """Test get_data_iterator with a dictionary."""
    data = {"id": 1, "name": "Test"}
    iterator = get_data_iterator(mock_processor, data)
    result = list(iterator)
    assert len(result) == 1
    assert result[0] == data


def test_get_data_iterator_with_list(mock_processor):
    """Test get_data_iterator with a list."""
    data = [{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]
    iterator = get_data_iterator(mock_processor, data)
    result = list(iterator)
    assert len(result) == 2
    assert result == data


def test_get_data_iterator_with_json_string(mock_processor):
    """Test get_data_iterator with a JSON string."""
    data = '[{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]'
    iterator = get_data_iterator(mock_processor, data)
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_get_data_iterator_with_jsonl_string(mock_processor):
    """Test get_data_iterator with a JSONL string."""
    data = '{"id": 1, "name": "Test 1"}\n{"id": 2, "name": "Test 2"}'
    iterator = get_data_iterator(mock_processor, data)
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_get_data_iterator_with_json_file(mock_processor, json_file):
    """Test get_data_iterator with a JSON file."""
    iterator = get_data_iterator(mock_processor, json_file)
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_get_data_iterator_with_jsonl_file(mock_processor, jsonl_file):
    """Test get_data_iterator with a JSONL file."""
    iterator = get_data_iterator(mock_processor, jsonl_file)
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_get_data_iterator_with_csv_file(mock_processor, csv_file):
    """Test get_data_iterator with a CSV file."""
    iterator = get_data_iterator(mock_processor, csv_file)
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == "1"
    assert result[0]["name"] == "Test 1"


def test_get_data_iterator_with_explicit_formats(
    mock_processor, json_file, jsonl_file, csv_file
):
    """Test get_data_iterator with explicitly specified formats."""
    # Test with JSON format
    iterator = get_data_iterator(mock_processor, json_file, input_format="json")
    result = list(iterator)
    assert len(result) == 2

    # Test with JSONL format
    iterator = get_data_iterator(mock_processor, jsonl_file, input_format="jsonl")
    result = list(iterator)
    assert len(result) == 2

    # Test with CSV format
    iterator = get_data_iterator(mock_processor, csv_file, input_format="csv")
    result = list(iterator)
    assert len(result) == 2


def test_get_data_iterator_with_invalid_format(mock_processor):
    """Test get_data_iterator with an invalid format."""
    with pytest.raises(ValueError):
        list(get_data_iterator(mock_processor, "test.json", input_format="invalid"))


def test_get_data_iterator_with_invalid_data_type(mock_processor):
    """Test get_data_iterator with an invalid data type."""
    with pytest.raises(ValidationError):
        list(get_data_iterator(mock_processor, 123))


def test_get_json_file_iterator(json_file):
    """Test get_json_file_iterator."""
    iterator = get_json_file_iterator(json_file)
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_get_json_file_iterator_file_not_found():
    """Test get_json_file_iterator with a non-existent file."""
    with pytest.raises(FileError):
        list(get_json_file_iterator("non_existent_file.json"))


def test_get_json_file_iterator_invalid_json(tmp_path):
    """Test get_json_file_iterator with invalid JSON."""
    file_path = tmp_path / "invalid.json"
    with open(file_path, "w") as f:
        f.write("{invalid json")

    with pytest.raises(ParsingError):
        list(get_json_file_iterator(str(file_path)))


def test_get_json_data_iterator_dict():
    """Test get_json_data_iterator with a dictionary."""
    data = {"id": 1, "name": "Test"}
    iterator = get_json_data_iterator(data)
    result = list(iterator)
    assert len(result) == 1
    assert result[0] == data


def test_get_json_data_iterator_list():
    """Test get_json_data_iterator with a list."""
    data = [{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]
    iterator = get_json_data_iterator(data)
    result = list(iterator)
    assert len(result) == 2
    assert result == data


def test_get_json_data_iterator_string():
    """Test get_json_data_iterator with a JSON string."""
    data = '[{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]'
    iterator = get_json_data_iterator(data)
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_get_json_data_iterator_invalid_type():
    """Test get_json_data_iterator with an invalid type."""
    with pytest.raises(ValidationError):
        list(get_json_data_iterator(123))


def test_get_jsonl_file_iterator(mock_processor, jsonl_file):
    """Test get_jsonl_file_iterator."""
    iterator = get_jsonl_file_iterator(mock_processor, jsonl_file)
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_get_jsonl_file_iterator_file_not_found(mock_processor):
    """Test get_jsonl_file_iterator with a non-existent file."""
    with pytest.raises(FileError):
        list(get_jsonl_file_iterator(mock_processor, "non_existent_file.jsonl"))


def test_get_jsonl_file_iterator_invalid_line(mock_processor, tmp_path):
    """Test get_jsonl_file_iterator with an invalid line."""
    file_path = tmp_path / "invalid.jsonl"
    with open(file_path, "w") as f:
        f.write('{"id": 1, "name": "Test 1"}\n')
        f.write("invalid json\n")
        f.write('{"id": 2, "name": "Test 2"}\n')

    # Set strict mode for this test
    mock_processor.config.error_handling.recovery_strategy.is_strict.return_value = True

    # This should raise an error in strict mode
    with pytest.raises(ParsingError):
        list(get_jsonl_file_iterator(mock_processor, str(file_path)))

    # Now set lenient mode and it should skip the bad line
    mock_processor.config.error_handling.recovery_strategy.is_strict.return_value = (
        False
    )
    result = list(get_jsonl_file_iterator(mock_processor, str(file_path)))
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_get_jsonl_data_iterator(mock_processor, tmp_path):
    """Test get_jsonl_data_iterator with actual data."""
    # Create test data
    jsonl_data = '{"id": 1, "name": "Test 1"}\n{"id": 2, "name": "Test 2"}'

    # Direct test without mocking
    iterator = get_jsonl_data_iterator(mock_processor, jsonl_data)
    result = list(iterator)

    # Verify results - we expect 2 records from the JSONL data
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_get_csv_file_iterator(mock_processor, csv_file):
    """Test get_csv_file_iterator."""
    iterator = get_csv_file_iterator(mock_processor, csv_file)
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == "1"
    assert result[0]["name"] == "Test 1"
    assert result[1]["id"] == "2"
    assert result[1]["name"] == "Test 2"


def test_get_csv_file_iterator_file_not_found(mock_processor):
    """Test get_csv_file_iterator with a non-existent file."""
    with pytest.raises(FileError):
        list(get_csv_file_iterator(mock_processor, "non_existent_file.csv"))


def test_get_csv_file_iterator_custom_options(mock_processor, tmp_path):
    """Test get_csv_file_iterator with custom options."""
    file_path = tmp_path / "test.csv"
    with open(file_path, "w") as f:
        f.write("id;name\n")
        f.write("1;Test 1\n")
        f.write("2;Test 2\n")

    iterator = get_csv_file_iterator(
        mock_processor, str(file_path), delimiter=";", has_header=True, infer_types=True
    )
    result = list(iterator)
    assert len(result) == 2
    assert result[0]["id"] == "1"
    assert result[0]["name"] == "Test 1"
