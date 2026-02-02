"""
Pytest configuration and fixtures for integration workflow tests.
"""

import json
import os

import pytest


@pytest.fixture
def test_output_dir(tmp_path):
    """
    Create a temporary directory for test outputs.

    Uses pytest's tmp_path for automatic cleanup.
    """
    output_dir = tmp_path / "test_output"
    output_dir.mkdir(exist_ok=True)
    return str(output_dir)


@pytest.fixture
def sample_records():
    """
    Generate a list of sample records for testing.
    """
    return [
        {
            "id": f"record-{i}",
            "name": f"Test Record {i}",
            "value": i * 10.5,
            "tags": ["test", f"tag-{i}"],
            "items": [{"item_id": f"item-{i}-{j}", "quantity": j} for j in range(1, 4)],
        }
        for i in range(1, 11)
    ]


@pytest.fixture
def sample_jsonl_file(sample_records, test_output_dir):
    """
    Create a sample JSONL file with test records.

    Returns the path to the created file.
    """
    file_path = os.path.join(test_output_dir, "sample_data.jsonl")

    with open(file_path, "w") as f:
        for record in sample_records:
            f.write(json.dumps(record) + "\n")

    return file_path
