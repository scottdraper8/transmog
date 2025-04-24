"""
Pytest configuration and fixtures for integration tests.
"""

import os
import json
import csv
import pytest
import tempfile
import shutil


@pytest.fixture
def test_output_dir():
    """
    Create a temporary directory for test outputs.

    The directory is automatically cleaned up after the test.
    """
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Clean up
    shutil.rmtree(temp_dir)


@pytest.fixture
def complex_data():
    """
    Provide a complex nested data structure for testing.
    """
    return {
        "id": "test-record-1",
        "name": "Integration Test Record",
        "active": True,
        "score": 95.5,
        "metadata": {
            "created_at": "2023-06-15T09:30:00Z",
            "updated_at": "2023-06-15T10:15:00Z",
            "tags": ["test", "integration", "complex"],
            "version": 2,
            "settings": {
                "visibility": "private",
                "notifications": True,
                "preferences": {"theme": "dark", "language": "en-US"},
            },
        },
        "items": [
            {
                "item_id": "item-1",
                "name": "First Item",
                "properties": {
                    "color": "blue",
                    "size": "medium",
                    "features": ["waterproof", "durable"],
                },
                "measurements": [
                    {"type": "width", "value": 10.5, "unit": "cm"},
                    {"type": "height", "value": 20.3, "unit": "cm"},
                ],
            },
            {
                "item_id": "item-2",
                "name": "Second Item",
                "properties": {
                    "color": "red",
                    "size": "large",
                    "features": ["lightweight", "foldable"],
                },
                "measurements": [
                    {"type": "width", "value": 15.2, "unit": "cm"},
                    {"type": "height", "value": 25.0, "unit": "cm"},
                ],
            },
        ],
        "contacts": [
            {"type": "email", "value": "test@example.com", "verified": True},
            {"type": "phone", "value": "+1234567890", "verified": False},
        ],
        "empty_array": [],
        "null_value": None,
    }


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


@pytest.fixture
def large_sample_records():
    """
    Generate a larger set of sample records for testing batch processing.
    """
    return [
        {
            "id": f"record-{i}",
            "name": f"Batch Test Record {i}",
            "value": i * 5.25,
            "metadata": {
                "batch": i // 50,
                "timestamp": f"2023-06-{(i % 30) + 1:02d}T{(i % 24):02d}:00:00Z",
            },
            "items": [
                {
                    "item_id": f"item-{i}-{j}",
                    "name": f"Item {j} of Record {i}",
                    "value": j * 2.5,
                }
                for j in range(1, (i % 5) + 2)
            ],
        }
        for i in range(1, 201)  # 200 records
    ]


@pytest.fixture
def csv_data():
    """
    Generate sample CSV data as a list of dictionaries.
    """
    return [
        {
            "id": "user-1",
            "name": "John Doe",
            "email": "john@example.com",
            "age": "32",
            "active": "true",
        },
        {
            "id": "user-2",
            "name": "Jane Smith",
            "email": "jane@example.com",
            "age": "28",
            "active": "true",
        },
        {
            "id": "user-3",
            "name": "Bob Johnson",
            "email": "bob@example.com",
            "age": "45",
            "active": "false",
        },
        {
            "id": "user-4",
            "name": "Alice Brown",
            "email": "alice@example.com",
            "age": "37",
            "active": "true",
        },
        {
            "id": "user-5",
            "name": "Charlie Davis",
            "email": "charlie@example.com",
            "age": "29",
            "active": "true",
        },
        {
            "id": "user-6",
            "name": "Eva Wilson",
            "email": "eva@example.com",
            "age": "41",
            "active": "false",
        },
        {
            "id": "user-7",
            "name": "David Miller",
            "email": "david@example.com",
            "age": "33",
            "active": "true",
        },
        {
            "id": "user-8",
            "name": "Grace Taylor",
            "email": "grace@example.com",
            "age": "26",
            "active": "true",
        },
        {
            "id": "user-9",
            "name": "Frank Roberts",
            "email": "frank@example.com",
            "age": "52",
            "active": "false",
        },
        {
            "id": "user-10",
            "name": "Helen Thomas",
            "email": "helen@example.com",
            "age": "39",
            "active": "true",
        },
    ]


@pytest.fixture
def sample_csv_file(csv_data, test_output_dir):
    """
    Create a sample CSV file with test data.

    Returns the path to the created file.
    """
    file_path = os.path.join(test_output_dir, "sample_data.csv")

    with open(file_path, "w", newline="") as f:
        if csv_data:
            fieldnames = csv_data[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)

    return file_path


@pytest.fixture
def nested_directory_structure(test_output_dir):
    """
    Create a nested directory structure with various file types for testing.

    Returns the path to the base directory containing the structure.
    """
    # Create base directories
    structure = {
        "data": {
            "raw": {"csv": {}, "json": {}, "text": {}},
            "processed": {},
            "temp": {},
        },
        "output": {"reports": {}, "exports": {}},
        "config": {},
    }

    # Create the directory structure
    base_dir = os.path.join(test_output_dir, "test_project")
    os.makedirs(base_dir)

    def create_dirs(structure, current_path):
        for key, value in structure.items():
            path = os.path.join(current_path, key)
            os.makedirs(path)
            if value:  # If it has subdirectories
                create_dirs(value, path)

    create_dirs(structure, base_dir)

    # Add some sample files
    with open(os.path.join(base_dir, "data", "raw", "csv", "sample1.csv"), "w") as f:
        f.write("id,name,value\n1,Item 1,10.5\n2,Item 2,20.3\n")

    with open(os.path.join(base_dir, "data", "raw", "json", "config.json"), "w") as f:
        json.dump({"version": "1.0", "settings": {"debug": True}}, f)

    with open(os.path.join(base_dir, "config", "settings.yaml"), "w") as f:
        f.write("# Sample YAML config\nversion: 1.0\ndebug: true\n")

    return base_dir
