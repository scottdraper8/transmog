"""
Pytest configuration for Transmog v1.1.0 tests.

This file contains fixtures and configuration for testing the Transmog package.
All tests use real functionality without mocks.
"""

import json
import tempfile
from pathlib import Path
from typing import Any

import pytest

# Import the actual v1.1.0 API
import transmog as tm

# ---- Test Data Fixtures ----


@pytest.fixture
def simple_data() -> dict[str, Any]:
    """Simple nested data structure for basic testing."""
    return {
        "id": 1,
        "name": "Test Entity",
        "status": "active",
        "metadata": {
            "created_at": "2023-01-01T00:00:00Z",
            "updated_at": "2023-01-02T00:00:00Z",
            "version": 1,
        },
    }


@pytest.fixture
def array_data() -> dict[str, Any]:
    """Data with arrays for testing array handling."""
    return {
        "id": 1,
        "name": "Company",
        "tags": ["tech", "startup", "ai"],
        "employees": [
            {
                "id": 101,
                "name": "Alice",
                "role": "Engineer",
                "skills": ["python", "sql", "docker"],
            },
            {
                "id": 102,
                "name": "Bob",
                "role": "Designer",
                "skills": ["figma", "photoshop"],
            },
        ],
    }


@pytest.fixture
def complex_nested_data() -> dict[str, Any]:
    """Complex deeply nested data structure."""
    return {
        "id": 1,
        "name": "Complex Entity",
        "organization": {
            "id": "org-1",
            "name": "Main Org",
            "departments": [
                {
                    "id": "dept-1",
                    "name": "Engineering",
                    "teams": [
                        {
                            "id": "team-1",
                            "name": "Backend",
                            "members": [
                                {"id": "emp-1", "name": "John", "role": "Senior"},
                                {"id": "emp-2", "name": "Jane", "role": "Junior"},
                            ],
                        },
                        {
                            "id": "team-2",
                            "name": "Frontend",
                            "members": [
                                {"id": "emp-3", "name": "Mike", "role": "Lead"}
                            ],
                        },
                    ],
                },
                {
                    "id": "dept-2",
                    "name": "Sales",
                    "teams": [
                        {
                            "id": "team-3",
                            "name": "Enterprise",
                            "members": [
                                {"id": "emp-4", "name": "Sarah", "role": "Manager"}
                            ],
                        }
                    ],
                },
            ],
        },
    }


@pytest.fixture
def batch_data() -> list[dict[str, Any]]:
    """Batch of records for testing."""
    return [
        {
            "id": i,
            "name": f"Record {i}",
            "value": i * 10,
            "tags": [f"tag{i}", f"category{i % 3}"],
        }
        for i in range(1, 11)
    ]


@pytest.fixture
def mixed_types_data() -> dict[str, Any]:
    """Data with various data types."""
    return {
        "id": 1,
        "name": "Mixed Types Test",
        "active": True,
        "score": 95.5,
        "count": 42,
        "created_at": "2023-01-01T00:00:00Z",
        "tags": ["string", "array"],
        "metadata": {
            "null_value": None,
            "empty_string": "",
            "zero": 0,
            "false_value": False,
            "nested_array": [1, 2, 3],
        },
    }


@pytest.fixture
def problematic_data() -> list[dict[str, Any]]:
    """Data that might cause processing issues."""
    return [
        {"id": 1, "name": "Valid Record"},
        {"id": None, "name": "Null ID"},
        {"name": "Missing ID"},
        {"id": "", "name": "Empty ID"},
        {"id": 2, "name": None},
        {"id": 3, "name": ""},
        {"id": 4, "circular_ref": None},  # Will be modified in tests
    ]


# ---- File Fixtures ----


@pytest.fixture
def json_file(tmp_path, simple_data) -> str:
    """Create a temporary JSON file."""
    file_path = tmp_path / "test.json"
    with open(file_path, "w") as f:
        json.dump(simple_data, f)
    return str(file_path)


@pytest.fixture
def jsonl_file(tmp_path, batch_data) -> str:
    """Create a temporary JSONL file."""
    file_path = tmp_path / "test.jsonl"
    with open(file_path, "w") as f:
        for record in batch_data:
            f.write(json.dumps(record) + "\n")
    return str(file_path)


@pytest.fixture
def csv_file(tmp_path) -> str:
    """Create a temporary CSV file."""
    file_path = tmp_path / "test.csv"
    csv_content = """id,name,value,active
1,Alice,100,true
2,Bob,200,false
3,Charlie,300,true
"""
    with open(file_path, "w") as f:
        f.write(csv_content)
    return str(file_path)


@pytest.fixture
def large_json_file(tmp_path) -> str:
    """Create a large JSON file for streaming tests."""
    file_path = tmp_path / "large.json"
    large_data = [
        {
            "id": i,
            "name": f"Record {i}",
            "data": {
                "value": i * 10,
                "category": f"cat_{i % 5}",
                "items": [{"item_id": f"{i}-{j}", "quantity": j} for j in range(1, 4)],
            },
        }
        for i in range(1, 1001)  # 1000 records
    ]
    with open(file_path, "w") as f:
        json.dump(large_data, f)
    return str(file_path)


# ---- Output Directory Fixtures ----


@pytest.fixture
def output_dir(tmp_path) -> Path:
    """Create a temporary output directory."""
    output_path = tmp_path / "output"
    output_path.mkdir(exist_ok=True)
    return output_path


@pytest.fixture
def temp_file(tmp_path) -> Path:
    """Create a temporary file path."""
    return tmp_path / "temp_output.json"


# ---- Utility Functions ----


def assert_valid_result(result: tm.FlattenResult) -> None:
    """Assert that a FlattenResult is valid."""
    assert isinstance(result, tm.FlattenResult)
    assert hasattr(result, "main")
    assert hasattr(result, "tables")
    assert isinstance(result.main, list)
    assert isinstance(result.tables, dict)


def assert_record_has_id(record: dict[str, Any]) -> None:
    """Assert that a record has some form of ID."""
    assert "_id" in record or "id" in record, f"Record missing ID: {record}"


def assert_files_created(paths: list[str]) -> None:
    """Assert that all specified file paths exist."""
    for path in paths:
        assert Path(path).exists(), f"File not created: {path}"


def load_json_file(file_path: str) -> Any:
    """Load data from a JSON file."""
    with open(file_path) as f:
        return json.load(f)


def count_files_in_dir(directory: Path, pattern: str = "*") -> int:
    """Count files matching pattern in directory."""
    return len(list(directory.glob(pattern)))
