"""
Pytest configuration file for Transmog tests.

This file contains fixtures and configuration for pytest tests.
"""

import os
import sys
import json
import pytest
from typing import Dict, List, Any

# Add the package root to sys.path for importing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from transmog import Processor


@pytest.fixture
def simple_data() -> Dict[str, Any]:
    """Return a simple nested JSON structure."""
    return {
        "id": 123,
        "name": "Test Entity",
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345",
        },
        "contacts": [
            {
                "type": "primary",
                "name": "John Doe",
                "phone": "555-1234",
                "details": {"department": "Sales", "position": "Manager"},
            },
            {
                "type": "secondary",
                "name": "Jane Smith",
                "phone": "555-5678",
                "details": {"department": "Support", "position": "Director"},
            },
        ],
    }


@pytest.fixture
def complex_data() -> Dict[str, Any]:
    """Return a complex nested JSON structure with multiple levels of nesting."""
    return {
        "id": 456,
        "name": "Complex Entity",
        "metadata": {
            "created_at": "2023-01-01T00:00:00Z",
            "updated_at": "2023-01-02T00:00:00Z",
            "status": "active",
            "tags": ["tag1", "tag2", "tag3"],
            "flags": {"important": True, "verified": False, "featured": True},
        },
        "details": {
            "description": "Complex nested structure",
            "attributes": {"color": "blue", "size": "medium", "weight": 15.5},
            "metrics": [
                {"name": "views", "value": 1000, "unit": "count"},
                {"name": "score", "value": 4.5, "unit": "points"},
            ],
        },
        "related_items": [
            {
                "id": "related-1",
                "name": "Related 1",
                "type": "reference",
                "strength": 0.9,
                "sub_items": [
                    {
                        "id": "sub-1-1",
                        "value": 0.1,
                        "properties": {"enabled": True, "visible": True},
                    },
                    {
                        "id": "sub-1-2",
                        "value": 0.2,
                        "properties": {"enabled": False, "visible": True},
                    },
                ],
            },
            {
                "id": "related-2",
                "name": "Related 2",
                "type": "similar",
                "strength": 0.7,
                "sub_items": [
                    {
                        "id": "sub-2-1",
                        "value": 0.3,
                        "properties": {"enabled": True, "visible": False},
                    }
                ],
            },
        ],
    }


@pytest.fixture
def batch_data() -> List[Dict[str, Any]]:
    """Return a batch of simple records for testing."""
    return [{"id": i, "name": f"Record {i}", "value": i * 10} for i in range(10)]


@pytest.fixture
def complex_batch() -> List[Dict[str, Any]]:
    """Return a batch of records with nested structures."""
    return [
        {
            "id": i,
            "name": f"Record {i}",
            "metadata": {
                "created": "2023-01-01",
                "type": "test" if i % 2 == 0 else "production",
            },
            "items": [
                {"id": f"{i}-{j}", "name": f"Item {j}", "quantity": j}
                for j in range(1, 4)
            ],
        }
        for i in range(5)
    ]


@pytest.fixture
def processor():
    """Fixture for a basic processor."""
    return Processor(
        cast_to_string=True,
        abbreviate_field_names=False,
        separator="_",  # Explicitly set separator to ensure consistency in tests
    )


@pytest.fixture
def test_output_dir(tmpdir) -> str:
    """Create and return a temporary output directory."""
    output_dir = os.path.join(tmpdir, "output")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


@pytest.fixture
def json_file(tmpdir, simple_data) -> str:
    """Create and return a temporary JSON file."""
    json_path = os.path.join(tmpdir, "test.json")
    with open(json_path, "w") as f:
        json.dump(simple_data, f)
    return json_path


@pytest.fixture
def jsonl_file(tmpdir, batch_data) -> str:
    """Create and return a temporary JSONL file."""
    jsonl_path = os.path.join(tmpdir, "test.jsonl")
    with open(jsonl_path, "w") as f:
        for record in batch_data:
            f.write(json.dumps(record) + "\n")
    return jsonl_path
