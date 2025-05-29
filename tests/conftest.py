"""
Pytest configuration for Transmog tests.

This file contains fixtures and configuration for testing the Transmog package.
"""

import json
import os
import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

# Add the package root to sys.path for importing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import core package components
from transmog import Processor
from transmog.config import TransmogConfig


# Clear any module-level caches before and after tests
@pytest.fixture(autouse=True)
def clear_caches():
    """Clear all caches before and after each test to prevent state pollution."""
    # Import locally to avoid circular imports
    from transmog.core.flattener import clear_caches

    # Clear caches before test
    clear_caches()

    # Run the test
    yield

    # Clear caches after test
    clear_caches()


# ---- Test Data Fixtures ----


@pytest.fixture
def simple_data() -> dict[str, Any]:
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
def complex_data() -> dict[str, Any]:
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
def batch_data() -> list[dict[str, Any]]:
    """Return a batch of simple records for testing."""
    return [{"id": i, "name": f"Record {i}", "value": i * 10} for i in range(10)]


@pytest.fixture
def complex_batch() -> list[dict[str, Any]]:
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
def deeply_nested_data() -> dict[str, Any]:
    """Return data with a deeply nested structure for max depth testing."""
    result = {"id": 789, "name": "Deeply Nested Structure"}

    # Create a deeply nested structure (10 levels deep)
    current = result
    for i in range(10):
        current["level"] = {"id": f"level-{i}", "name": f"Level {i}"}
        current = current["level"]

    return result


# ---- Test File Fixtures ----


@pytest.fixture
def json_file(tmp_path, simple_data) -> str:
    """Create and return a temporary JSON file."""
    json_path = tmp_path / "test.json"
    with open(json_path, "w") as f:
        json.dump(simple_data, f)
    return str(json_path)


@pytest.fixture
def jsonl_file(tmp_path, batch_data) -> str:
    """Create and return a temporary JSONL file."""
    jsonl_path = tmp_path / "test.jsonl"
    with open(jsonl_path, "w") as f:
        for record in batch_data:
            f.write(json.dumps(record) + "\n")
    return str(jsonl_path)


@pytest.fixture
def csv_file(tmp_path, batch_data) -> str:
    """Create and return a temporary CSV file."""
    import csv

    csv_path = tmp_path / "test.csv"

    # Get all keys from the batch
    keys = set()
    for record in batch_data:
        keys.update(record.keys())
    keys = sorted(keys)

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(batch_data)

    return str(csv_path)


# ---- Processor Fixtures ----


@pytest.fixture
def processor():
    """Return a basic processor with default configuration."""
    config = (
        TransmogConfig.default()
        .with_processing(cast_to_string=True)
        .with_naming(
            separator="_",  # Ensure consistency in tests
            deeply_nested_threshold=4,  # Use standard deeply nested threshold
        )
    )
    return Processor(config=config)


@pytest.fixture
def memory_optimized_processor():
    """Return a memory-optimized processor."""
    return Processor.memory_optimized()


@pytest.fixture
def performance_optimized_processor():
    """Return a performance-optimized processor."""
    return Processor.performance_optimized()


# ---- Dependency Management Fixtures ----


@pytest.fixture
def dependency_status():
    """Return the availability status of optional dependencies."""
    return {
        "pyarrow": _is_dependency_available("pyarrow"),
        "orjson": _is_dependency_available("orjson"),
        "zstandard": _is_dependency_available("zstandard"),
    }


def _is_dependency_available(module_name):
    """Check if a dependency is available."""
    try:
        __import__(module_name)
        return True
    except ImportError:
        return False


@pytest.fixture
def dependency_injection_factory():
    """Create a factory for injecting dependencies."""

    def _create_dependency_injector(module_name, mock_attributes=None):
        """
        Create an injector for dependencies.

        Args:
            module_name: Name of the module to inject
            mock_attributes: Dictionary of attributes to mock

        Returns:
            Function that injects dependencies
        """
        mock_attributes = mock_attributes or {}

        def _injector(monkeypatch):
            # Try to import the real module
            try:
                real_module = __import__(module_name)

                # Create a mock that wraps the real module
                mock_module = MagicMock(wraps=real_module)

                # Override specific attributes
                for attr_name, mock_value in mock_attributes.items():
                    setattr(mock_module, attr_name, mock_value)

            except ImportError:
                # If real module is not available, create a pure mock
                mock_module = MagicMock()

                # Add a custom is_available method that returns False
                mock_module.is_available = lambda: False

                # Set all required mock attributes
                for attr_name, mock_value in mock_attributes.items():
                    setattr(mock_module, attr_name, mock_value)

            # Apply the mock
            monkeypatch.setitem(sys.modules, module_name, mock_module)

            return mock_module

        return _injector

    return _create_dependency_injector


# Configure pytest
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "requires_pyarrow: mark test that requires PyArrow"
    )
    config.addinivalue_line(
        "markers", "requires_orjson: mark test that requires orjson"
    )
    config.addinivalue_line(
        "markers", "requires_zstandard: mark test that requires zstandard"
    )
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line(
        "markers", "benchmark: mark test as a performance benchmark"
    )


# Define dependency-specific test skipping
def pytest_runtest_setup(item):
    """Skip tests based on dependency requirements."""
    for marker in item.iter_markers():
        if marker.name == "requires_pyarrow" and not _is_dependency_available(
            "pyarrow"
        ):
            pytest.skip("PyArrow is required for this test")
        elif marker.name == "requires_orjson" and not _is_dependency_available(
            "orjson"
        ):
            pytest.skip("orjson is required for this test")
        elif marker.name == "requires_zstandard" and not _is_dependency_available(
            "zstandard"
        ):
            pytest.skip("zstandard is required for this test")
