"""
Tests for processor strategy interface conformance.

This module defines an abstract test class for testing processor strategy implementations.
"""

import json

import pytest

from transmog import TransmogConfig
from transmog.process import (
    BatchStrategy,
    ChunkedStrategy,
    CSVStrategy,
    FileStrategy,
    InMemoryStrategy,
    ProcessingStrategy,
)


class TestStrategyInterface:
    """Test that processor strategies conform to the required interface."""

    def test_strategy_interface(self):
        """Test that ProcessingStrategy implements the required interface."""
        # Check base strategy class interface
        assert hasattr(ProcessingStrategy, "process"), (
            "ProcessingStrategy is missing process method"
        )

        # Check concrete strategies implement the interface
        for strategy_class in [
            InMemoryStrategy,
            FileStrategy,
            BatchStrategy,
            ChunkedStrategy,
            CSVStrategy,
        ]:
            assert hasattr(strategy_class, "process"), (
                f"{strategy_class.__name__} is missing process method"
            )

            # Create an instance with minimal config
            strategy = strategy_class(TransmogConfig.default())
            assert callable(strategy.process), (
                f"{strategy_class.__name__}.process must be callable"
            )


class AbstractStrategyTest:
    """
    Abstract base class for processor strategy tests.

    This class defines a standardized set of tests that should apply to all strategy implementations.
    Subclasses must implement appropriate fixtures if needed.
    """

    # Strategy class to test - should be set by subclasses
    strategy_class = None

    @pytest.fixture
    def config(self):
        """Create a standard configuration."""
        return TransmogConfig.default().with_processing(cast_to_string=True)

    @pytest.fixture
    def strategy(self, config):
        """Create a strategy instance."""
        if self.strategy_class is None:
            pytest.fail("Subclass must define strategy_class")
        return self.strategy_class(config)

    @pytest.fixture
    def simple_data(self):
        """Create a simple data structure."""
        return {
            "id": "123",
            "name": "Test Entity",
            "addr": {"street": "123 Main St", "city": "Anytown", "zip": "12345"},
        }

    @pytest.fixture
    def batch_data(self):
        """Create a batch of simple records."""
        return [
            {"id": f"record{i}", "name": f"Record {i}", "value": i * 10}
            for i in range(10)
        ]

    @pytest.fixture
    def json_file(self, tmp_path, batch_data):
        """Create a temporary JSON file with test data."""
        file_path = tmp_path / "test_data.json"
        with open(file_path, "w") as f:
            json.dump(batch_data, f)
        return str(file_path)

    @pytest.fixture
    def csv_file(self, tmp_path):
        """Create a temporary CSV file with test data."""
        file_path = tmp_path / "test_data.csv"
        with open(file_path, "w") as f:
            f.write("id,name,value\n")
            for i in range(10):
                f.write(f"record{i},Record {i},{i * 10}\n")
        return str(file_path)

    @pytest.fixture
    def complex_data(self):
        """Create a complex data structure with nested arrays."""
        return {
            "id": "456",
            "name": "Complex Entity",
            "items": [
                {"id": "item1", "name": "Item 1", "value": 100},
                {"id": "item2", "name": "Item 2", "value": 200},
                {"id": "item3", "name": "Item 3", "value": 300},
            ],
        }

    @pytest.fixture
    def complex_batch(self):
        """Create a batch of complex data."""
        return [
            {
                "id": f"entity{i}",
                "name": f"Entity {i}",
                "items": [
                    {"id": f"item{i}1", "name": f"Item {i}.1", "value": i * 100 + 1},
                    {"id": f"item{i}2", "name": f"Item {i}.2", "value": i * 100 + 2},
                ],
            }
            for i in range(5)
        ]

    def test_strategy_process_result_type(self, strategy):
        """Test that strategy process method returns a ProcessingResult."""
        # Skip test method if not implemented by concrete class
        pytest.skip("This test must be implemented by concrete strategy test classes")

    def test_strategy_handles_entity_name(self, strategy):
        """Test that strategy handles entity_name parameter."""
        # Skip test method if not implemented by concrete class
        pytest.skip("This test must be implemented by concrete strategy test classes")

    def test_strategy_preserves_data_structure(self, strategy):
        """Test that strategy preserves the original data structure."""
        # Skip test method if not implemented by concrete class
        pytest.skip("This test must be implemented by concrete strategy test classes")
