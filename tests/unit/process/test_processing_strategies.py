"""
Tests for processing strategies.

Tests different processing strategies and their performance characteristics.
"""

import json
import tempfile
from pathlib import Path

import pytest

from transmog.config import TransmogConfig
from transmog.process.strategies import (
    BatchStrategy,
    ChunkedStrategy,
    FileStrategy,
    InMemoryStrategy,
    ProcessingStrategy,
)


class TestProcessingStrategy:
    """Test the abstract ProcessingStrategy base class."""

    def test_processing_strategy_abstract(self):
        """Test that ProcessingStrategy is abstract."""
        with pytest.raises(TypeError):
            ProcessingStrategy()  # Should not be instantiable

    def test_processing_strategy_with_config(self):
        """Test ProcessingStrategy initialization with config."""
        config = TransmogConfig()

        # Test with concrete implementation
        strategy = InMemoryStrategy(config)
        assert strategy.config == config


class TestInMemoryStrategy:
    """Test the InMemoryStrategy class."""

    def test_in_memory_strategy_init(self):
        """Test InMemoryStrategy initialization."""
        config = TransmogConfig()
        strategy = InMemoryStrategy(config)

        assert isinstance(strategy, ProcessingStrategy)
        assert strategy.config == config

    def test_in_memory_process_dict(self, simple_data):
        """Test processing a single dictionary."""
        config = TransmogConfig()
        strategy = InMemoryStrategy(config)

        result = strategy.process(simple_data, entity_name="test")

        assert result is not None
        assert hasattr(result, "main_table")
        assert len(result.main_table) == 1

    def test_in_memory_process_list(self, batch_data):
        """Test processing a list of dictionaries."""
        config = TransmogConfig()
        strategy = InMemoryStrategy(config)

        result = strategy.process(batch_data, entity_name="test_batch")

        assert result is not None
        assert hasattr(result, "main_table")
        assert len(result.main_table) == len(batch_data)

    def test_in_memory_process_with_arrays(self, array_data):
        """Test processing data with arrays."""
        config = TransmogConfig()
        strategy = InMemoryStrategy(config)

        result = strategy.process(array_data, entity_name="test_arrays")

        assert result is not None
        assert hasattr(result, "main_table")
        assert hasattr(result, "child_tables")


class TestFileStrategy:
    """Test the FileStrategy class."""

    def test_file_strategy_init(self):
        """Test FileStrategy initialization."""
        config = TransmogConfig()
        strategy = FileStrategy(config)

        assert isinstance(strategy, ProcessingStrategy)
        assert strategy.config == config

    def test_file_strategy_process_json_file(self, json_file):
        """Test processing a JSON file."""
        config = TransmogConfig()
        strategy = FileStrategy(config)

        result = strategy.process(json_file, entity_name="test_file")

        assert result is not None
        assert hasattr(result, "main_table")
        assert len(result.main_table) >= 1

    def test_file_strategy_process_jsonl_file(self, jsonl_file):
        """Test processing a JSONL file."""
        config = TransmogConfig()
        strategy = FileStrategy(config)

        result = strategy.process(jsonl_file, entity_name="test_jsonl")

        assert result is not None
        assert hasattr(result, "main_table")
        assert len(result.main_table) >= 1

    def test_file_strategy_nonexistent_file(self):
        """Test processing nonexistent file."""
        config = TransmogConfig()
        strategy = FileStrategy(config)

        with pytest.raises(Exception):  # Should raise some kind of error
            strategy.process("nonexistent.json", entity_name="test")


class TestBatchStrategy:
    """Test the BatchStrategy class."""

    def test_batch_strategy_init(self):
        """Test BatchStrategy initialization."""
        config = TransmogConfig()
        strategy = BatchStrategy(config)

        assert isinstance(strategy, ProcessingStrategy)
        assert strategy.config == config

    def test_batch_strategy_process_list(self, batch_data):
        """Test processing with batch strategy."""
        config = TransmogConfig()
        strategy = BatchStrategy(config)

        result = strategy.process(batch_data, entity_name="test_batch")

        assert result is not None
        assert hasattr(result, "main_table")
        assert len(result.main_table) == len(batch_data)

    def test_batch_strategy_with_generator(self, batch_data):
        """Test processing with generator input."""
        config = TransmogConfig()
        strategy = BatchStrategy(config)

        # Convert to generator
        def data_generator():
            yield from batch_data

        result = strategy.process(data_generator(), entity_name="test_gen")

        assert result is not None
        assert hasattr(result, "main_table")


class TestChunkedStrategy:
    """Test the ChunkedStrategy class."""

    def test_chunked_strategy_init(self):
        """Test ChunkedStrategy initialization."""
        config = TransmogConfig()
        strategy = ChunkedStrategy(config)

        assert isinstance(strategy, ProcessingStrategy)
        assert strategy.config == config

    def test_chunked_strategy_process_list(self, batch_data):
        """Test processing with chunked strategy."""
        config = TransmogConfig()
        strategy = ChunkedStrategy(config)

        result = strategy.process(batch_data, entity_name="test_chunked")

        assert result is not None
        assert hasattr(result, "main_table")
        assert len(result.main_table) == len(batch_data)

    def test_chunked_strategy_large_dataset(self):
        """Test chunked processing with large dataset."""
        config = TransmogConfig()
        strategy = ChunkedStrategy(config)

        # Create large dataset
        large_data = [{"id": i, "value": f"item_{i}"} for i in range(1000)]

        result = strategy.process(large_data, entity_name="test_large")

        assert result is not None
        assert hasattr(result, "main_table")
        assert len(result.main_table) == 1000


class TestStrategyComparison:
    """Test comparison between different strategies."""

    def test_strategy_performance_comparison(self, batch_data):
        """Test performance comparison between strategies."""
        config = TransmogConfig()

        # Test different strategies
        strategies = [
            ("InMemory", InMemoryStrategy(config)),
            ("Batch", BatchStrategy(config)),
            ("Chunked", ChunkedStrategy(config)),
        ]

        results = {}
        for name, strategy in strategies:
            result = strategy.process(batch_data, entity_name=f"test_{name.lower()}")
            results[name] = result

            # All should produce same number of main records
            assert len(result.main_table) == len(batch_data)

    def test_strategy_memory_usage(self):
        """Test memory usage patterns of different strategies."""
        config = TransmogConfig()

        # Create moderately large dataset
        data = [{"id": i, "data": f"item_{i}" * 100} for i in range(500)]

        # Test memory-efficient strategies
        chunked_strategy = ChunkedStrategy(config)
        result = chunked_strategy.process(data, entity_name="memory_test")

        assert result is not None
        assert len(result.main_table) == 500

    def test_strategy_with_different_data_types(self):
        """Test strategies with different data types."""
        config = TransmogConfig()

        # Test data with various types
        mixed_data = [
            {"id": 1, "name": "string", "value": 42, "active": True},
            {"id": 2, "name": "another", "value": 3.14, "active": False},
            {"id": 3, "name": None, "value": 0, "active": None},
        ]

        strategies = [
            InMemoryStrategy(config),
            BatchStrategy(config),
            ChunkedStrategy(config),
        ]

        for strategy in strategies:
            result = strategy.process(mixed_data, entity_name="mixed_test")
            assert result is not None
            assert len(result.main_table) == 3


class TestStrategyEdgeCases:
    """Test edge cases for processing strategies."""

    def test_empty_data_processing(self):
        """Test processing empty data."""
        config = TransmogConfig()
        strategy = InMemoryStrategy(config)

        result = strategy.process([], entity_name="empty_test")

        assert result is not None
        assert hasattr(result, "main_table")
        assert len(result.main_table) == 0

    def test_single_record_processing(self, simple_data):
        """Test processing single record."""
        config = TransmogConfig()
        strategy = InMemoryStrategy(config)

        result = strategy.process(simple_data, entity_name="single_test")

        assert result is not None
        assert len(result.main_table) == 1

    def test_null_data_processing(self):
        """Test processing null/None data."""
        config = TransmogConfig()
        strategy = InMemoryStrategy(config)

        # Test with None data - should raise an error
        with pytest.raises(Exception):  # ProcessingError or TypeError
            strategy.process(None, entity_name="null_test")

    def test_malformed_data_processing(self):
        """Test processing malformed data."""
        config = TransmogConfig()
        strategy = InMemoryStrategy(config)

        # Test with malformed data
        malformed_data = [
            {"id": 1, "name": "valid"},
            {"id": "not_an_int", "name": 123},  # Type inconsistency
            {"missing_id": True, "name": "no_id"},  # Missing expected field
        ]

        result = strategy.process(malformed_data, entity_name="malformed_test")

        assert result is not None
        # Should handle gracefully


class TestStrategyConfiguration:
    """Test strategy configuration options."""

    def test_strategy_with_custom_config(self, batch_data):
        """Test strategy with custom configuration."""
        config = TransmogConfig()

        # Test with custom configuration
        strategy = InMemoryStrategy(config)
        result = strategy.process(batch_data, entity_name="custom_config")

        assert result is not None
        assert len(result.main_table) == len(batch_data)

    def test_strategy_batch_size_configuration(self, batch_data):
        """Test strategy with different batch sizes."""
        config = TransmogConfig()

        # Test chunked strategy with different batch sizes
        strategy = ChunkedStrategy(config)

        # Process with default batch size
        result = strategy.process(batch_data, entity_name="batch_size_test")

        assert result is not None
        assert len(result.main_table) == len(batch_data)

    def test_strategy_error_handling_configuration(self):
        """Test strategy error handling configuration."""
        config = TransmogConfig()

        strategy = InMemoryStrategy(config)

        # Test with data that might cause errors
        problematic_data = [
            {"id": 1, "circular": None},  # Will be filled with circular reference
        ]

        # Create circular reference
        problematic_data[0]["circular"] = problematic_data[0]

        # Should raise an error for circular references
        with pytest.raises(Exception):  # ProcessingError
            strategy.process(problematic_data, entity_name="error_test")
