"""
Tests for batch processing functionality.

Tests batch processing of records with different configurations and sizes.
"""

from typing import Any, Dict, List

import pytest

from transmog.config import TransmogConfig
from transmog.core.hierarchy import process_record_batch
from transmog.process import Processor


class TestBatchProcessing:
    """Test batch processing functionality."""

    @pytest.fixture
    def sample_records(self) -> List[Dict[str, Any]]:
        """Sample records for batch processing tests."""
        return [
            {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 25, "city": "Los Angeles"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
            {"id": 4, "name": "Diana", "age": 28, "city": "Houston"},
            {"id": 5, "name": "Eve", "age": 32, "city": "Phoenix"},
        ]

    @pytest.fixture
    def nested_records(self) -> List[Dict[str, Any]]:
        """Nested records for batch processing tests."""
        return [
            {
                "id": 1,
                "company": "TechCorp",
                "employees": [
                    {"name": "Alice", "role": "Engineer"},
                    {"name": "Bob", "role": "Designer"},
                ],
                "location": {"city": "San Francisco", "state": "CA"},
            },
            {
                "id": 2,
                "company": "DataCorp",
                "employees": [
                    {"name": "Charlie", "role": "Analyst"},
                    {"name": "Diana", "role": "Manager"},
                ],
                "location": {"city": "Austin", "state": "TX"},
            },
        ]

    def test_batch_processing_basic(self, sample_records):
        """Test basic batch processing functionality."""
        # Create config with batch processing settings
        config = TransmogConfig(batch_size=3)
        processor = Processor(config)

        result = processor.process(sample_records, entity_name="users")

        assert len(result.main_table) == len(sample_records)
        assert result.entity_name == "users"

        # Check that all records were processed
        for i, record in enumerate(sample_records):
            processed_record = result.main_table[i]
            assert processed_record["name"] == record["name"]
            assert processed_record["age"] == str(record["age"])  # Cast to string
            assert processed_record["city"] == record["city"]

    def test_batch_processing_different_sizes(self, sample_records):
        """Test batch processing with different batch sizes."""
        batch_sizes = [1, 2, 3, 5, 10]

        for batch_size in batch_sizes:
            config = TransmogConfig(batch_size=batch_size)
            processor = Processor(config)

            result = processor.process(sample_records, entity_name="users")

            # Results should be the same regardless of batch size
            assert len(result.main_table) == len(sample_records)
            assert result.entity_name == "users"

    def test_batch_processing_memory_efficiency(self, sample_records):
        """Test batch processing memory efficiency."""
        # Small batch size for memory efficiency
        config = TransmogConfig(batch_size=2, cache_enabled=True, cache_maxsize=1000)
        processor = Processor(config)

        result = processor.process(sample_records, entity_name="users")

        assert len(result.main_table) == len(sample_records)
        # Memory optimized should still produce correct results
        assert all("name" in record for record in result.main_table)

    def test_batch_processing_with_arrays(self, nested_records):
        """Test batch processing with nested arrays."""
        config = TransmogConfig(batch_size=1)
        processor = Processor(config)

        result = processor.process(nested_records, entity_name="companies")

        # Should have main table and child tables for employees
        assert len(result.main_table) == len(nested_records)
        assert len(result.child_tables) > 0

        # Check that employees were extracted to child tables
        employee_tables = [
            name for name in result.child_tables.keys() if "employees" in name
        ]
        assert len(employee_tables) > 0

    def test_batch_processing_error_handling(self, sample_records):
        """Test batch processing with error handling."""
        # Add a problematic record
        problematic_records = sample_records + [
            {"id": "invalid", "name": None, "age": "not_a_number"}
        ]

        # Use error tolerant config
        config = TransmogConfig(
            batch_size=2, recovery_strategy="skip", allow_malformed_data=True
        )
        processor = Processor(config)

        result = processor.process(problematic_records, entity_name="users")

        # Should process valid records and handle errors gracefully
        assert len(result.main_table) >= len(sample_records)

    def test_batch_processing_deterministic_results(self, sample_records):
        """Test that batch processing produces deterministic results."""
        config = TransmogConfig(batch_size=2)
        processor = Processor(config)

        # Process the same data multiple times
        result1 = processor.process(sample_records, entity_name="users")
        result2 = processor.process(sample_records, entity_name="users")

        # Results should be identical (excluding generated IDs)
        assert len(result1.main_table) == len(result2.main_table)

        for record1, record2 in zip(result1.main_table, result2.main_table):
            # Compare non-ID fields
            for key in record1.keys():
                if not key.startswith("__transmog"):
                    assert record1[key] == record2[key]


class TestBatchProcessingIntegration:
    """Test batch processing integration with other features."""

    @pytest.fixture
    def large_dataset(self) -> List[Dict[str, Any]]:
        """Large dataset for integration testing."""
        return [
            {
                "id": i,
                "name": f"User_{i}",
                "data": {"value": i * 2, "category": f"cat_{i % 3}"},
                "tags": [f"tag_{i}", f"tag_{i + 1}"],
            }
            for i in range(100)
        ]

    def test_batch_processing_with_file_output(self, large_dataset, tmp_path):
        """Test batch processing with file output."""
        config = TransmogConfig(batch_size=20)
        processor = Processor(config)

        result = processor.process(large_dataset, entity_name="users")

        # Save to files
        output_dir = tmp_path / "batch_output"
        output_paths = result.write_all_json(str(output_dir))

        assert len(output_paths) > 0
        assert all(path.endswith(".json") for path in output_paths.values())

    def test_batch_processing_performance_comparison(self, large_dataset):
        """Test batch processing performance with different configurations."""
        import time

        # Small batch size
        config_small = TransmogConfig(batch_size=10)
        processor_small = Processor(config_small)

        start_time = time.time()
        result_small = processor_small.process(large_dataset, entity_name="users")
        small_batch_time = time.time() - start_time

        # Large batch size
        config_large = TransmogConfig(batch_size=50)
        processor_large = Processor(config_large)

        start_time = time.time()
        result_large = processor_large.process(large_dataset, entity_name="users")
        large_batch_time = time.time() - start_time

        # Both should produce same results
        assert len(result_small.main_table) == len(result_large.main_table)

        # Performance comparison (large batches might be faster)
        # Just ensure both complete in reasonable time
        assert small_batch_time < 10.0  # seconds
        assert large_batch_time < 10.0  # seconds


class TestBatchProcessingEdgeCases:
    """Test edge cases in batch processing."""

    def test_batch_processing_very_small_batches(self):
        """Test batch processing with very small batch sizes."""
        records = [{"id": i, "value": f"item_{i}"} for i in range(10)]

        config = TransmogConfig(batch_size=1)
        processor = Processor(config)

        result = processor.process(records, entity_name="items")

        assert len(result.main_table) == len(records)

    def test_batch_processing_very_large_batches(self):
        """Test batch processing with very large batch sizes."""
        records = [{"id": i, "value": f"item_{i}"} for i in range(10)]

        # Batch size larger than dataset
        config = TransmogConfig(batch_size=100)
        processor = Processor(config)

        result = processor.process(records, entity_name="items")

        assert len(result.main_table) == len(records)

    def test_batch_processing_with_complex_nesting(self):
        """Test batch processing with complex nested structures."""
        complex_records = [
            {
                "id": i,
                "level1": {
                    "level2": {
                        "level3": {
                            "value": i,
                            "items": [{"item": f"item_{j}"} for j in range(3)],
                        }
                    }
                },
            }
            for i in range(5)
        ]

        config = TransmogConfig(batch_size=2)
        processor = Processor(config)

        result = processor.process(complex_records, entity_name="complex")

        assert len(result.main_table) == len(complex_records)
        assert len(result.child_tables) > 0

    def test_batch_processing_memory_stress(self):
        """Test batch processing under memory stress conditions."""
        # Create a large number of records with nested data
        stress_records = [
            {
                "id": i,
                "data": {f"field_{j}": f"value_{i}_{j}" for j in range(50)},
                "arrays": [[f"item_{i}_{j}_{k}" for k in range(10)] for j in range(5)],
            }
            for i in range(20)
        ]

        # Use memory optimized config with small batches
        config = TransmogConfig(batch_size=5, cache_enabled=True, cache_maxsize=1000)
        processor = Processor(config)

        result = processor.process(stress_records, entity_name="stress_test")

        # Should complete without memory errors
        assert len(result.main_table) == len(stress_records)
        assert len(result.child_tables) > 0
