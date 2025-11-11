"""
Tests for batch processing functionality.

Tests batch processing of records with different configurations and sizes.
"""

from typing import Any

import pytest

import transmog as tm
from transmog.config import TransmogConfig
from transmog.core.hierarchy import process_record_batch
from transmog.types import RecoveryMode


class TestBatchProcessing:
    """Test batch processing functionality."""

    @pytest.fixture
    def sample_records(self) -> list[dict[str, Any]]:
        """Sample records for batch processing tests."""
        return [
            {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 25, "city": "Los Angeles"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
            {"id": 4, "name": "Diana", "age": 28, "city": "Houston"},
            {"id": 5, "name": "Eve", "age": 32, "city": "Phoenix"},
        ]

    @pytest.fixture
    def nested_records(self) -> list[dict[str, Any]]:
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
        result = tm.flatten(sample_records, name="users", config=config)

        assert len(result.main) == len(sample_records)

        for i, record in enumerate(sample_records):
            processed_record = result.main[i]
            assert processed_record["name"] == record["name"]
            assert processed_record["age"] == record["age"]
            assert processed_record["city"] == record["city"]

    def test_batch_processing_different_sizes(self, sample_records):
        """Test batch processing with different batch sizes."""
        batch_sizes = [1, 2, 3, 5, 10]

        for batch_size in batch_sizes:
            config = TransmogConfig(batch_size=batch_size)
            result = tm.flatten(sample_records, name="users", config=config)

            # Results should be the same regardless of batch size
            assert len(result.main) == len(sample_records)

    def test_batch_processing_memory_efficiency(self, sample_records):
        """Test batch processing memory efficiency."""
        # Small batch size for memory efficiency
        config = TransmogConfig(batch_size=2)
        result = tm.flatten(sample_records, name="users", config=config)

        assert len(result.main) == len(sample_records)
        # Memory optimized should still produce correct results
        assert all("name" in record for record in result.main)

    def test_batch_processing_with_arrays(self, nested_records):
        """Test batch processing with nested arrays."""
        config = TransmogConfig(batch_size=1)
        result = tm.flatten(nested_records, name="companies", config=config)

        # Should have main table and child tables for employees
        assert len(result.main) == len(nested_records)
        assert len(result.tables) > 0

        # Check that employees were extracted to child tables
        employee_tables = [name for name in result.tables.keys() if "employees" in name]
        assert len(employee_tables) > 0

    def test_batch_processing_error_handling(self, sample_records):
        """Test batch processing with error handling."""
        # Add a problematic record
        problematic_records = sample_records + [
            {"id": "invalid", "name": None, "age": "not_a_number"}
        ]

        # Use error tolerant config
        config = TransmogConfig(batch_size=2, recovery_mode=RecoveryMode.SKIP)

        result = tm.flatten(problematic_records, name="users", config=config)

        # Should process valid records and handle errors gracefully
        assert len(result.main) >= len(sample_records)

    def test_batch_processing_deterministic_results(self, sample_records):
        """Test that batch processing produces deterministic results."""
        config = TransmogConfig(batch_size=2)

        # Process the same data multiple times
        result1 = tm.flatten(sample_records, name="users", config=config)
        result2 = tm.flatten(sample_records, name="users", config=config)

        # Results should be identical (excluding generated IDs)
        assert len(result1.main) == len(result2.main)

        metadata_fields = {
            config.id_field,
            config.parent_field,
        }
        if config.time_field:
            metadata_fields.add(config.time_field)

        for record1, record2 in zip(result1.main, result2.main):
            # Compare non-ID fields
            for key in record1.keys():
                if key in metadata_fields or key.startswith("__transmog"):
                    continue
                assert record1[key] == record2[key]


class TestBatchProcessingIntegration:
    """Test batch processing integration with other features."""

    @pytest.fixture
    def large_dataset(self) -> list[dict[str, Any]]:
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

        result = tm.flatten(large_dataset, name="users", config=config)

        output_dir = tmp_path / "batch_output"
        output_paths = result.save(str(output_dir), output_format="csv")

        if isinstance(output_paths, dict):
            saved_files = list(output_paths.values())
        else:
            saved_files = output_paths

        assert len(saved_files) > 0
        assert all(path.endswith(".csv") for path in saved_files)

    def test_batch_processing_performance_comparison(self, large_dataset):
        """Test batch processing performance with different configurations."""
        import time

        # Small batch size
        config_small = TransmogConfig(batch_size=10)

        start_time = time.time()
        result_small = tm.flatten(large_dataset, name="users", config=config_small)
        small_batch_time = time.time() - start_time

        # Large batch size
        config_large = TransmogConfig(batch_size=50)

        start_time = time.time()
        result_large = tm.flatten(large_dataset, name="users", config=config_large)
        large_batch_time = time.time() - start_time

        # Both should produce same results
        assert len(result_small.main) == len(result_large.main)

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

        result = tm.flatten(records, name="items", config=config)

        assert len(result.main) == len(records)

    def test_batch_processing_very_large_batches(self):
        """Test batch processing with very large batch sizes."""
        records = [{"id": i, "value": f"item_{i}"} for i in range(10)]

        # Batch size larger than dataset
        config = TransmogConfig(batch_size=100)

        result = tm.flatten(records, name="items", config=config)

        assert len(result.main) == len(records)

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

        result = tm.flatten(complex_records, name="complex", config=config)

        assert len(result.main) == len(complex_records)
        assert len(result.tables) > 0

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
        config = TransmogConfig(batch_size=5)

        result = tm.flatten(stress_records, name="stress_test", config=config)

        # Should complete without memory errors
        assert len(result.main) == len(stress_records)
        assert len(result.tables) > 0
