"""
Performance tests for processing speed.

Tests processing speed benchmarks and performance optimization.
"""

import json
import tempfile
import time
from pathlib import Path

import pytest

import transmog as tm


class TestProcessingSpeedBenchmarks:
    """Test processing speed benchmarks."""

    def test_small_dataset_performance(self, batch_data):
        """Test performance with small dataset (10 records)."""
        start_time = time.time()

        result = tm.flatten(batch_data, name="small_perf")

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == len(batch_data)

        # Should process small dataset quickly (< 1 second)
        assert processing_time < 1.0
        print(f"Small dataset ({len(batch_data)} records): {processing_time:.4f}s")

    def test_medium_dataset_performance(self):
        """Test performance with medium dataset (1000 records)."""
        # Create medium dataset
        medium_data = [
            {
                "id": i,
                "name": f"Record {i}",
                "data": {
                    "value": i * 10,
                    "category": f"cat_{i % 10}",
                    "active": i % 2 == 0,
                },
                "tags": [f"tag_{i}", f"type_{i % 5}"],
            }
            for i in range(1000)
        ]

        start_time = time.time()

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
        result = tm.flatten(medium_data, name="medium_perf", config=config)

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == 1000

        # Should process medium dataset reasonably quickly (< 10 seconds)
        assert processing_time < 10.0
        print(f"Medium dataset (1000 records): {processing_time:.4f}s")
        print(f"Rate: {1000 / processing_time:.1f} records/second")

    def test_large_dataset_performance(self):
        """Test performance with large dataset (10000 records)."""
        # Create large dataset
        large_data = [
            {
                "id": i,
                "name": f"Record {i}",
                "metadata": {
                    "created": f"2023-01-{(i % 28) + 1:02d}",
                    "type": f"type_{i % 20}",
                    "priority": i % 5,
                },
            }
            for i in range(10000)
        ]

        start_time = time.time()

        result = tm.flatten(large_data, name="large_perf")

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == 10000

        # Should process large dataset within reasonable time (< 60 seconds)
        assert processing_time < 60.0
        print(f"Large dataset (10000 records): {processing_time:.4f}s")
        print(f"Rate: {10000 / processing_time:.1f} records/second")

    def test_complex_nested_performance(self):
        """Test performance with complex nested structures."""
        # Create complex nested data
        complex_data = [
            {
                "id": i,
                "organization": {
                    "id": f"org_{i % 10}",
                    "departments": [
                        {
                            "id": f"dept_{j}",
                            "name": f"Department {j}",
                            "employees": [
                                {
                                    "id": f"emp_{i}_{j}_{k}",
                                    "name": f"Employee {k}",
                                    "skills": [
                                        f"skill_{k}_{skill_idx}"
                                        for skill_idx in range(3)
                                    ],
                                }
                                for k in range(5)
                            ],
                        }
                        for j in range(3)
                    ],
                },
            }
            for i in range(100)
        ]

        start_time = time.time()

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
        result = tm.flatten(complex_data, name="complex_perf", config=config)

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == 100
        assert len(result.tables) > 0  # Should have child tables

        # Complex nested structures take longer but should be reasonable
        assert processing_time < 30.0
        print(f"Complex nested (100 records): {processing_time:.4f}s")

    def test_wide_dataset_performance(self):
        """Test performance with wide dataset (many fields)."""
        # Create dataset with many fields
        wide_data = [
            {
                **{f"field_{j}": f"value_{i}_{j}" for j in range(100)},
                "id": i,
                "name": f"Wide Record {i}",
            }
            for i in range(1000)
        ]

        start_time = time.time()

        result = tm.flatten(wide_data, name="wide_perf")

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == 1000

        # Wide datasets should still process efficiently
        assert processing_time < 15.0
        print(f"Wide dataset (1000 records, 100+ fields): {processing_time:.4f}s")

    def test_array_heavy_performance(self):
        """Test performance with array-heavy data."""
        # Create data with many arrays
        array_heavy_data = [
            {
                "id": i,
                "name": f"Array Record {i}",
                "tags": [f"tag_{j}" for j in range(20)],
                "items": [
                    {"item_id": f"item_{i}_{j}", "values": [k for k in range(10)]}
                    for j in range(10)
                ],
                "categories": [f"cat_{k}" for k in range(15)],
            }
            for i in range(500)
        ]

        start_time = time.time()

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
        result = tm.flatten(array_heavy_data, name="array_perf", config=config)

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == 500
        assert len(result.tables) > 0

        # Array processing takes more time but should be reasonable
        assert processing_time < 45.0
        print(f"Array-heavy dataset (500 records): {processing_time:.4f}s")


class TestFileProcessingPerformance:
    """Test file processing performance."""

    def test_json_file_processing_performance(self, large_json_file):
        """Test JSON file processing performance."""
        start_time = time.time()

        result = tm.flatten_file(large_json_file, name="file_perf")

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == 1000  # From large_json_file fixture

        # File processing should be efficient
        assert processing_time < 15.0
        print(f"JSON file processing (1000 records): {processing_time:.4f}s")

    def test_streaming_performance(self):
        """Test streaming processing performance."""
        # Create large dataset for streaming
        large_streaming_data = [
            {
                "id": i,
                "data": f"streaming_record_{i}",
                "nested": {"value": i * 2, "category": f"stream_cat_{i % 20}"},
            }
            for i in range(2000)
        ]

        start_time = time.time()

        # Use memory-efficient processing with small batch size
        config = tm.TransmogConfig(
            cache_size=1000,
            batch_size=100,
        )
        result = tm.flatten(
            large_streaming_data,
            name="stream_perf",
            config=config,
        )

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == 2000

        # Streaming should be memory-efficient and reasonably fast
        assert processing_time < 25.0
        print(f"Streaming processing (2000 records): {processing_time:.4f}s")


class TestMemoryPerformance:
    """Test memory usage performance."""

    def test_memory_efficient_processing(self):
        """Test memory-efficient processing mode."""
        # Create data that could use significant memory
        memory_test_data = [
            {
                "id": i,
                "large_text": "x" * 1000,  # 1KB text per record
                "data": {
                    "nested_large": "y" * 500,
                    "more_data": [f"item_{j}" * 10 for j in range(20)],
                },
            }
            for i in range(1000)
        ]

        start_time = time.time()

        # Use memory-efficient processing
        config = tm.TransmogConfig(
            cache_size=1000,
            batch_size=50,
        )
        result = tm.flatten(
            memory_test_data,
            name="memory_perf",
            config=config,
        )

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == 1000

        # Memory-efficient mode might be slower but should complete
        print(f"Memory-efficient processing (1000 records): {processing_time:.4f}s")

    def test_batch_processing_performance(self):
        """Test batch processing performance with different batch sizes."""
        test_data = [
            {"id": i, "name": f"Batch Record {i}", "value": i * 5} for i in range(2000)
        ]

        batch_sizes = [10, 50, 100, 500, 1000]
        results = {}

        for batch_size in batch_sizes:
            start_time = time.time()

            config = tm.TransmogConfig(batch_size=batch_size)
            result = tm.flatten(test_data, name=f"batch_{batch_size}", config=config)

            end_time = time.time()
            processing_time = end_time - start_time

            assert result is not None
            assert len(result.main) == 2000

            results[batch_size] = processing_time
            print(f"Batch size {batch_size}: {processing_time:.4f}s")

        # Verify all batch sizes completed successfully
        assert len(results) == len(batch_sizes)


class TestPerformanceComparisons:
    """Test performance comparisons between different approaches."""

    def test_array_handling_performance_comparison(self):
        """Test performance comparison of different array handling modes."""
        array_data = [
            {
                "id": i,
                "name": f"Array Test {i}",
                "items": [{"item_id": j, "value": f"item_{i}_{j}"} for j in range(10)],
                "tags": [f"tag_{k}" for k in range(5)],
            }
            for i in range(500)
        ]

        mode_map = {
            "separate": tm.ArrayMode.SEPARATE,
            "inline": tm.ArrayMode.INLINE,
            "skip": tm.ArrayMode.SKIP,
        }
        results = {}

        for mode_name, mode_value in mode_map.items():
            start_time = time.time()

            config = tm.TransmogConfig(array_mode=mode_value)
            result = tm.flatten(array_data, name=f"array_{mode_name}", config=config)

            end_time = time.time()
            processing_time = end_time - start_time

            assert result is not None
            assert len(result.main) == 500

            results[mode_name] = processing_time
            print(f"Array mode '{mode_name}': {processing_time:.4f}s")

        # All modes should complete successfully
        assert len(results) == len(mode_map)

    def test_error_handling_performance_impact(self):
        """Test performance impact of different error handling modes."""
        # Create data with some problematic records
        mixed_data = []
        for i in range(1000):
            if i % 100 == 0:  # 1% problematic records
                mixed_data.append({"id": None, "name": f"Problem {i}"})
            else:
                mixed_data.append({"id": i, "name": f"Good Record {i}"})

        error_modes = ["skip", "warn", "strict"]
        results = {}

        for mode in error_modes:
            start_time = time.time()

            try:
                result = tm.flatten(mixed_data, name=f"error_{mode}", errors=mode)

                end_time = time.time()
                processing_time = end_time - start_time

                results[mode] = processing_time
                print(f"Error mode '{mode}': {processing_time:.4f}s")

                if result:
                    print(f"  Processed {len(result.main)} records")

            except Exception as e:
                end_time = time.time()
                processing_time = end_time - start_time
                results[mode] = processing_time
                print(f"Error mode '{mode}': {processing_time:.4f}s (failed: {e})")

        # At least some modes should complete
        assert len(results) > 0

    def test_configuration_performance_impact(self):
        """Test performance impact of different configuration options."""
        test_data = [
            {"id": i, "nested": {"level2": {"level3": {"value": f"deep_{i}"}}}}
            for i in range(1000)
        ]

        configs = [
            {"name": "default", "config": tm.TransmogConfig()},
            {
                "name": "preserve_types",
                "config": tm.TransmogConfig(cast_to_string=False),
            },
            {"name": "custom_separator", "config": tm.TransmogConfig(separator=".")},
            {"name": "low_threshold", "config": tm.TransmogConfig(nested_threshold=2)},
            {
                "name": "high_threshold",
                "config": tm.TransmogConfig(nested_threshold=10),
            },
        ]

        results = {}

        for config_dict in configs:
            start_time = time.time()

            result = tm.flatten(
                test_data, name=config_dict["name"], config=config_dict["config"]
            )

            end_time = time.time()
            processing_time = end_time - start_time

            assert result is not None
            assert len(result.main) == 1000

            results[config_dict["name"]] = processing_time
            print(f"Config '{config_dict['name']}': {processing_time:.4f}s")

        # All configurations should complete
        assert len(results) == len(configs)
