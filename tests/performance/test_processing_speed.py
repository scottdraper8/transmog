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

    @pytest.mark.parametrize(
        "size,threshold,description",
        [
            (10, 1.0, "small"),
            (1000, 10.0, "medium"),
            (10000, 60.0, "large"),
        ],
    )
    def test_dataset_performance_by_size(self, size, threshold, description):
        """Test performance with datasets of various sizes."""
        # Create dataset of specified size
        test_data = [
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
            for i in range(size)
        ]

        start_time = time.time()

        config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
        result = tm.flatten(test_data, name=f"{description}_perf", config=config)

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == size

        # Should process within threshold
        assert processing_time < threshold, (
            f"{description.capitalize()} dataset ({size} records) took "
            f"{processing_time:.4f}s, expected < {threshold}s"
        )

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

        result = tm.flatten(large_json_file, name="file_perf")

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
        config = tm.TransmogConfig(batch_size=100)
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


class TestPerformanceComparisons:
    """Test performance comparisons between different approaches."""

    @pytest.mark.parametrize(
        "mode_name,mode_value",
        [
            ("separate", tm.ArrayMode.SEPARATE),
            ("inline", tm.ArrayMode.INLINE),
            ("skip", tm.ArrayMode.SKIP),
        ],
    )
    def test_array_handling_performance_by_mode(self, mode_name, mode_value):
        """Test performance of different array handling modes."""
        array_data = [
            {
                "id": i,
                "name": f"Array Test {i}",
                "items": [{"item_id": j, "value": f"item_{i}_{j}"} for j in range(10)],
                "tags": [f"tag_{k}" for k in range(5)],
            }
            for i in range(500)
        ]

        start_time = time.time()

        config = tm.TransmogConfig(array_mode=mode_value)
        result = tm.flatten(array_data, name=f"array_{mode_name}", config=config)

        end_time = time.time()
        processing_time = end_time - start_time

        assert result is not None
        assert len(result.main) == 500

        # Verify mode-specific behavior
        if mode_name == "separate":
            assert len(result.tables) > 0, "SEPARATE mode should create child tables"
        elif mode_name == "skip":
            assert len(result.tables) == 0, "SKIP mode should not create child tables"
            assert "items" not in result.main[0], "SKIP mode should not include arrays"

        # Should complete within reasonable time
        assert processing_time < 30.0

    def test_configuration_performance_impact(self):
        """Test performance impact of different configuration options."""
        test_data = [
            {"id": i, "nested": {"level2": {"level3": {"value": f"deep_{i}"}}}}
            for i in range(1000)
        ]

        configs = [
            {"name": "default", "config": tm.TransmogConfig()},
            {
                "name": "include_nulls",
                "config": tm.TransmogConfig(include_nulls=True),
            },
            {"name": "small_batch", "config": tm.TransmogConfig(batch_size=100)},
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
