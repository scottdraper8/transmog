"""
Benchmarks for caching effectiveness.

This module contains benchmark tests to measure the performance impact
of caching on various operations in the Transmog package.
"""

from typing import Any

import pytest

from transmog import Processor, TransmogConfig
from transmog.config import settings
from transmog.core.flattener import clear_caches, flatten_json, refresh_cache_config


def generate_data_with_repeated_values(
    num_records: int = 100, repetition_factor: float = 0.8
) -> list[dict[str, Any]]:
    """
    Generate test data with controlled value repetition.

    Args:
        num_records: Number of records to generate
        repetition_factor: Factor controlling how often values repeat (0-1)
                           Higher values mean more repetition

    Returns:
        List of records with controlled value repetition
    """
    data = []

    # Calculate how many unique values to generate based on repetition factor
    # Lower repetition_factor means more unique values
    unique_values = max(1, int(num_records * (1 - repetition_factor)))

    # Generate unique values pool
    value_pool = {
        f"field_{i}": f"value_{i % unique_values}" for i in range(num_records * 5)
    }

    # Generate records using values from the pool
    for i in range(num_records):
        record = {
            "id": f"record_{i}",
            "metadata": {
                "created": "2023-01-01",
                "status": i % 5,  # Limited set of statuses for repetition
                "tags": ["tag_" + str(i % 10) for j in range(3)],  # Limited set of tags
            },
            "details": {},
        }

        # Add repeated fields with values from the pool
        for j in range(20):  # 20 fields per record
            field_name = f"field_{j}"
            record["details"][field_name] = value_pool[field_name]

        data.append(record)

    return data


def generate_data_with_unique_values(num_records: int = 100) -> list[dict[str, Any]]:
    """
    Generate test data with mostly unique values.

    Args:
        num_records: Number of records to generate

    Returns:
        List of records with unique values
    """
    data = []

    # Generate records with unique values
    for i in range(num_records):
        record = {
            "id": f"record_{i}",
            "metadata": {
                "created": f"2023-01-{i % 30 + 1:02d}",  # Different dates
                "status": f"status_{i}",  # Unique statuses
                "tags": [f"tag_{i}_{j}" for j in range(3)],  # Unique tags
            },
            "details": {},
        }

        # Add fields with unique values
        for j in range(20):  # 20 fields per record
            record["details"][f"field_{j}"] = f"unique_value_{i}_{j}"

        data.append(record)

    return data


@pytest.fixture(scope="function")
def reset_caches():
    """Reset caches before and after tests."""
    clear_caches()
    # Store original settings
    original_cache_enabled = getattr(settings, "cache_enabled", True)
    original_cache_maxsize = getattr(settings, "cache_maxsize", 10000)

    yield

    # Restore original settings
    settings.cache_enabled = original_cache_enabled
    settings.cache_maxsize = original_cache_maxsize
    clear_caches()
    refresh_cache_config()


@pytest.mark.benchmark
class TestCachingBenchmarks:
    """Benchmark tests for caching effectiveness."""

    @pytest.mark.parametrize("cache_enabled", [True, False])
    def test_flatten_cache_impact_repeated_values(
        self, cache_enabled, benchmark, reset_caches
    ):
        """Benchmark the impact of caching with repeated values."""
        # Set cache configuration
        settings.cache_enabled = cache_enabled
        refresh_cache_config()

        # Generate data with high value repetition
        data = generate_data_with_repeated_values(num_records=50, repetition_factor=0.9)

        # Function to benchmark
        def process_all():
            results = []
            for record in data:
                result = flatten_json(
                    record, separator="_", cast_to_string=True, skip_arrays=True
                )
                results.append(result)
            return results

        # Benchmark
        benchmark.group = "Caching-RepeatedValues"
        benchmark.extra_info = {"cache_enabled": cache_enabled}
        results = benchmark(process_all)

        # Verify results
        assert len(results) == len(data)

    @pytest.mark.parametrize("cache_enabled", [True, False])
    def test_flatten_cache_impact_unique_values(
        self, cache_enabled, benchmark, reset_caches
    ):
        """Benchmark the impact of caching with unique values."""
        # Set cache configuration
        settings.cache_enabled = cache_enabled
        refresh_cache_config()

        # Generate data with unique values
        data = generate_data_with_unique_values(num_records=50)

        # Function to benchmark
        def process_all():
            results = []
            for record in data:
                result = flatten_json(
                    record, separator="_", cast_to_string=True, skip_arrays=True
                )
                results.append(result)
            return results

        # Benchmark
        benchmark.group = "Caching-UniqueValues"
        benchmark.extra_info = {"cache_enabled": cache_enabled}
        results = benchmark(process_all)

        # Verify results
        assert len(results) == len(data)

    @pytest.mark.parametrize("cache_maxsize", [100, 1000, 10000])
    def test_cache_size_impact(self, cache_maxsize, benchmark, reset_caches):
        """Benchmark the impact of cache size on performance."""
        # Set cache configuration
        settings.cache_enabled = True
        settings.cache_maxsize = cache_maxsize
        refresh_cache_config()

        # Generate data with moderate value repetition
        data = generate_data_with_repeated_values(num_records=50, repetition_factor=0.5)

        # Function to benchmark
        def process_all():
            results = []
            for record in data:
                result = flatten_json(
                    record, separator="_", cast_to_string=True, skip_arrays=True
                )
                results.append(result)
            return results

        # Benchmark
        benchmark.group = "Caching-CacheSize"
        benchmark.extra_info = {"cache_maxsize": cache_maxsize}
        results = benchmark(process_all)

        # Verify results
        assert len(results) == len(data)

    @pytest.mark.parametrize("cache_enabled", [True, False])
    def test_processor_cache_impact(self, cache_enabled, benchmark, reset_caches):
        """Benchmark the impact of caching on the full Processor performance."""
        # Set cache configuration
        config = TransmogConfig.default().with_caching(enabled=cache_enabled)

        # Create processor
        processor = Processor(config=config)

        # Generate data with high value repetition
        data = generate_data_with_repeated_values(num_records=20, repetition_factor=0.9)

        # Benchmark
        benchmark.group = "Processor-Caching"
        benchmark.extra_info = {"cache_enabled": cache_enabled}
        result = benchmark(processor.process, data, entity_name="benchmark")

        # Verify results
        assert result.get_main_table()

    @pytest.mark.parametrize("repetition_factor", [0.1, 0.5, 0.9])
    def test_cache_hit_rate_impact(self, repetition_factor, benchmark, reset_caches):
        """Benchmark how cache hit rate affects performance."""
        # Set cache configuration
        settings.cache_enabled = True
        settings.cache_maxsize = 10000
        refresh_cache_config()

        # Generate data with specified repetition factor
        data = generate_data_with_repeated_values(
            num_records=20, repetition_factor=repetition_factor
        )

        # Function to benchmark
        def process_all():
            results = []
            for record in data:
                result = flatten_json(
                    record, separator="_", cast_to_string=True, skip_arrays=True
                )
                results.append(result)
            return results

        # Clear cache before this run
        clear_caches()

        # Benchmark
        benchmark.group = "Caching-HitRate"
        benchmark.extra_info = {"repetition_factor": repetition_factor}
        results = benchmark(process_all)

        # Verify results
        assert len(results) == len(data)
