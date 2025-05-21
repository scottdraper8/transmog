"""
Benchmarks for core transformation components.

This module contains benchmark tests for the core transformation components:
- Flattener (flatten_json)
- Extractor (extract_arrays)
- Hierarchy (process_structure)
"""

from typing import Any

import pytest

from transmog.core.extractor import extract_arrays
from transmog.core.flattener import clear_caches, flatten_json
from transmog.core.hierarchy import process_structure
from transmog.core.metadata import get_current_timestamp


def generate_nested_data(depth: int = 3, width: int = 3) -> dict[str, Any]:
    """Generate synthetic nested data with controllable depth and width."""
    if depth <= 0:
        return {"value": "leaf"}

    result = {}
    # Add some scalar values
    result["id"] = f"node-{depth}-{width}"
    result["level"] = depth

    # Add nested objects with controlled depth
    for i in range(width):
        result[f"nested_{i}"] = generate_nested_data(depth - 1, width)

    # Add an array at each level
    result["items"] = []
    for i in range(width):
        item = {
            "item_id": f"item-{depth}-{i}",
            "value": i * depth,
        }
        if depth > 1:
            item["details"] = generate_nested_data(depth - 1, max(1, width - 1))
        result["items"].append(item)

    return result


def generate_array_heavy_data(
    array_count: int = 3, array_size: int = 5
) -> dict[str, Any]:
    """Generate data with many arrays at different nesting levels."""
    data = {
        "id": "root",
        "metadata": {
            "created": "2023-01-01",
            "source": "benchmark",
        },
    }

    # Add top-level arrays
    for a in range(array_count):
        array_name = f"array_{a}"
        data[array_name] = []

        for i in range(array_size):
            item = {
                "id": f"{array_name}_item_{i}",
                "value": i * 10,
            }

            # Add nested array to each item
            item["sub_items"] = []
            for j in range(array_size // 2):
                sub_item = {
                    "id": f"{array_name}_subitem_{i}_{j}",
                    "value": j * 5,
                }
                item["sub_items"].append(sub_item)

            data[array_name].append(item)

    return data


@pytest.fixture(scope="function")
def reset_caches():
    """Reset any caches before and after each test."""
    clear_caches()
    yield
    clear_caches()


@pytest.mark.benchmark
class TestFlattenerBenchmarks:
    """Benchmark tests for the flattener module."""

    @pytest.mark.parametrize("depth", [2, 4, 6])
    def test_flatten_depth_scaling(self, depth, benchmark, reset_caches):
        """Benchmark how flattening performance scales with nesting depth."""
        # Generate test data with specified depth
        data = generate_nested_data(depth=depth, width=3)

        # Benchmark flattening
        benchmark.group = "Flattener-Depth"
        benchmark.extra_info = {"depth": depth}
        result = benchmark(
            flatten_json, data, separator="_", cast_to_string=True, skip_arrays=True
        )

        # Verify the result
        assert isinstance(result, dict)

    @pytest.mark.parametrize("width", [2, 5, 10])
    def test_flatten_width_scaling(self, width, benchmark, reset_caches):
        """Benchmark how flattening performance scales with object width."""
        # Generate test data with specified width
        data = generate_nested_data(depth=3, width=width)

        # Benchmark flattening
        benchmark.group = "Flattener-Width"
        benchmark.extra_info = {"width": width}
        result = benchmark(
            flatten_json, data, separator="_", cast_to_string=True, skip_arrays=True
        )

        # Verify the result
        assert isinstance(result, dict)

    @pytest.mark.parametrize("cast_to_string", [True, False])
    def test_flatten_cast_to_string(self, cast_to_string, benchmark, reset_caches):
        """Benchmark the impact of string casting on performance."""
        # Generate test data
        data = generate_nested_data(depth=4, width=3)

        # Benchmark flattening with different cast_to_string settings
        benchmark.group = "Flattener-Options"
        benchmark.extra_info = {"cast_to_string": cast_to_string}
        result = benchmark(
            flatten_json,
            data,
            separator="_",
            cast_to_string=cast_to_string,
            skip_arrays=True,
        )

        # Verify the result
        assert isinstance(result, dict)

    @pytest.mark.parametrize(
        "include_empty,skip_null",
        [(True, True), (False, True), (True, False), (False, False)],
    )
    def test_flatten_null_empty_handling(
        self, include_empty, skip_null, benchmark, reset_caches
    ):
        """Benchmark the impact of null and empty value handling on performance."""
        # Generate test data with null and empty values
        data = generate_nested_data(depth=3, width=3)
        # Add null and empty values
        data["null_value"] = None
        data["empty_value"] = ""
        data["nested_with_nulls"] = {"a": None, "b": "", "c": "value"}

        # Benchmark flattening with different null/empty handling settings
        benchmark.group = "Flattener-NullHandling"
        benchmark.extra_info = {"include_empty": include_empty, "skip_null": skip_null}
        result = benchmark(
            flatten_json,
            data,
            separator="_",
            cast_to_string=True,
            include_empty=include_empty,
            skip_null=skip_null,
            skip_arrays=True,
        )

        # Verify the result
        assert isinstance(result, dict)


@pytest.mark.benchmark
class TestExtractorBenchmarks:
    """Benchmark tests for the array extractor module."""

    @pytest.mark.parametrize("array_count", [1, 3, 5])
    def test_extract_array_count_scaling(self, array_count, benchmark):
        """Benchmark how extraction performance scales with number of arrays."""
        # Generate test data with specified number of arrays
        data = generate_array_heavy_data(array_count=array_count, array_size=5)

        # Benchmark array extraction
        benchmark.group = "Extractor-ArrayCount"
        benchmark.extra_info = {"array_count": array_count}
        result = benchmark(
            extract_arrays,
            data,
            parent_id="test-parent",
            entity_name="benchmark",
            separator="_",
            cast_to_string=True,
        )

        # Verify the result
        assert isinstance(result, dict)
        # Should have at least array_count tables (possibly more due to nested arrays)
        assert len(result) >= array_count

    @pytest.mark.parametrize("array_size", [2, 10, 50])
    def test_extract_array_size_scaling(self, array_size, benchmark):
        """Benchmark how extraction performance scales with array size."""
        # Generate test data with specified array size
        data = generate_array_heavy_data(array_count=2, array_size=array_size)

        # Benchmark array extraction
        benchmark.group = "Extractor-ArraySize"
        benchmark.extra_info = {"array_size": array_size}
        result = benchmark(
            extract_arrays,
            data,
            parent_id="test-parent",
            entity_name="benchmark",
            separator="_",
            cast_to_string=True,
        )

        # Verify the result
        assert isinstance(result, dict)

    @pytest.mark.parametrize("deeply_nested_threshold", [2, 4, 10])
    def test_extract_with_deeply_nested_threshold(
        self, deeply_nested_threshold, benchmark
    ):
        """Benchmark the impact of deeply nested path handling on extraction performance."""
        # Generate test data
        data = generate_array_heavy_data(array_count=3, array_size=10)

        # Benchmark array extraction with different deeply nested threshold settings
        benchmark.group = "Extractor-DeeplyNested"
        benchmark.extra_info = {"deeply_nested_threshold": deeply_nested_threshold}
        result = benchmark(
            extract_arrays,
            data,
            parent_id="test-parent",
            entity_name="benchmark",
            separator="_",
            cast_to_string=True,
            deeply_nested_threshold=deeply_nested_threshold,
        )

        # Verify the result
        assert isinstance(result, dict)


@pytest.mark.benchmark
class TestHierarchyBenchmarks:
    """Benchmark tests for the hierarchy processing module."""

    @pytest.mark.parametrize("depth", [2, 4])
    @pytest.mark.parametrize("width", [2, 5])
    def test_process_structure_scaling(self, depth, width, benchmark):
        """Benchmark how process_structure scales with data complexity."""
        # Generate test data with specified depth and width
        data = generate_nested_data(depth=depth, width=width)

        # Benchmark process_structure
        benchmark.group = "Hierarchy-Complexity"
        benchmark.extra_info = {"depth": depth, "width": width}
        main_obj, child_tables = benchmark(
            process_structure,
            data,
            entity_name="benchmark",
            separator="_",
            cast_to_string=True,
            extract_time=get_current_timestamp(),
            deeply_nested_threshold=4,
            visit_arrays=True,
        )

        # Verify the result
        assert isinstance(main_obj, dict)
        assert isinstance(child_tables, dict)

    @pytest.mark.parametrize("visit_arrays", [True, False])
    def test_process_structure_visit_arrays(self, visit_arrays, benchmark):
        """Benchmark the impact of visit_arrays parameter on process_structure performance."""
        # Generate test data
        data = generate_array_heavy_data(array_count=3, array_size=5)

        # Benchmark process_structure with different visit_arrays settings
        benchmark.group = "Hierarchy-VisitArrays"
        benchmark.extra_info = {"visit_arrays": visit_arrays}
        main_obj, child_tables = benchmark(
            process_structure,
            data,
            entity_name="benchmark",
            separator="_",
            cast_to_string=True,
            extract_time=get_current_timestamp(),
            deeply_nested_threshold=4,
            visit_arrays=visit_arrays,
        )

        # Verify the result
        assert isinstance(main_obj, dict)
        if visit_arrays:
            assert len(child_tables) > 0
        else:
            assert len(child_tables) == 0
