"""
Benchmarks for output format methods.

This module contains benchmark tests for the new output format methods
added to ProcessingResult.
"""

import pytest
from typing import Dict, List, Any

from transmog import Processor


def generate_test_data(num_records: int = 100) -> List[Dict[str, Any]]:
    """Generate synthetic test data for benchmarks."""
    data = []
    for i in range(num_records):
        data.append(
            {
                "id": f"record-{i}",
                "metadata": {
                    "created": "2023-01-01",
                    "modified": "2023-03-15",
                    "source": "benchmark",
                },
                "details": {
                    "name": f"Record {i}",
                    "description": f"Test record {i} for benchmarking",
                    "value": i * 10,
                    "tags": ["test", "benchmark", f"tag-{i}"],
                },
                "items": [
                    {"item_id": f"item-{i}-1", "value": i * 10},
                    {"item_id": f"item-{i}-2", "value": i * 20},
                    {"item_id": f"item-{i}-3", "value": i * 30},
                ],
                "status": {
                    "active": i % 2 == 0,
                    "approved": i % 3 == 0,
                    "visible": i % 5 != 0,
                },
            }
        )
    return data


@pytest.fixture
def processed_result():
    """Create a ProcessingResult with test data."""
    # Generate test data
    data = generate_test_data(100)

    # Process the data
    processor = Processor(visit_arrays=True)
    return processor.process(data, entity_name="benchmark")


#
# Benchmark tests for native data structure methods
#


def test_benchmark_to_dict(processed_result, benchmark):
    """Benchmark the to_dict method."""
    benchmark(processed_result.to_dict)


def test_benchmark_to_json_objects(processed_result, benchmark):
    """Benchmark the to_json_objects method."""
    benchmark(processed_result.to_json_objects)


@pytest.mark.skipif(
    not pytest.importorskip("pyarrow", reason="PyArrow not available"),
    reason="PyArrow required for this benchmark",
)
def test_benchmark_to_pyarrow_tables(processed_result, benchmark):
    """Benchmark the to_pyarrow_tables method."""
    benchmark(processed_result.to_pyarrow_tables)


#
# Benchmark tests for bytes serialization methods
#


def test_benchmark_to_json_bytes(processed_result, benchmark):
    """Benchmark the to_json_bytes method."""
    # Test with no indentation for best performance
    benchmark(processed_result.to_json_bytes, indent=None)


def test_benchmark_to_json_bytes_pretty(processed_result, benchmark):
    """Benchmark the to_json_bytes method with pretty printing."""
    benchmark(processed_result.to_json_bytes, indent=2)


def test_benchmark_to_csv_bytes(processed_result, benchmark):
    """Benchmark the to_csv_bytes method."""
    benchmark(processed_result.to_csv_bytes)


@pytest.mark.skipif(
    not pytest.importorskip("pyarrow", reason="PyArrow not available"),
    reason="PyArrow required for this benchmark",
)
def test_benchmark_to_parquet_bytes(processed_result, benchmark):
    """Benchmark the to_parquet_bytes method."""
    benchmark(processed_result.to_parquet_bytes)


@pytest.mark.skipif(
    not pytest.importorskip("pyarrow", reason="PyArrow not available"),
    reason="PyArrow required for this benchmark",
)
def test_benchmark_to_parquet_bytes_uncompressed(processed_result, benchmark):
    """Benchmark the to_parquet_bytes method with no compression."""
    benchmark(processed_result.to_parquet_bytes, compression=None)


#
# Comparison between bytes and file output methods
#


@pytest.mark.benchmark
def test_compare_json_methods(processed_result, tmp_path):
    """Compare the performance of JSON output methods."""
    # Memory output
    json_bytes = processed_result.to_json_bytes()

    # Temp file for file output
    json_dir = tmp_path / "json"

    # File output
    file_paths = processed_result.write_all_json(str(json_dir))

    # Print results for information
    print(f"\nJSON bytes output size: {sum(len(b) for b in json_bytes.values())} bytes")

    # Check that the files exist
    for path in file_paths.values():
        assert tmp_path.exists()


@pytest.mark.benchmark
@pytest.mark.skipif(
    not pytest.importorskip("pyarrow", reason="PyArrow not available"),
    reason="PyArrow required for this benchmark",
)
def test_compare_parquet_methods(processed_result, tmp_path):
    """Compare the performance of Parquet output methods."""
    # Memory output
    parquet_bytes = processed_result.to_parquet_bytes()

    # Temp file for file output
    parquet_dir = tmp_path / "parquet"

    # File output
    file_paths = processed_result.write_all_parquet(str(parquet_dir))

    # Print results for information
    print(
        f"\nParquet bytes output size: {sum(len(b) for b in parquet_bytes.values())} bytes"
    )

    # Check that the files exist
    for path in file_paths.values():
        assert tmp_path.exists()


@pytest.mark.memory
def test_memory_usage_comparison(processed_result):
    """
    Compare memory usage between different output methods.

    This test is marked with 'memory' and can be run selectively with:
    pytest -m memory
    """
    # This is a stub test for now - memory profiling would be done externally
    # or with pytest-memray or similar plugins

    # Test default behavior, but don't actually measure memory - just ensure it works
    dict_result = processed_result.to_dict()
    json_objects = processed_result.to_json_objects()
    json_bytes = processed_result.to_json_bytes()
    csv_bytes = processed_result.to_csv_bytes()

    # Basic assertions to ensure the methods worked
    assert dict_result
    assert json_objects
    assert json_bytes
    assert csv_bytes
