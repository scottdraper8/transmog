"""
Benchmarks for processing modes.

This module contains benchmark tests to compare different processing modes:
- Standard (in-memory) processing
- Streaming processing
- Chunked processing
"""

import os
import tempfile
from typing import Any

import pytest

from transmog import ProcessingMode, Processor, TransmogConfig


def generate_benchmark_data(num_records: int = 100) -> list[dict[str, Any]]:
    """Generate benchmark data with a mix of nested structures and arrays."""
    data = []
    for i in range(num_records):
        record = {
            "id": f"record-{i}",
            "details": {
                "name": f"Record {i}",
                "description": f"Test record {i} for benchmarking",
                "value": i * 10,
                "tags": ["test", "benchmark", f"tag-{i}"],
            },
            "metadata": {
                "created": "2023-01-01",
                "modified": "2023-03-15",
                "source": "benchmark",
            },
            "status": {"active": i % 2 == 0, "approved": i % 3 == 0},
            "items": [
                {"id": f"item-{i}-1", "value": i * 100 + 1},
                {"id": f"item-{i}-2", "value": i * 100 + 2},
                {"id": f"item-{i}-3", "value": i * 100 + 3},
            ],
        }
        data.append(record)
    return data


@pytest.mark.benchmark
class TestProcessingModeBenchmarks:
    """Benchmark tests for different processing modes."""

    @pytest.mark.parametrize("num_records", [50, 200, 500])
    def test_standard_vs_chunked_scaling(self, num_records, benchmark):
        """Benchmark how standard and chunked processing scale with data size."""
        # Generate larger dataset for more visible differences
        data = generate_benchmark_data(num_records)

        # Create processors for each mode
        standard_processor = Processor(
            TransmogConfig.default().with_processing(
                processing_mode=ProcessingMode.STANDARD
            )
        )
        chunked_processor = Processor(
            TransmogConfig.default().with_processing(
                processing_mode=ProcessingMode.LOW_MEMORY
            )
        )

        # Combine both operations in a single function to benchmark
        def run_both_processors():
            # Run standard processing
            standard_result = standard_processor.process(data, entity_name="benchmark")

            # Verify standard result
            assert standard_result.get_main_table()
            assert len(standard_result.get_main_table()) == num_records

            # Run chunked processing
            chunked_result = chunked_processor.process_chunked(
                data, entity_name="benchmark", chunk_size=50
            )

            # Verify chunked result
            assert chunked_result.get_main_table()
            assert len(chunked_result.get_main_table()) == num_records

            return standard_result, chunked_result

        # Benchmark both
        benchmark.group = f"Processing-{num_records}Records"
        results = benchmark(run_both_processors)

        # Results already verified in the benchmarked function

    @pytest.mark.parametrize("chunk_size", [10, 50, 100])
    def test_chunk_size_impact(self, chunk_size, benchmark):
        """Benchmark the impact of chunk size on processing performance."""
        # Generate a fixed-size dataset
        data = generate_benchmark_data(200)

        # Create processor with memory-efficient config
        processor = Processor(
            TransmogConfig.default().with_processing(
                processing_mode=ProcessingMode.LOW_MEMORY
            )
        )

        # Benchmark with different chunk sizes
        benchmark.group = "ChunkSize"
        benchmark.extra_info = {"chunk_size": chunk_size}
        result = benchmark(
            processor.process_chunked,
            data,
            entity_name="benchmark",
            chunk_size=chunk_size,
        )

        # Verify result
        assert result.get_main_table()
        assert len(result.get_main_table()) == 200

    def test_streaming_vs_standard_output(self, benchmark):
        """Benchmark streaming output vs. standard output to files."""
        # Generate benchmark data
        data = generate_benchmark_data(100)

        # Create processor
        processor = Processor()

        # Create temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Benchmark standard processing with file output
            def standard_process_with_file_output():
                result = processor.process(data, entity_name="benchmark")
                # Write each format
                json_path = os.path.join(temp_dir, "standard", "json")
                result.write_all_json(json_path)
                csv_path = os.path.join(temp_dir, "standard", "csv")
                result.write_all_csv(csv_path)

                return result

            benchmark.group = "OutputMode"
            benchmark.extra_info = {"mode": "standard"}
            standard_result = benchmark(standard_process_with_file_output)

            # Verify standard result
            assert standard_result.get_main_table()
            assert len(standard_result.get_main_table()) == 100

    def test_process_modes_memory_usage(self, benchmark):
        """Test memory usage of different processing modes (approximate)."""
        # Generate larger dataset to make memory differences more noticeable
        data = generate_benchmark_data(500)

        # Create processors with different modes
        standard_processor = Processor(
            TransmogConfig.default().with_processing(
                processing_mode=ProcessingMode.STANDARD
            )
        )
        memory_efficient_processor = Processor(
            TransmogConfig.default().with_processing(
                processing_mode=ProcessingMode.LOW_MEMORY
            )
        )
        performance_processor = Processor(
            TransmogConfig.default().with_processing(
                processing_mode=ProcessingMode.HIGH_PERFORMANCE
            )
        )

        # Memory usage can't be directly measured with pytest-benchmark
        # But we can measure execution time which correlates with memory pressure

        # Use a single benchmark call to run all three modes
        def run_all_modes():
            # Run standard processing
            standard_result = standard_processor.process(data, entity_name="benchmark")

            # Run memory-efficient processing
            memory_result = memory_efficient_processor.process_chunked(
                data, entity_name="benchmark", chunk_size=50
            )

            # Run performance-optimized processing
            performance_result = performance_processor.process(
                data, entity_name="benchmark"
            )

            # Verify results
            for result in [standard_result, memory_result, performance_result]:
                assert len(result.get_main_table()) == 500

            return standard_result, memory_result, performance_result

        # Benchmark all three modes together
        benchmark.group = "ProcessingMode"
        results = benchmark(run_all_modes)

        # Results already verified in the benchmarked function

    @pytest.mark.parametrize("visit_arrays", [True, False])
    def test_array_processing_impact(self, visit_arrays, benchmark):
        """Benchmark the impact of array processing on performance."""
        # Generate benchmark data with multiple arrays
        data = generate_benchmark_data(100)

        # Create processor with specific visit_arrays setting
        processor = Processor(
            TransmogConfig.default().with_processing(visit_arrays=visit_arrays)
        )

        # Benchmark processing
        benchmark.group = "ArrayProcessing"
        benchmark.extra_info = {"visit_arrays": visit_arrays}
        result = benchmark(processor.process, data, entity_name="benchmark")

        # Verify results
        assert result.get_main_table()
        # When visit_arrays is False, there should be no child tables
        if not visit_arrays:
            assert len(result.get_table_names()) == 0
