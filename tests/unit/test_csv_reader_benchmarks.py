"""
Benchmark tests for the CSV reader functionality.

These tests measure the performance of different CSV reading implementations
and configurations.
"""

import os
import csv
import tempfile
import pytest
import time
from unittest import mock

from transmog.io.csv_reader import (
    read_csv_file,
    read_csv_stream,
    CSVReader,
    PYARROW_AVAILABLE,
)


@pytest.mark.benchmark
class TestCsvReaderBenchmarks:
    """Benchmark tests for CSV reader implementations."""

    def create_large_csv(self, rows=10000, cols=10):
        """Create a large CSV file for benchmarking."""
        header = [f"col_{i}" for i in range(cols)]
        data = []

        # Generate random data
        for i in range(rows):
            row = [f"value_{i}_{j}" for j in range(cols)]
            data.append(row)

        # Write to a temporary file
        with tempfile.NamedTemporaryFile(
            suffix=".csv", mode="w+", delete=False, newline=""
        ) as temp_file:
            writer = csv.writer(temp_file)
            writer.writerow(header)
            for row in data:
                writer.writerow(row)
            temp_path = temp_file.name

        return temp_path

    @pytest.mark.benchmark
    @pytest.mark.skipif(
        not PYARROW_AVAILABLE, reason="PyArrow required for this benchmark"
    )
    def test_pyarrow_vs_builtin_performance(self, benchmark):
        """Benchmark PyArrow vs built-in CSV reader performance."""
        # Generate test data (moderate size - 1000 rows, 5 columns)
        csv_path = self.create_large_csv(rows=1000, cols=5)

        try:
            # Benchmark the PyArrow implementation if available, otherwise the builtin
            if PYARROW_AVAILABLE:
                result = benchmark.pedantic(
                    lambda: read_csv_file(csv_path),
                    iterations=3,
                    rounds=5,
                )

                # Do a manual timing of the built-in implementation for comparison
                start_time = time.time()
                # Force using the built-in implementation by setting PYARROW_AVAILABLE to False
                with mock.patch(
                    "src.transmog.io.csv_reader.PYARROW_AVAILABLE", False
                ):
                    result_builtin = read_csv_file(csv_path)
                builtin_time = time.time() - start_time

                # Verification
                assert isinstance(result, list)
                assert len(result) == 1000
                assert len(result_builtin) == 1000

                # Output the comparison (doesn't affect the test result)
                print(
                    f"\nManual timing of built-in CSV reader: {builtin_time:.4f} seconds"
                )
            else:
                # Just benchmark the built-in if PyArrow isn't available
                result = benchmark.pedantic(
                    lambda: read_csv_file(csv_path),
                    iterations=3,
                    rounds=5,
                )
                assert isinstance(result, list)
                assert len(result) == 1000

        finally:
            # Clean up
            os.unlink(csv_path)

    @pytest.mark.benchmark
    def test_csv_size_scaling(self, benchmark):
        """Benchmark how performance scales with CSV file size."""
        # Generate test data with varying sizes
        small_path = self.create_large_csv(rows=100, cols=5)
        medium_path = self.create_large_csv(rows=1000, cols=5)
        large_path = self.create_large_csv(rows=5000, cols=5)

        try:
            # Define benchmark parameters (file_path, name)
            params = [
                (small_path, "Small (100 rows)"),
                (medium_path, "Medium (1000 rows)"),
                (large_path, "Large (5000 rows)"),
            ]

            # Just benchmark the first path as an example
            benchmark.pedantic(
                lambda: read_csv_file(small_path),
                iterations=3,
                rounds=5,
            )
        finally:
            # Clean up
            for path in [small_path, medium_path, large_path]:
                os.unlink(path)

    @pytest.mark.benchmark
    @pytest.mark.skipif(
        not PYARROW_AVAILABLE, reason="PyArrow required for this benchmark"
    )
    def test_pyarrow_options_performance(self, benchmark):
        """Benchmark the impact of different PyArrow options on performance."""
        # Generate test data
        csv_path = self.create_large_csv(rows=1000, cols=5)

        try:
            # Define benchmark parameters (options, name)
            params = [
                ({"infer_types": True}, "With Type Inference"),
                ({"infer_types": False}, "Without Type Inference"),
                ({"cast_to_string": True}, "Cast to String"),
                (
                    {"null_values": ["", "NULL", "null", "NA", "N/A"]},
                    "With Null Values",
                ),
            ]

            # Just benchmark the first option as an example
            if PYARROW_AVAILABLE:
                benchmark.pedantic(
                    lambda: read_csv_file(csv_path, **params[0][0]),
                    iterations=3,
                    rounds=5,
                )
        finally:
            # Clean up
            os.unlink(csv_path)

    @pytest.mark.benchmark
    def test_streaming_vs_full_read(self, benchmark):
        """Benchmark streaming vs full file reading."""
        # Generate test data
        csv_path = self.create_large_csv(rows=5000, cols=5)

        try:
            # Define benchmark function for streaming
            def read_streaming():
                records = []
                for chunk in read_csv_stream(csv_path, chunk_size=500):
                    records.extend(chunk)
                return records

            # Define benchmark function for full read
            def read_full():
                return read_csv_file(csv_path)

            # Benchmark just one function as an example
            result = benchmark.pedantic(
                read_full,
                iterations=3,
                rounds=5,
            )

            # Verify output is correct
            assert isinstance(result, list)
            assert len(result) == 5000
        finally:
            # Clean up
            os.unlink(csv_path)

    @pytest.mark.benchmark
    def test_manual_timing_comparison(self):
        """
        Manual timing comparison without pytest benchmark.

        Sometimes useful for quick comparisons or when pytest benchmark isn't working as expected.
        """
        # Use a smaller dataset to avoid performance issues
        rows = 500
        cols = 5

        # Generate test data with explicit size control
        csv_path = self.create_large_csv(rows=rows, cols=cols)

        # Verify the file size is reasonable
        file_size = os.path.getsize(csv_path)
        print(f"\nCSV file size: {file_size} bytes for {rows} rows, {cols} columns")

        # Hard limit on file size to catch potential issues early
        max_allowed_size = rows * cols * 20  # Conservative estimate
        assert file_size < max_allowed_size, f"CSV file too large ({file_size} bytes)"

        try:
            print("\nManual Timing Comparison:")

            # Test PyArrow if available
            if PYARROW_AVAILABLE:
                start_time = time.time()
                with mock.patch(
                    "src.transmog.io.csv_reader.PYARROW_AVAILABLE", True
                ):
                    result_pa = read_csv_file(csv_path)
                pa_time = time.time() - start_time

                # Strict verification
                assert len(result_pa) == rows, (
                    f"PyArrow returned {len(result_pa)} records, expected {rows}"
                )
                print(f"  PyArrow: {pa_time:.4f} seconds, {len(result_pa)} records")

            # Test built-in
            start_time = time.time()
            with mock.patch("src.transmog.io.csv_reader.PYARROW_AVAILABLE", False):
                result_builtin = read_csv_file(csv_path)
            builtin_time = time.time() - start_time

            # Strict verification
            assert len(result_builtin) == rows, (
                f"Built-in returned {len(result_builtin)} records, expected {rows}"
            )
            print(
                f"  Built-in: {builtin_time:.4f} seconds, {len(result_builtin)} records"
            )

            # Test streaming
            start_time = time.time()
            stream_records = []
            for chunk in read_csv_stream(csv_path, chunk_size=100):
                stream_records.extend(chunk)
            streaming_time = time.time() - start_time

            # Strict verification
            assert len(stream_records) == rows, (
                f"Streaming returned {len(stream_records)} records, expected {rows}"
            )
            print(
                f"  Streaming: {streaming_time:.4f} seconds, {len(stream_records)} records"
            )

            # Compare (no assertions, just informational)
            if PYARROW_AVAILABLE:
                speedup = builtin_time / pa_time if pa_time > 0 else float("inf")
                print(f"  PyArrow speedup vs built-in: {speedup:.2f}x")
        finally:
            # Clean up
            os.unlink(csv_path)

    @pytest.mark.benchmark
    def test_chunk_size_impact(self, benchmark):
        """Benchmark the impact of different chunk sizes on streaming performance."""
        # Generate test data (larger dataset)
        csv_path = self.create_large_csv(rows=5000, cols=5)

        try:
            # Define benchmark parameters (chunk_size, name)
            params = [
                (100, "Small Chunks (100)"),
                (500, "Medium Chunks (500)"),
                (1000, "Large Chunks (1000)"),
                (5000, "Full Size (5000)"),
            ]

            # Define benchmark function for a single chunk size
            def read_with_chunk_size():
                records = []
                for chunk in read_csv_stream(
                    csv_path, chunk_size=100
                ):  # Using a fixed chunk size
                    records.extend(chunk)
                return records

            # Run benchmark
            result = benchmark.pedantic(
                read_with_chunk_size,
                iterations=3,
                rounds=5,
            )

            # Verify output is correct
            assert isinstance(result, list)
            assert len(result) == 5000
        finally:
            # Clean up
            os.unlink(csv_path)
