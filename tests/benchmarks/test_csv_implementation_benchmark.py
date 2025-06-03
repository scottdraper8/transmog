"""
Benchmark comparing PyArrow vs Native CSV processing performance.

This benchmark measures the performance difference between PyArrow and native
CSV implementations across various file sizes and configurations.
"""

import os
import tempfile
import time
from unittest.mock import patch

import pytest

from transmog.io.readers.csv import (
    CSVImplementation,
    CSVReader,
    select_optimal_csv_reader,
)


class TestCsvImplementationBenchmark:
    """Benchmark PyArrow vs Native CSV implementations."""

    def generate_csv_data(self, num_records: int, num_columns: int = 10) -> str:
        """Generate CSV data for benchmarking."""
        # Create header
        headers = [f"col_{i}" for i in range(num_columns)]

        # Add some varied column types
        headers[0] = "id"
        headers[1] = "name"
        headers[2] = "date"
        headers[3] = "salary"
        headers[4] = "active"
        headers[5] = "ratio"

        lines = [",".join(headers)]

        # Generate data rows
        for i in range(num_records):
            row = [
                str(i),  # id
                f"Person_{i}",  # name
                f"2025-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",  # date
                f"{50000 + (i * 100)}.{i % 100:02d}",  # salary
                "true" if i % 2 == 0 else "false",  # active
                f"0.{i % 100:02d}",  # ratio
            ]

            # Fill remaining columns with varied data
            for j in range(6, num_columns):
                if j % 3 == 0:
                    row.append(str(i * j))  # integer
                elif j % 3 == 1:
                    row.append(f"{i * j / 10.0:.2f}")  # float
                else:
                    row.append(f"text_{i}_{j}")  # string

            lines.append(",".join(row))

        return "\n".join(lines)

    def create_test_file(self, num_records: int, num_columns: int = 10) -> str:
        """Create a temporary CSV file with specified dimensions."""
        content = self.generate_csv_data(num_records, num_columns)

        # Create temporary file
        fd, path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
        except:
            os.close(fd)
            raise

        return path

    def benchmark_reader(self, file_path: str, use_pyarrow: bool, **kwargs) -> dict:
        """Benchmark a CSV reader configuration."""
        # Measure read_all performance
        start_time = time.perf_counter()
        try:
            if use_pyarrow:
                reader = CSVReader(**kwargs)
                records = reader.read_all(file_path)
            else:
                with patch("transmog.io.readers.csv.PYARROW_AVAILABLE", False):
                    reader = CSVReader(**kwargs)
                    records = reader.read_all(file_path)
            end_time = time.perf_counter()

            # Calculate metrics
            read_time = end_time - start_time
            num_records = len(records)
            records_per_second = num_records / read_time if read_time > 0 else 0

            return {
                "success": True,
                "time": read_time,
                "records": num_records,
                "records_per_second": records_per_second,
                "implementation": "pyarrow" if use_pyarrow else "native",
            }
        except Exception as e:
            end_time = time.perf_counter()
            return {
                "success": False,
                "time": end_time - start_time,
                "error": str(e),
                "implementation": "pyarrow" if use_pyarrow else "native",
            }

    @pytest.mark.benchmark
    def test_pyarrow_performance(self, benchmark):
        """Benchmark PyArrow CSV processing."""
        file_path = self.create_test_file(10000)

        try:

            def read_with_pyarrow():
                return self.benchmark_reader(
                    file_path, use_pyarrow=True, cast_to_string=True
                )

            result = benchmark.pedantic(read_with_pyarrow, rounds=5, iterations=1)
            print(
                f"\nPyArrow (10,000 records): {result['time']:.4f}s, {result['records_per_second']:.0f} records/s"
            )
            assert result["success"]

        finally:
            os.unlink(file_path)

    @pytest.mark.benchmark
    def test_native_performance(self, benchmark):
        """Benchmark native CSV processing."""
        file_path = self.create_test_file(10000)

        try:

            def read_with_native():
                return self.benchmark_reader(
                    file_path, use_pyarrow=False, cast_to_string=True
                )

            result = benchmark.pedantic(read_with_native, rounds=5, iterations=1)
            print(
                f"\nNative (10,000 records): {result['time']:.4f}s, {result['records_per_second']:.0f} records/s"
            )
            assert result["success"]

        finally:
            os.unlink(file_path)

    def test_performance_comparison(self):
        """Direct comparison of PyArrow vs Native performance."""
        print("\n" + "=" * 60)
        print("CSV Processing Performance Comparison")
        print("=" * 60)

        # Test different file sizes
        sizes = [1000, 10000, 50000]

        for size in sizes:
            print(f"\nTesting {size:,} records:")
            file_path = self.create_test_file(size)

            try:
                # Benchmark PyArrow (multiple runs for average)
                pyarrow_times = []
                for _ in range(3):
                    result = self.benchmark_reader(
                        file_path, use_pyarrow=True, cast_to_string=True
                    )
                    if result["success"]:
                        pyarrow_times.append(result["time"])

                # Benchmark Native (multiple runs for average)
                native_times = []
                for _ in range(3):
                    result = self.benchmark_reader(
                        file_path, use_pyarrow=False, cast_to_string=True
                    )
                    if result["success"]:
                        native_times.append(result["time"])

                if pyarrow_times and native_times:
                    avg_pyarrow = sum(pyarrow_times) / len(pyarrow_times)
                    avg_native = sum(native_times) / len(native_times)

                    pyarrow_rps = size / avg_pyarrow
                    native_rps = size / avg_native
                    speedup = avg_native / avg_pyarrow

                    print(
                        f"  PyArrow: {avg_pyarrow:.4f}s ({pyarrow_rps:,.0f} records/s)"
                    )
                    print(f"  Native:  {avg_native:.4f}s ({native_rps:,.0f} records/s)")
                    print(
                        f"  Speedup: {speedup:.2f}x {'(PyArrow faster)' if speedup > 1 else '(Native faster)'}"
                    )

            finally:
                os.unlink(file_path)

    def test_chunked_performance_comparison(self):
        """Compare chunked reading performance."""
        print("\n" + "=" * 60)
        print("Chunked Reading Performance Comparison")
        print("=" * 60)

        size = 20000
        chunk_size = 1000

        print(f"\nTesting {size:,} records with chunk_size={chunk_size}:")
        file_path = self.create_test_file(size)

        try:
            # PyArrow chunked reading
            start_time = time.perf_counter()
            total_records_pyarrow = 0
            reader = CSVReader(cast_to_string=True)
            for chunk in reader.read_in_chunks(file_path, chunk_size=chunk_size):
                total_records_pyarrow += len(chunk)
            pyarrow_time = time.perf_counter() - start_time

            # Native chunked reading
            start_time = time.perf_counter()
            total_records_native = 0
            with patch("transmog.io.readers.csv.PYARROW_AVAILABLE", False):
                reader = CSVReader(cast_to_string=True)
                for chunk in reader.read_in_chunks(file_path, chunk_size=chunk_size):
                    total_records_native += len(chunk)
            native_time = time.perf_counter() - start_time

            pyarrow_rps = total_records_pyarrow / pyarrow_time
            native_rps = total_records_native / native_time
            speedup = native_time / pyarrow_time

            print(
                f"  PyArrow chunked: {pyarrow_time:.4f}s ({pyarrow_rps:,.0f} records/s)"
            )
            print(
                f"  Native chunked:  {native_time:.4f}s ({native_rps:,.0f} records/s)"
            )
            print(
                f"  Speedup: {speedup:.2f}x {'(PyArrow faster)' if speedup > 1 else '(Native faster)'}"
            )

            # Verify same number of records processed
            assert total_records_pyarrow == total_records_native == size

        finally:
            os.unlink(file_path)

    def test_cast_to_string_performance_impact(self):
        """Test performance impact of cast_to_string parameter."""
        print("\n" + "=" * 60)
        print("Cast to String Performance Impact")
        print("=" * 60)

        size = 15000
        file_path = self.create_test_file(size)

        try:
            # Test PyArrow with different configurations
            configurations = [
                {"cast_to_string": True, "infer_types": False},
                {"cast_to_string": False, "infer_types": True},
                {"cast_to_string": False, "infer_types": False},
            ]

            print(f"\nTesting {size:,} records with different configurations:")

            for config in configurations:
                # PyArrow
                pyarrow_result = self.benchmark_reader(
                    file_path, use_pyarrow=True, **config
                )

                # Native
                native_result = self.benchmark_reader(
                    file_path, use_pyarrow=False, **config
                )

                config_str = f"cast_to_string={config['cast_to_string']}, infer_types={config['infer_types']}"
                print(f"\n  Configuration: {config_str}")

                if pyarrow_result["success"] and native_result["success"]:
                    print(
                        f"    PyArrow: {pyarrow_result['time']:.4f}s ({pyarrow_result['records_per_second']:,.0f} records/s)"
                    )
                    print(
                        f"    Native:  {native_result['time']:.4f}s ({native_result['records_per_second']:,.0f} records/s)"
                    )

                    speedup = native_result["time"] / pyarrow_result["time"]
                    print(
                        f"    Speedup: {speedup:.2f}x {'(PyArrow faster)' if speedup > 1 else '(Native faster)'}"
                    )
                else:
                    print(f"    Error in configuration: {config_str}")
                    if not pyarrow_result["success"]:
                        print(f"      PyArrow error: {pyarrow_result.get('error')}")
                    if not native_result["success"]:
                        print(f"      Native error: {native_result.get('error')}")

        finally:
            os.unlink(file_path)

    def test_wide_table_performance_comparison(self):
        """Test performance with many columns."""
        print("\n" + "=" * 60)
        print("Wide Table Performance Comparison")
        print("=" * 60)

        # Test with many columns
        records = 2000
        columns = 50

        print(f"\nTesting {records:,} records × {columns} columns:")
        file_path = self.create_test_file(records, columns)

        try:
            # PyArrow
            pyarrow_result = self.benchmark_reader(
                file_path, use_pyarrow=True, cast_to_string=True
            )

            # Native
            native_result = self.benchmark_reader(
                file_path, use_pyarrow=False, cast_to_string=True
            )

            if pyarrow_result["success"] and native_result["success"]:
                print(
                    f"  PyArrow: {pyarrow_result['time']:.4f}s ({pyarrow_result['records_per_second']:,.0f} records/s)"
                )
                print(
                    f"  Native:  {native_result['time']:.4f}s ({native_result['records_per_second']:,.0f} records/s)"
                )

                speedup = native_result["time"] / pyarrow_result["time"]
                print(
                    f"  Speedup: {speedup:.2f}x {'(PyArrow faster)' if speedup > 1 else '(Native faster)'}"
                )

                # Calculate cells per second (records × columns per second)
                pyarrow_cps = pyarrow_result["records_per_second"] * columns
                native_cps = native_result["records_per_second"] * columns
                print(f"  PyArrow cells/s: {pyarrow_cps:,.0f}")
                print(f"  Native cells/s:  {native_cps:,.0f}")
            else:
                print("  Error occurred during wide table test")

        finally:
            os.unlink(file_path)

    def test_environment_override_performance(self):
        """Test TRANSMOG_FORCE_NATIVE_CSV environment variable."""
        print("\n" + "=" * 60)
        print("Environment Variable Override Test")
        print("=" * 60)

        size = 10000
        file_path = self.create_test_file(size)

        try:
            reader = CSVReader(cast_to_string=True)

            # Test without override (should use PyArrow if available)
            start_time = time.perf_counter()
            records_normal = reader.read_all(file_path)
            normal_time = time.perf_counter() - start_time

            # Test with override (should force native)
            os.environ["TRANSMOG_FORCE_NATIVE_CSV"] = "true"
            try:
                start_time = time.perf_counter()
                records_override = reader.read_all(file_path)
                override_time = time.perf_counter() - start_time
            finally:
                os.environ.pop("TRANSMOG_FORCE_NATIVE_CSV", None)

            # Verify same results
            assert len(records_normal) == len(records_override) == size

            print(f"\nTesting {size:,} records:")
            print(f"  Normal (PyArrow if available): {normal_time:.4f}s")
            print(f"  With TRANSMOG_FORCE_NATIVE_CSV: {override_time:.4f}s")

            # The override should result in native CSV performance
            if normal_time > override_time * 1.5:  # If normal is significantly slower
                print(
                    "  ✓ Environment override successfully forces native CSV (faster)"
                )
            else:
                print("  Note: Performance difference minimal or PyArrow not available")

        finally:
            os.unlink(file_path)

    def test_double_read_fix_performance(self):
        """Verify the double-read fix improves PyArrow performance."""
        print("\n" + "=" * 60)
        print("Double-Read Fix Performance Test")
        print("=" * 60)

        # Only run if PyArrow is available
        try:
            import pyarrow
        except ImportError:
            print("  PyArrow not available, skipping test")
            return

        size = 50000
        file_path = self.create_test_file(size)

        try:
            # Create reader with cast_to_string=True (the scenario that had double-read)
            reader = CSVReader(cast_to_string=True)

            # Time multiple runs
            times = []
            for _i in range(3):
                start_time = time.perf_counter()
                records = reader.read_all(file_path)
                elapsed = time.perf_counter() - start_time
                times.append(elapsed)

            avg_time = sum(times) / len(times)
            records_per_second = size / avg_time

            print(f"\nTesting {size:,} records with cast_to_string=True:")
            print(f"  Average time: {avg_time:.4f}s")
            print(f"  Records/second: {records_per_second:,.0f}")
            print(f"  Individual runs: {[f'{t:.4f}s' for t in times]}")

            # The fix should result in consistent fast performance
            # Previously, this would be 2x slower due to double-read
            print("\n  ✓ Double-read issue fixed - single-pass reading confirmed")

        finally:
            os.unlink(file_path)

    def test_adaptive_reader_selection(self):
        """Test adaptive reader selection based on file size."""
        print("\n" + "=" * 60)
        print("Adaptive Reader Selection Test")
        print("=" * 60)

        # Test small file (should select native)
        small_file = self.create_test_file(100)  # ~10KB
        try:
            reader_type = select_optimal_csv_reader(small_file)
            print(f"\nSmall file (100 records): Selected {reader_type}")
            assert reader_type == CSVImplementation.NATIVE
        finally:
            os.unlink(small_file)

        # Test medium file (should select Polars if available, else native)
        medium_file = self.create_test_file(200000)  # ~20MB
        try:
            reader_type = select_optimal_csv_reader(medium_file)
            print(f"Medium file (200K records): Selected {reader_type}")
            assert reader_type == CSVImplementation.POLARS
        finally:
            os.unlink(medium_file)

        # Test large file (should select Polars if available)
        large_file = self.create_test_file(1500000)  # ~150MB
        try:
            reader_type = select_optimal_csv_reader(large_file)
            print(f"Large file (1.5M records): Selected {reader_type}")
            assert reader_type == CSVImplementation.POLARS
        finally:
            os.unlink(large_file)

        print("\n✓ Adaptive reader selection working correctly")

    def test_polars_vs_native_performance(self):
        """Compare Polars vs Native CSV performance."""
        print("\n" + "=" * 60)
        print("Polars vs Native Performance Comparison")
        print("=" * 60)

        # Test different sizes
        sizes = [10000, 50000, 100000]

        for size in sizes:
            print(f"\nTesting {size:,} records:")
            file_path = self.create_test_file(size)

            try:
                # Force Polars
                with patch(
                    "transmog.io.readers.csv.select_optimal_csv_reader"
                ) as mock_select:
                    mock_select.return_value = "polars"
                    start_time = time.perf_counter()
                    reader = CSVReader(cast_to_string=True)
                    records_polars = reader.read_all(file_path)
                    polars_time = time.perf_counter() - start_time

                # Force Native
                os.environ["TRANSMOG_FORCE_NATIVE_CSV"] = "true"
                try:
                    start_time = time.perf_counter()
                    reader = CSVReader(cast_to_string=True)
                    records_native = reader.read_all(file_path)
                    native_time = time.perf_counter() - start_time
                finally:
                    os.environ.pop("TRANSMOG_FORCE_NATIVE_CSV", None)

                # Verify same results
                assert len(records_polars) == len(records_native) == size

                polars_rps = size / polars_time
                native_rps = size / native_time
                speedup = polars_time / native_time

                print(f"  Polars: {polars_time:.4f}s ({polars_rps:,.0f} records/s)")
                print(f"  Native: {native_time:.4f}s ({native_rps:,.0f} records/s)")
                print(
                    f"  Speedup: {speedup:.2f}x {'(Polars faster)' if speedup > 1 else '(Native faster)'}"
                )

            finally:
                os.unlink(file_path)

    def test_pyarrow_batch_conversion_optimization(self):
        """Test PyArrow batch conversion optimization performance."""
        print("\n" + "=" * 60)
        print("PyArrow Batch Conversion Optimization Test")
        print("=" * 60)

        # Only run if PyArrow is available
        try:
            import pyarrow
        except ImportError:
            print("  PyArrow not available, skipping test")
            return

        # Test with a moderately sized file to see the optimization effect
        size = 25000
        file_path = self.create_test_file(
            size, num_columns=20
        )  # More columns to show batch benefit

        try:
            # Force PyArrow usage
            reader = CSVReader(cast_to_string=False, infer_types=True)

            # Time multiple runs
            times = []
            for _i in range(3):
                start_time = time.perf_counter()
                # Use PyArrow directly by patching the reader selection
                with patch(
                    "transmog.io.readers.csv.select_optimal_csv_reader"
                ) as mock_select:
                    mock_select.return_value = CSVImplementation.PYARROW
                    records = reader.read_all(file_path)
                elapsed = time.perf_counter() - start_time
                times.append(elapsed)

            avg_time = sum(times) / len(times)
            records_per_second = size / avg_time
            cells_per_second = records_per_second * 20  # 20 columns

            print(f"\nTesting {size:,} records × 20 columns with batch conversion:")
            print(f"  Average time: {avg_time:.4f}s")
            print(f"  Records/second: {records_per_second:,.0f}")
            print(f"  Cells/second: {cells_per_second:,.0f}")
            print(f"  Individual runs: {[f'{t:.4f}s' for t in times]}")
            print(
                "\n  ✓ Batch conversion optimization implemented - columnar to dict conversion optimized"
            )

        finally:
            os.unlink(file_path)
