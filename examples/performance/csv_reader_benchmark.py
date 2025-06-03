"""Example Name: CSV Reader Performance Benchmark.

Demonstrates: Benchmarking different CSV reader implementations in Transmog.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/processing/csv-processing.html
- https://transmog.readthedocs.io/en/latest/api/csv-reader.html

Learning Objectives:
- Compare performance of different CSV reader implementations
- Understand when to use each reader
- Learn about performance optimization techniques
- See the impact of file size on reader selection
"""

import csv
import os
import tempfile
import time
from typing import Any

# Import from transmog package
import transmog as tm


def create_csv_file(num_records: int, num_columns: int = 10) -> str:
    """Create a temporary CSV file with specified dimensions."""
    # Create temporary file
    fd, filepath = tempfile.mkstemp(suffix=".csv")

    try:
        with os.fdopen(fd, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)

            # Write header
            headers = [f"col_{i}" for i in range(num_columns)]
            headers[0] = "id"
            headers[1] = "name"
            headers[2] = "value"
            headers[3] = "active"
            writer.writerow(headers)

            # Write data rows
            for i in range(num_records):
                row = [
                    str(i),  # id
                    f"Item_{i}",  # name
                    f"{i * 100.5:.2f}",  # value
                    "true" if i % 2 == 0 else "false",  # active
                ]
                # Fill remaining columns
                for j in range(4, num_columns):
                    row.append(f"data_{i}_{j}")
                writer.writerow(row)
    except:
        os.close(fd)
        raise

    return filepath


def benchmark_csv_reader(filepath: str, force_native: bool = False) -> dict[str, Any]:
    """Benchmark CSV reader performance."""
    # Set environment variable if forcing native reader
    if force_native:
        os.environ["TRANSMOG_FORCE_NATIVE_CSV"] = "true"

    try:
        # Create processor
        processor = tm.Processor()

        # Time the processing
        start_time = time.perf_counter()
        result = processor.process_file(
            file_path=filepath, entity_name="benchmark_data"
        )
        end_time = time.perf_counter()

        # Calculate metrics
        elapsed_time = end_time - start_time
        records = result.get_main_table()
        num_records = len(records)
        records_per_second = num_records / elapsed_time if elapsed_time > 0 else 0

        return {
            "time": elapsed_time,
            "records": num_records,
            "records_per_second": records_per_second,
            "reader": "native" if force_native else "adaptive",
        }

    finally:
        # Clean up environment variable
        if force_native:
            os.environ.pop("TRANSMOG_FORCE_NATIVE_CSV", None)


def format_number(num: float) -> str:
    """Format large numbers with commas."""
    return f"{num:,.0f}"


def main():
    """Run the CSV reader performance benchmark."""
    print("=== CSV Reader Performance Benchmark ===")
    print(
        "\nThis benchmark compares CSV reader performance across different file sizes."
    )
    print("Testing adaptive reader selection vs forced native reader.\n")

    # Test different file sizes
    test_sizes = [
        (1_000, "Small (1K rows)"),
        (10_000, "Medium (10K rows)"),
        (50_000, "Large (50K rows)"),
        (100_000, "Very Large (100K rows)"),
    ]

    results = []

    for num_records, description in test_sizes:
        print(f"\n--- Testing {description} ---")

        # Create test file
        print(f"Creating CSV file with {format_number(num_records)} records...")
        filepath = create_csv_file(num_records, num_columns=20)
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        print(f"File size: {file_size:.2f} MB")

        try:
            # Test with adaptive reader selection
            print("\nTesting with adaptive reader selection...")
            adaptive_result = benchmark_csv_reader(filepath, force_native=False)
            print(f"  Time: {adaptive_result['time']:.4f}s")
            print(
                f"  Records/sec: {format_number(adaptive_result['records_per_second'])}"
            )

            # Test with forced native reader
            print("\nTesting with forced native reader...")
            native_result = benchmark_csv_reader(filepath, force_native=True)
            print(f"  Time: {native_result['time']:.4f}s")
            print(
                f"  Records/sec: {format_number(native_result['records_per_second'])}"
            )

            # Calculate speedup
            speedup = native_result["time"] / adaptive_result["time"]
            if speedup > 1:
                print(f"\n  → Adaptive is {speedup:.2f}x faster")
            else:
                print(f"\n  → Native is {1 / speedup:.2f}x faster")

            # Store results
            results.append(
                {
                    "size": num_records,
                    "description": description,
                    "file_size_mb": file_size,
                    "adaptive": adaptive_result,
                    "native": native_result,
                    "speedup": speedup,
                }
            )

        finally:
            # Clean up test file
            os.unlink(filepath)

    # Summary
    print("\n" + "=" * 60)
    print("PERFORMANCE SUMMARY")
    print("=" * 60)

    print(
        "\n{:<20} {:<15} {:<15} {:<15}".format(
            "File Size", "Adaptive (s)", "Native (s)", "Winner"
        )
    )
    print("-" * 65)

    for result in results:
        adaptive_time = result["adaptive"]["time"]
        native_time = result["native"]["time"]
        winner = "Adaptive" if result["speedup"] > 1 else "Native"

        print(
            "{:<20} {:<15.4f} {:<15.4f} {:<15}".format(
                result["description"], adaptive_time, native_time, winner
            )
        )

    # Performance recommendations
    print("\n" + "=" * 60)
    print("PERFORMANCE RECOMMENDATIONS")
    print("=" * 60)
    print("\n1. Small files (<10K rows):")
    print("   - Native CSV reader is typically fastest")
    print("   - Use TRANSMOG_FORCE_NATIVE_CSV=true for consistent performance")

    print("\n2. Medium files (10K-100K rows):")
    print("   - Performance depends on available libraries")
    print("   - Native often wins due to lower overhead")

    print("\n3. Large files (>100K rows):")
    print("   - Polars provides best performance")
    print("   - PyArrow is optimized but has columnar-to-row overhead")

    print("\n4. Environment variable for immediate optimization:")
    print("   export TRANSMOG_FORCE_NATIVE_CSV=true")

    print("\n5. For production workloads:")
    print("   - Test with your actual data sizes")
    print("   - Use chunk_size parameter for very large files")

    # Show current reader availability
    print("\n" + "=" * 60)
    print("READER AVAILABILITY")
    print("=" * 60)

    try:
        import pyarrow

        print(f"✓ PyArrow is available (version {pyarrow.__version__})")
    except ImportError:
        print("✗ PyArrow is not available")

    try:
        import polars

        print(f"✓ Polars is available (version {polars.__version__})")
    except ImportError:
        print("✗ Polars is not available")

    print("\nNative CSV reader is always available (Python stdlib)")


if __name__ == "__main__":
    main()
