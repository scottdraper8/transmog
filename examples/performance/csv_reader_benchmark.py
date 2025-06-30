"""Example Name: CSV Reader Performance Benchmark.

Demonstrates: Benchmarking different CSV reader implementations in Transmog v1.1.0.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/processing/csv-processing.html
- https://transmog.readthedocs.io/en/latest/api/csv-reader.html

Learning Objectives:
- Compare performance of different CSV reader implementations
- Understand when to use each reader
- Learn about performance optimization techniques
- See the impact of file size on reader selection
- Show both simple API and advanced Processor usage for benchmarking
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


def benchmark_csv_simple_api(
    filepath: str, force_native: bool = False
) -> dict[str, Any]:
    """Benchmark CSV reader performance using the simple API."""
    # Set environment variable if forcing native reader
    if force_native:
        os.environ["TRANSMOG_FORCE_NATIVE_CSV"] = "true"

    try:
        # Time the processing using simple API
        start_time = time.perf_counter()
        result = tm.flatten_file(filepath, name="benchmark_data")
        end_time = time.perf_counter()

        # Calculate metrics
        elapsed_time = end_time - start_time
        records = result.main
        num_records = len(records)
        records_per_second = num_records / elapsed_time if elapsed_time > 0 else 0

        return {
            "time": elapsed_time,
            "records": num_records,
            "records_per_second": records_per_second,
            "reader": "native" if force_native else "adaptive",
            "api": "simple",
        }

    finally:
        # Clean up environment variable
        if force_native:
            os.environ.pop("TRANSMOG_FORCE_NATIVE_CSV", None)


def benchmark_csv_advanced_api(
    filepath: str, force_native: bool = False
) -> dict[str, Any]:
    """Benchmark CSV reader performance using the advanced Processor API."""
    # Set environment variable if forcing native reader
    if force_native:
        os.environ["TRANSMOG_FORCE_NATIVE_CSV"] = "true"

    try:
        # For advanced benchmarking, use the Processor directly
        from transmog.process import Processor

        processor = Processor()

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
            "api": "advanced",
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
    print("=== CSV Reader Performance Benchmark (Transmog v1.1.0) ===")
    print(
        "\nThis benchmark compares CSV reader performance across different file sizes"
    )
    print("and APIs. Testing adaptive reader selection vs forced native reader.")
    print("Also comparing the new simple API vs advanced Processor API.\n")

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
            # Test simple API with adaptive reader selection
            print("\nTesting simple API with adaptive reader...")
            simple_adaptive = benchmark_csv_simple_api(filepath, force_native=False)
            print(f"  Time: {simple_adaptive['time']:.4f}s")
            print(
                f"  Records/sec: {format_number(simple_adaptive['records_per_second'])}"
            )

            # Test simple API with forced native reader
            print("\nTesting simple API with native reader...")
            simple_native = benchmark_csv_simple_api(filepath, force_native=True)
            print(f"  Time: {simple_native['time']:.4f}s")
            print(
                f"  Records/sec: {format_number(simple_native['records_per_second'])}"
            )

            # Test advanced API with adaptive reader selection
            print("\nTesting advanced API with adaptive reader...")
            advanced_adaptive = benchmark_csv_advanced_api(filepath, force_native=False)
            print(f"  Time: {advanced_adaptive['time']:.4f}s")
            print(
                f"  Records/sec: {format_number(advanced_adaptive['records_per_second'])}"
            )

            # Test advanced API with forced native reader
            print("\nTesting advanced API with native reader...")
            advanced_native = benchmark_csv_advanced_api(filepath, force_native=True)
            print(f"  Time: {advanced_native['time']:.4f}s")
            print(
                f"  Records/sec: {format_number(advanced_native['records_per_second'])}"
            )

            # Calculate speedups
            simple_speedup = simple_native["time"] / simple_adaptive["time"]
            advanced_speedup = advanced_native["time"] / advanced_adaptive["time"]
            api_speedup = advanced_adaptive["time"] / simple_adaptive["time"]

            print(
                f"\n  → Simple API: {'Adaptive' if simple_speedup > 1 else 'Native'} is {max(simple_speedup, 1 / simple_speedup):.2f}x faster"
            )
            print(
                f"  → Advanced API: {'Adaptive' if advanced_speedup > 1 else 'Native'} is {max(advanced_speedup, 1 / advanced_speedup):.2f}x faster"
            )
            print(
                f"  → API Comparison: {'Advanced' if api_speedup > 1 else 'Simple'} API is {max(api_speedup, 1 / api_speedup):.2f}x faster"
            )

            # Store results
            results.append(
                {
                    "size": num_records,
                    "description": description,
                    "file_size_mb": file_size,
                    "simple_adaptive": simple_adaptive,
                    "simple_native": simple_native,
                    "advanced_adaptive": advanced_adaptive,
                    "advanced_native": advanced_native,
                    "simple_speedup": simple_speedup,
                    "advanced_speedup": advanced_speedup,
                    "api_speedup": api_speedup,
                }
            )

        finally:
            # Clean up test file
            os.unlink(filepath)

    # Summary
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)

    print(
        "\n{:<20} {:<12} {:<12} {:<12} {:<12} {:<10}".format(
            "File Size", "Simple-A", "Simple-N", "Adv-A", "Adv-N", "Best"
        )
    )
    print("-" * 78)

    for result in results:
        simple_a = result["simple_adaptive"]["time"]
        simple_n = result["simple_native"]["time"]
        adv_a = result["advanced_adaptive"]["time"]
        adv_n = result["advanced_native"]["time"]

        times = [
            ("Simple-A", simple_a),
            ("Simple-N", simple_n),
            ("Adv-A", adv_a),
            ("Adv-N", adv_n),
        ]
        best = min(times, key=lambda x: x[1])[0]

        print(
            "{:<20} {:<12.4f} {:<12.4f} {:<12.4f} {:<12.4f} {:<10}".format(
                result["description"], simple_a, simple_n, adv_a, adv_n, best
            )
        )

    # Performance recommendations
    print("\n" + "=" * 80)
    print("PERFORMANCE RECOMMENDATIONS")
    print("=" * 80)
    print("\n1. API Selection:")
    print("   - Simple API (tm.flatten_file): Use for most applications")
    print("   - Advanced API (Processor): Use when you need custom configuration")
    print("   - Performance difference is typically minimal")

    print("\n2. Reader Selection by File Size:")
    print("   - Small files (<10K rows): Native CSV reader is typically fastest")
    print(
        "   - Medium files (10K-100K rows): Performance depends on available libraries"
    )
    print(
        "   - Large files (>100K rows): Polars/PyArrow can provide better performance"
    )

    print("\n3. Environment Optimization:")
    print("   - Use TRANSMOG_FORCE_NATIVE_CSV=true for consistent performance")
    print("   - Test with your actual data patterns and sizes")

    print("\n4. Memory Considerations:")
    print("   - For very large files, consider streaming:")
    print("     tm.flatten_stream(data, output_dir, format='csv')")

    print("\n5. Production Recommendations:")
    print("   - Use the simple API unless you need advanced features")
    print("   - Profile with your actual data before optimizing")
    print("   - Consider chunked processing for memory efficiency")

    # Show current reader availability
    print("\n" + "=" * 80)
    print("READER AVAILABILITY")
    print("=" * 80)

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

    print("✓ Native CSV reader is always available (Python stdlib)")

    # Example usage patterns
    print("\n" + "=" * 80)
    print("EXAMPLE USAGE PATTERNS")
    print("=" * 80)
    print("\n# Simple API (recommended for most use cases):")
    print("import transmog as tm")
    print("result = tm.flatten_file('data.csv', name='my_data')")
    print("result.save('output.json')")

    print("\n# Advanced API (for custom configuration):")
    print("from transmog.process import Processor")
    print("from transmog.config import TransmogConfig")
    print("config = TransmogConfig.default().with_processing(cast_to_string=False)")
    print("processor = Processor(config)")
    print("result = processor.process_file('data.csv', entity_name='my_data')")

    print("\n# Streaming (for very large files):")
    print("tm.flatten_stream(data, 'output/', name='big_data', format='parquet')")


if __name__ == "__main__":
    main()
