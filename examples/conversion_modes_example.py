"""Conversion Modes Example for Transmog.

This example demonstrates the three conversion modes available in Transmog
and their performance characteristics, memory usage, and appropriate use cases:

1. EAGER - Convert immediately and cache results (default)
2. LAZY - Convert only when needed without caching
3. MEMORY_EFFICIENT - Clear intermediate data after conversion

Each mode has different tradeoffs for memory usage and performance.
"""

import os
import sys
import time

import psutil

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog package
from transmog import ProcessingMode, Processor, TransmogConfig
from transmog.process.result import ConversionMode


def generate_test_data(num_records=1000):
    """Generate test data with nested structures."""
    data = []
    for i in range(num_records):
        record = {
            "id": i,
            "name": f"Record {i}",
            "details": {
                "created_at": "2023-10-15T12:00:00Z",
                "status": "active" if i % 2 == 0 else "inactive",
                "tags": ["sample", "test", "example"],
                "score": i / 10.0,
                "metadata": {
                    "source": "generated",
                    "version": "1.0",
                    "priority": i % 5,
                },
            },
            "items": [
                {
                    "item_id": j,
                    "name": f"Item {j}",
                    "quantity": j * 2,
                    "price": j * 10.5,
                    "categories": ["cat-A", "cat-B", "cat-C"],
                }
                for j in range(1, 4)
            ],
            "contacts": [
                {"type": "email", "value": f"user{i}@example.com", "primary": True},
                {"type": "phone", "value": f"555-{i:04d}", "primary": False},
            ],
        }
        data.append(record)
    return data


def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def print_header(title):
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, "="))
    print("=" * 80)


def measure_performance(function, *args, **kwargs):
    """Measure performance and memory usage of a function."""
    # Record starting memory
    start_memory = get_memory_usage()

    # Time the operation
    start_time = time.time()
    result = function(*args, **kwargs)
    end_time = time.time()

    # Record ending memory
    end_memory = get_memory_usage()

    return {
        "result": result,
        "time": end_time - start_time,
        "memory_start": start_memory,
        "memory_end": end_memory,
        "memory_delta": end_memory - start_memory,
    }


def main():
    """Run the conversion modes example."""
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "output", "conversion_modes")
    os.makedirs(output_dir, exist_ok=True)

    # Generate test data
    print("Generating test data...")
    data = generate_test_data(num_records=5000)
    print(f"Generated {len(data)} records")

    # Initial memory usage
    initial_memory = get_memory_usage()
    print(f"Initial memory usage: {initial_memory:.2f} MB")

    # Process the data once to get a result object
    print("Processing data...")
    config = TransmogConfig.default().with_processing(
        visit_arrays=True, processing_mode=ProcessingMode.DEFAULT
    )
    processor = Processor(config)
    result = processor.process(data, entity_name="records")
    print(f"Processed {len(result.get_main_table())} main records")
    print(f"Child tables: {', '.join(result.get_table_names())}")

    # Clone the result with different conversion modes
    eager_result = result.with_conversion_mode(ConversionMode.EAGER)  # Default
    lazy_result = result.with_conversion_mode(ConversionMode.LAZY)
    memory_efficient_result = result.with_conversion_mode(
        ConversionMode.MEMORY_EFFICIENT
    )

    print_header("1. EAGER Mode (Default)")
    print(
        "EAGER mode converts data immediately and caches results for "
        "faster repeated access."
    )
    print("Best for: Interactive use, smaller datasets, multiple format conversions")

    # First conversion (not cached yet)
    print("\nFirst conversion to JSON bytes:")
    eager_first = measure_performance(eager_result.to_json_bytes)
    print(f"  Time: {eager_first['time']:.4f} seconds")
    print(f"  Memory change: {eager_first['memory_delta']:.2f} MB")

    # Second conversion (should use cache)
    print("\nSecond conversion to JSON bytes (should use cache):")
    eager_second = measure_performance(eager_result.to_json_bytes)
    print(f"  Time: {eager_second['time']:.4f} seconds")
    print(f"  Memory change: {eager_second['memory_delta']:.2f} MB")
    print(
        f"  Speed improvement: "
        f"{eager_first['time'] / max(eager_second['time'], 0.0001):.2f}x faster"
    )

    # Different format
    print("\nConversion to CSV bytes:")
    eager_csv = measure_performance(eager_result.to_csv_bytes)
    print(f"  Time: {eager_csv['time']:.4f} seconds")
    print(f"  Memory change: {eager_csv['memory_delta']:.2f} MB")

    print_header("2. LAZY Mode")
    print("LAZY mode converts data only when needed, without caching results.")
    print("Best for: One-time conversions, moderate-sized datasets")

    # First conversion
    print("\nFirst conversion to JSON bytes:")
    lazy_first = measure_performance(lazy_result.to_json_bytes)
    print(f"  Time: {lazy_first['time']:.4f} seconds")
    print(f"  Memory change: {lazy_first['memory_delta']:.2f} MB")

    # Second conversion (should be similar to first)
    print("\nSecond conversion to JSON bytes (no caching):")
    lazy_second = measure_performance(lazy_result.to_json_bytes)
    print(f"  Time: {lazy_second['time']:.4f} seconds")
    print(f"  Memory change: {lazy_second['memory_delta']:.2f} MB")
    print(
        f"  Ratio to first conversion: {lazy_second['time'] / lazy_first['time']:.2f}x"
    )

    # Different format
    print("\nConversion to CSV bytes:")
    lazy_csv = measure_performance(lazy_result.to_csv_bytes)
    print(f"  Time: {lazy_csv['time']:.4f} seconds")
    print(f"  Memory change: {lazy_csv['memory_delta']:.2f} MB")

    print_header("3. MEMORY_EFFICIENT Mode")
    print("MEMORY_EFFICIENT mode minimizes memory usage by clearing intermediate data.")
    print("Best for: Large datasets, memory-constrained environments")

    # First conversion
    print("\nFirst conversion to JSON bytes:")
    efficient_first = measure_performance(memory_efficient_result.to_json_bytes)
    print(f"  Time: {efficient_first['time']:.4f} seconds")
    print(f"  Memory change: {efficient_first['memory_delta']:.2f} MB")

    # Second conversion (should reconvert from source)
    print("\nSecond conversion to JSON bytes (reconverts from source):")
    efficient_second = measure_performance(memory_efficient_result.to_json_bytes)
    print(f"  Time: {efficient_second['time']:.4f} seconds")
    print(f"  Memory change: {efficient_second['memory_delta']:.2f} MB")

    # Write to file with memory efficiency
    print("\nWriting to files with memory efficiency:")
    # Measure time/memory for file writing
    file_write_metrics = measure_performance(
        memory_efficient_result.write_all_csv,
        os.path.join(output_dir, "memory_efficient"),
    )
    print(f"  Time: {file_write_metrics['time']:.4f} seconds")
    print(f"  Memory change: {file_write_metrics['memory_delta']:.2f} MB")
    print(f"  Output written to: {os.path.join(output_dir, 'memory_efficient')}")

    print_header("4. Comparison Summary")

    # Memory usage comparison
    print("\nMemory Usage Comparison:")
    print(
        f"  EAGER mode:           "
        f"{eager_first['memory_delta'] + eager_csv['memory_delta']:.2f} MB"
    )
    print(
        f"  LAZY mode:            "
        f"{lazy_first['memory_delta'] + lazy_csv['memory_delta']:.2f} MB"
    )
    print(
        f"  MEMORY_EFFICIENT mode: "
        f"{efficient_first['memory_delta'] + file_write_metrics['memory_delta']:.2f} MB"
    )

    # Speed comparison for repeated operations
    print("\nSpeed Comparison for Repeated Operations:")
    print(
        f"  EAGER mode:           {eager_second['time']:.4f} seconds "
        f"(fastest for repeated access)"
    )
    print(f"  LAZY mode:            {lazy_second['time']:.4f} seconds")
    print(f"  MEMORY_EFFICIENT mode: {efficient_second['time']:.4f} seconds")

    print_header("5. Practical Conversion Mode Selection Guide")
    print("When to use each conversion mode:")

    print("\nUse EAGER mode (default) when:")
    print("- Working with smaller datasets")
    print("- In interactive environments (notebooks, exploratory analysis)")
    print("- Converting to multiple formats repeatedly")
    print("- Memory is not a concern")

    print("\nUse LAZY mode when:")
    print("- Converting data only once")
    print("- Working with moderate-sized datasets")
    print("- Need balance between memory usage and performance")

    print("\nUse MEMORY_EFFICIENT mode when:")
    print("- Working with very large datasets")
    print("- In memory-constrained environments")
    print("- Memory efficiency is more important than speed")
    print("- Generating output files is the primary goal")

    print("\nExample selection code:")
    print("""
    # Choose mode based on dataset size
    from transmog import Processor, TransmogConfig, ProcessingMode
    from transmog.process.result import ConversionMode

    # For small datasets (default)
    processor = Processor(TransmogConfig.default())
    result = processor.process(data)  # Uses EAGER conversion by default

    # For medium datasets
    result = processor.process(data).with_conversion_mode(ConversionMode.LAZY)

    # For large datasets
    result = processor.process(data).with_conversion_mode(
        ConversionMode.MEMORY_EFFICIENT
    )

    # Or set directly on the processor config
    config = TransmogConfig.default().with_result(
        conversion_mode=ConversionMode.MEMORY_EFFICIENT
    )
    processor = Processor(config)
    result = processor.process(data)  # Already uses memory-efficient mode

    # For streaming processing of very large datasets
    processor.stream_process(
        data,
        entity_name="records",
        output_format="csv",
        output_destination="./output"
    )
    """)

    print(f"\nAll results written to: {output_dir}")


if __name__ == "__main__":
    main()
