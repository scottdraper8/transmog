"""Example Name: Processing Modes.

Demonstrates: Different processing modes in Transmog.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/processing/processing-overview.html
- https://transmog.readthedocs.io/en/latest/api/process.html

Learning Objectives:
- Understanding the different processing modes
- How to select appropriate modes for different data sizes
- Trade-offs between memory usage and performance
- How to measure processing performance
"""

import os
import time

# Try to import psutil for memory measurements
try:
    import psutil

    HAVE_PSUTIL = True
except ImportError:
    HAVE_PSUTIL = False
    print("psutil not installed. Memory usage measurements not available.")
    print("Install with: pip install psutil")

# Import from transmog package
import transmog as tm
from transmog import ProcessingMode


def get_memory_usage_mb():
    """Get current memory usage in MB."""
    if HAVE_PSUTIL:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    return 0


def generate_test_data(num_records=100):
    """Generate test data with the specified number of records."""
    records = []
    for i in range(num_records):
        record = {
            "id": i,
            "name": f"Record {i}",
            "tags": ["test", "sample", "data"],
            "details": {
                "created": "2023-01-01",
                "updated": "2023-06-30",
                "status": "active" if i % 2 == 0 else "inactive",
                "metadata": {
                    "version": "1.0",
                    "source": "test generator",
                    "params": {"seed": 123, "factor": 0.5},
                },
            },
            "items": [
                {"name": f"Item {j}", "value": j * 10, "active": j % 2 == 0}
                for j in range(5)
            ],
        }
        records.append(record)
    return records


def main():
    """Run the processing modes example."""
    print("=== Processing Modes Example ===")

    # Generate dataset for testing
    print("\nGenerating test data...")
    medium_data = generate_test_data(num_records=100)
    large_data = generate_test_data(num_records=1000)

    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "processing_modes"
    )
    os.makedirs(output_dir, exist_ok=True)

    print("\n=== Standard Processing Mode ===")
    print("Description: Balances memory usage and performance")

    # Measure memory before
    mem_before = get_memory_usage_mb()

    # Time the standard processing
    start_time = time.time()
    processor = tm.Processor(
        tm.TransmogConfig.default().with_processing(
            processing_mode=ProcessingMode.STANDARD
        )
    )
    result = processor.process(data=medium_data, entity_name="records")
    end_time = time.time()

    # Measure memory after
    mem_after = get_memory_usage_mb()

    # Report results
    print(f"Processing time: {end_time - start_time:.4f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {mem_after:.2f} MB (delta: {mem_after - mem_before:.2f} MB)"
        )
    print(f"Tables created: {len(result.get_table_names()) + 1}")

    # Write results
    result.write_all_json(os.path.join(output_dir, "standard"))

    print("\n=== Memory-Optimized Processing Mode ===")
    print("Description: Reduces memory usage at the cost of some performance")

    # Clear previous result to free memory
    result = None

    # Measure memory before
    mem_before = get_memory_usage_mb()

    # Time the memory-optimized processing
    start_time = time.time()
    processor = tm.Processor(
        tm.TransmogConfig.default().with_processing(
            processing_mode=ProcessingMode.MEMORY_OPTIMIZED
        )
    )
    result = processor.process(data=large_data, entity_name="records")
    end_time = time.time()

    # Measure memory after
    mem_after = get_memory_usage_mb()

    # Report results
    print(f"Processing time: {end_time - start_time:.4f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {mem_after:.2f} MB (delta: {mem_after - mem_before:.2f} MB)"
        )
    print(f"Tables created: {len(result.get_table_names()) + 1}")

    # Write results
    result.write_all_json(os.path.join(output_dir, "memory_optimized"))

    print("\n=== Performance-Optimized Processing Mode ===")
    print("Description: Maximizes speed at the cost of higher memory usage")

    # Clear previous result to free memory
    result = None

    # Measure memory before
    mem_before = get_memory_usage_mb()

    # Time the performance-optimized processing
    start_time = time.time()
    processor = tm.Processor(
        tm.TransmogConfig.default().with_processing(
            processing_mode=ProcessingMode.PERFORMANCE_OPTIMIZED
        )
    )
    result = processor.process(data=large_data, entity_name="records")
    end_time = time.time()

    # Measure memory after
    mem_after = get_memory_usage_mb()

    # Report results
    print(f"Processing time: {end_time - start_time:.4f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {mem_after:.2f} MB (delta: {mem_after - mem_before:.2f} MB)"
        )
    print(f"Tables created: {len(result.get_table_names()) + 1}")

    # Write results
    result.write_all_json(os.path.join(output_dir, "performance_optimized"))

    print("\n=== Streaming Processing Mode ===")
    print("Description: Processes data in a streaming fashion with minimal memory")

    # Clear previous result to free memory
    result = None

    # Measure memory before
    mem_before = get_memory_usage_mb()

    # Time the streaming processing
    start_time = time.time()
    processor = tm.Processor()

    # Use stream_process to write directly to files
    processor.stream_process(
        data=large_data,
        entity_name="records",
        output_format="json",
        output_destination=os.path.join(output_dir, "streaming"),
        indent=2,  # Format JSON with indentation
    )
    end_time = time.time()

    # Measure memory after
    mem_after = get_memory_usage_mb()

    # Report results
    print(f"Processing time: {end_time - start_time:.4f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {mem_after:.2f} MB (delta: {mem_after - mem_before:.2f} MB)"
        )

    # Count files created
    streaming_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "streaming"))
        if f.endswith(".json")
    ]
    print(f"Files created: {len(streaming_files)}")

    print("\n=== Mode Comparison ===")
    print("Standard Mode: Balanced memory and performance")
    print("Memory-Optimized Mode: Lower memory usage, may be slower")
    print("Performance-Optimized Mode: Faster processing, higher memory usage")
    print("Streaming Mode: Minimal memory usage, directly writes to outputs")

    print(f"\nOutput files written to: {output_dir}")


if __name__ == "__main__":
    main()
