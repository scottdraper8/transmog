"""Example Name: Performance Optimization.

Demonstrates: Techniques for optimizing Transmog performance.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/advanced/performance-optimization.html
- https://transmog.readthedocs.io/en/latest/tutorials/advanced/optimizing-memory-usage.html

Learning Objectives:
- How to optimize processing for different performance goals
- How to measure and compare processing performance
- How to balance memory usage and processing speed
- How to select appropriate configuration for different data sizes
"""

import gc
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


def get_memory_usage():
    """Get current memory usage in MB."""
    if HAVE_PSUTIL:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    return 0


def generate_test_data(num_records, nesting_depth=3, array_size=5):
    """Generate test data with specified complexity."""
    print(
        f"Generating {num_records} records with nesting depth {nesting_depth} "
        f"and array size {array_size}..."
    )

    def create_nested_object(depth, prefix="field"):
        """Create a nested object with specified depth."""
        if depth <= 0:
            return {"value": prefix}

        obj = {
            "id": depth,
            "name": f"{prefix}_name_{depth}",
            "active": depth % 2 == 0,
            "score": float(depth) / 10.0,
        }

        # Add nested object
        obj["nested"] = create_nested_object(depth - 1, f"{prefix}_sub")

        # Add array of objects
        if depth > 1:
            obj["items"] = [
                {
                    "id": i,
                    "name": f"item_{depth}_{i}",
                    "details": create_nested_object(depth - 2, f"item_detail_{i}"),
                }
                for i in range(array_size)
            ]

        return obj

    # Generate records
    records = []
    for i in range(num_records):
        record = {
            "id": i,
            "name": f"Record {i}",
            "timestamp": "2023-01-01T12:00:00Z",
            "data": create_nested_object(nesting_depth, f"record_{i}"),
        }
        records.append(record)

    return records


def run_benchmark(processor, data, entity_name, description):
    """Run a benchmark with the given processor and data."""
    print(f"\n=== {description} ===")

    # Clean up memory first
    gc.collect()

    # Measure memory before processing
    memory_before = get_memory_usage()

    # Time the processing
    start_time = time.time()
    result = processor.process(data=data, entity_name=entity_name)
    end_time = time.time()

    # Measure memory after processing
    memory_after = get_memory_usage()
    processing_time = end_time - start_time
    memory_used = memory_after - memory_before

    # Calculate metrics
    total_records = sum(len(table) for table in result.to_dict().values())
    records_per_second = total_records / processing_time if processing_time > 0 else 0

    # Report results
    print(f"Processing time: {processing_time:.4f} seconds")
    if HAVE_PSUTIL:
        print(f"Memory usage: {memory_after:.2f} MB (delta: {memory_used:.2f} MB)")
    print(f"Tables created: {len(result.get_table_names()) + 1}")  # +1 for main table
    print(f"Total records processed: {total_records}")
    print(f"Processing speed: {records_per_second:.2f} records/second")

    return {
        "time": processing_time,
        "memory": memory_used,
        "tables": len(result.get_table_names()) + 1,
        "records": total_records,
        "speed": records_per_second,
    }


def main():
    """Run the performance optimization example."""
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "performance"
    )
    os.makedirs(output_dir, exist_ok=True)

    print("=== Performance Optimization Example ===")

    # Measure initial memory usage
    initial_memory = get_memory_usage()
    print(f"Initial memory usage: {initial_memory:.2f} MB")

    # Generate test data sets of different sizes
    small_data = generate_test_data(num_records=100, nesting_depth=3, array_size=3)
    medium_data = generate_test_data(num_records=500, nesting_depth=3, array_size=5)

    # Example 1: Default Configuration Baseline
    default_processor = tm.Processor()
    default_metrics = run_benchmark(
        processor=default_processor,
        data=medium_data,
        entity_name="records",
        description="Default Configuration (Baseline)",
    )

    # Example 2: Memory-Optimized Configuration
    memory_processor = tm.Processor(
        tm.TransmogConfig.default().with_processing(
            processing_mode=ProcessingMode.MEMORY_OPTIMIZED,
            batch_size=100,  # Smaller batch size for memory optimization
        )
    )

    memory_metrics = run_benchmark(
        processor=memory_processor,
        data=medium_data,
        entity_name="records",
        description="Memory-Optimized Configuration",
    )

    # Example 3: Performance-Optimized Configuration
    performance_processor = tm.Processor(
        tm.TransmogConfig.default().with_processing(
            processing_mode=ProcessingMode.PERFORMANCE_OPTIMIZED,
            batch_size=1000,  # Larger batch size for performance optimization
        )
    )

    performance_metrics = run_benchmark(
        processor=performance_processor,
        data=medium_data,
        entity_name="records",
        description="Performance-Optimized Configuration",
    )

    # Example 4: Custom Optimized Configuration
    custom_processor = tm.Processor(
        tm.TransmogConfig.default()
        .with_processing(
            processing_mode=ProcessingMode.STANDARD,
            batch_size=250,  # Balanced batch size
            skip_null=True,  # Skip null values for performance
            include_empty=False,  # Skip empty values for performance
            cast_to_string=True,  # String conversion can be faster than type
            # preservation
        )
        .with_naming(
            deep_nesting_threshold=3,  # Better handling of deep nesting
            max_table_component_length=8,  # Limit name length
        )
    )

    custom_metrics = run_benchmark(
        processor=custom_processor,
        data=medium_data,
        entity_name="records",
        description="Custom Optimized Configuration",
    )

    # Example 5: Configuration Comparison for Different Data Sizes
    print("\n=== Configuration Comparison for Small Dataset ===")

    # Test with small dataset
    small_default = run_benchmark(
        processor=default_processor,
        data=small_data,
        entity_name="records",
        description="Default with Small Dataset",
    )

    small_memory = run_benchmark(
        processor=memory_processor,
        data=small_data,
        entity_name="records",
        description="Memory-Optimized with Small Dataset",
    )

    small_performance = run_benchmark(
        processor=performance_processor,
        data=small_data,
        entity_name="records",
        description="Performance-Optimized with Small Dataset",
    )

    # Summarize results
    print("\n=== Performance Summary ===")

    # Create comparison table header
    print("\nPerformance Comparison (Medium Dataset):")
    print(
        f"{'Configuration':<25} {'Time (s)':<10} {'Memory (MB)':<12} "
        f"{'Speed (rec/s)':<15}"
    )
    print("-" * 65)

    # Add comparison data rows
    print(
        f"{'Default':<25} {default_metrics['time']:<10.4f} "
        f"{default_metrics['memory']:<12.2f} {default_metrics['speed']:<15.2f}"
    )
    print(
        f"{'Memory-Optimized':<25} {memory_metrics['time']:<10.4f} "
        f"{memory_metrics['memory']:<12.2f} {memory_metrics['speed']:<15.2f}"
    )
    print(
        f"{'Performance-Optimized':<25} {performance_metrics['time']:<10.4f} "
        f"{performance_metrics['memory']:<12.2f} {performance_metrics['speed']:<15.2f}"
    )
    print(
        f"{'Custom Optimized':<25} {custom_metrics['time']:<10.4f} "
        f"{custom_metrics['memory']:<12.2f} {custom_metrics['speed']:<15.2f}"
    )

    print("\nPerformance Comparison (Small Dataset):")
    print(
        f"{'Configuration':<25} {'Time (s)':<10} {'Memory (MB)':<12} "
        f"{'Speed (rec/s)':<15}"
    )
    print("-" * 65)
    print(
        f"{'Default':<25} {small_default['time']:<10.4f} "
        f"{small_default['memory']:<12.2f} {small_default['speed']:<15.2f}"
    )
    print(
        f"{'Memory-Optimized':<25} {small_memory['time']:<10.4f} "
        f"{small_memory['memory']:<12.2f} {small_memory['speed']:<15.2f}"
    )
    print(
        f"{'Performance-Optimized':<25} {small_performance['time']:<10.4f} "
        f"{small_performance['memory']:<12.2f} {small_performance['speed']:<15.2f}"
    )

    # Key observations
    print("\nKey Observations:")
    print("1. Memory-optimized configuration reduces memory usage but may be slower")
    print("2. Performance-optimized configuration is faster but uses more memory")
    print("3. For small datasets, the performance difference may be negligible")
    print(
        "4. Custom optimization can balance memory and performance for specific "
        "use cases"
    )

    # Recommendations
    print("\nRecommendations:")
    print("- For small datasets: Default configuration is sufficient")
    print(
        "- For large datasets with memory constraints: Memory-optimized configuration"
    )
    print(
        "- For large datasets with performance priority: Performance-optimized "
        "configuration"
    )
    print("- For streaming: Memory-optimized configuration with appropriate batch size")

    print("\nPerformance Optimization Parameters:")
    print("- processing_mode: Controls overall processing approach")
    print("- batch_size: Controls memory vs. performance tradeoff")
    print("- skip_null and include_empty: Affects processing speed and output size")
    print("- cast_to_string: Type conversion affects processing speed")
    print("- deep_nesting_threshold: Affects how deeply nested structures are handled")


if __name__ == "__main__":
    main()
