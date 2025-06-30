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


def run_simple_benchmark(data, name, description, **kwargs):
    """Run a benchmark with the simplified API."""
    print(f"\n=== {description} ===")

    # Clean up memory first
    gc.collect()

    # Measure memory before processing
    memory_before = get_memory_usage()

    # Time the processing
    start_time = time.time()
    result = tm.flatten(data, name=name, **kwargs)
    end_time = time.time()

    # Measure memory after processing
    memory_after = get_memory_usage()
    processing_time = end_time - start_time
    memory_used = memory_after - memory_before

    # Calculate metrics
    total_records = len(result.main) + sum(
        len(table) for table in result.tables.values()
    )
    records_per_second = total_records / processing_time if processing_time > 0 else 0

    # Report results
    print(f"Processing time: {processing_time:.4f} seconds")
    if HAVE_PSUTIL:
        print(f"Memory usage: {memory_after:.2f} MB (delta: {memory_used:.2f} MB)")
    print(f"Tables created: {len(result.tables) + 1}")  # +1 for main table
    print(f"Total records processed: {total_records}")
    print(f"Processing speed: {records_per_second:.2f} records/second")

    return {
        "time": processing_time,
        "memory": memory_used,
        "tables": len(result.tables) + 1,
        "records": total_records,
        "speed": records_per_second,
        "result": result,
    }


def run_advanced_benchmark(data, entity_name, description, config):
    """Run a benchmark with advanced configuration using the Processor class."""
    print(f"\n=== {description} ===")

    # Import the advanced classes for configuration
    from transmog.process import Processor

    # Clean up memory first
    gc.collect()

    # Measure memory before processing
    memory_before = get_memory_usage()

    # Time the processing
    start_time = time.time()
    processor = Processor(config=config)
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

    # Example 1: Default Configuration Baseline (Simplified API)
    default_metrics = run_simple_benchmark(
        data=medium_data,
        name="records",
        description="Default Configuration (Baseline)",
    )

    # Example 2: Simple optimizations with the simple API
    optimized_metrics = run_simple_benchmark(
        data=medium_data,
        name="records",
        description="Optimized Configuration (Simple API)",
        # Use parameters that improve performance
        add_timestamp=False,  # Skip timestamp for better performance
        natural_ids=False,  # Skip natural ID detection for better performance
    )

    # Example 3: Streaming Processing for Memory Efficiency
    print("\n=== Streaming Processing Example ===")
    print("For very large datasets, use streaming processing:")

    # Create a larger dataset to demonstrate streaming
    large_data = generate_test_data(num_records=1000, nesting_depth=2, array_size=3)

    # Streaming processing with the simple API
    streaming_start = time.time()

    # Save directly to file using streaming
    streaming_output = os.path.join(output_dir, "streaming_output.json")
    stream_result = tm.flatten_stream(
        large_data,
        name="records",
        output_path=streaming_output,
        chunk_size=100,  # Process in chunks of 100 records
    )

    streaming_end = time.time()
    streaming_time = streaming_end - streaming_start

    print(f"Streaming processing time: {streaming_time:.4f} seconds")
    print(f"Output saved to: {streaming_output}")

    # Example 4: Advanced Configuration (using Processor directly)
    print("\n=== Advanced Configuration Examples ===")
    print("For advanced performance tuning, you can still access the Processor class:")

    # Import advanced configuration classes
    from transmog.config import TransmogConfig, ProcessingMode

    # Memory-optimized configuration
    memory_config = TransmogConfig.default().with_processing(
        processing_mode=ProcessingMode.LOW_MEMORY,
        batch_size=100,  # Smaller batch size for memory optimization
    )

    memory_metrics = run_advanced_benchmark(
        data=medium_data,
        entity_name="records",
        description="Memory-Optimized Configuration (Advanced)",
        config=memory_config,
    )

    # Performance-optimized configuration
    performance_config = TransmogConfig.default().with_processing(
        processing_mode=ProcessingMode.HIGH_PERFORMANCE,
        batch_size=1000,  # Larger batch size for performance optimization
    )

    performance_metrics = run_advanced_benchmark(
        data=medium_data,
        entity_name="records",
        description="Performance-Optimized Configuration (Advanced)",
        config=performance_config,
    )

    # Custom optimized configuration
    custom_config = (
        TransmogConfig.default()
        .with_processing(
            processing_mode=ProcessingMode.STANDARD,
            batch_size=250,  # Balanced batch size
            skip_null=True,  # Skip null values for performance
            include_empty=False,  # Skip empty values for performance
            cast_to_string=True,  # String conversion can be faster
        )
        .with_naming(
            deeply_nested_threshold=3,  # Better handling of deep nesting
        )
    )

    custom_metrics = run_advanced_benchmark(
        data=medium_data,
        entity_name="records",
        description="Custom Optimized Configuration (Advanced)",
        config=custom_config,
    )

    # Example 5: Configuration Comparison for Different Data Sizes
    print("\n=== Configuration Comparison for Small Dataset ===")

    # Test with small dataset using simple API
    small_default = run_simple_benchmark(
        data=small_data,
        name="records",
        description="Default with Small Dataset",
    )

    small_optimized = run_simple_benchmark(
        data=small_data,
        name="records",
        description="Optimized with Small Dataset",
        add_timestamp=False,
        natural_ids=False,
    )

    # Summarize results
    print("\n=== Performance Summary ===")

    # Simple API comparison
    print("\nSimple API Performance Comparison (Medium Dataset):")
    print(
        f"{'Configuration':<25} {'Time (s)':<10} {'Memory (MB)':<12} "
        f"{'Speed (rec/s)':<15}"
    )
    print("-" * 65)

    simple_results = [
        ("Default", default_metrics),
        ("Optimized", optimized_metrics),
    ]

    for name, metrics in simple_results:
        print(
            f"{name:<25} {metrics['time']:<10.4f} "
            f"{metrics['memory']:<12.2f} {metrics['speed']:<15.2f}"
        )

    # Advanced configuration comparison
    print("\nAdvanced Configuration Performance Comparison (Medium Dataset):")
    print(
        f"{'Configuration':<25} {'Time (s)':<10} {'Memory (MB)':<12} "
        f"{'Speed (rec/s)':<15}"
    )
    print("-" * 65)

    advanced_results = [
        ("Memory-Optimized", memory_metrics),
        ("Performance-Optimized", performance_metrics),
        ("Custom Optimized", custom_metrics),
    ]

    for name, metrics in advanced_results:
        print(
            f"{name:<25} {metrics['time']:<10.4f} "
            f"{metrics['memory']:<12.2f} {metrics['speed']:<15.2f}"
        )

    print("\nSmall Dataset Comparison:")
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
        f"{'Optimized':<25} {small_optimized['time']:<10.4f} "
        f"{small_optimized['memory']:<12.2f} {small_optimized['speed']:<15.2f}"
    )

    # Save some results for analysis
    print("\n=== Saving Results ===")
    default_metrics["result"].save(os.path.join(output_dir, "default_result.json"))
    optimized_metrics["result"].save(os.path.join(output_dir, "optimized_result.json"))

    # Key observations
    print("\nKey Observations:")
    print("1. Simple API optimizations can provide significant performance gains")
    print("2. Streaming processing is ideal for very large datasets")
    print("3. Advanced configuration provides fine-grained control")
    print("4. For small datasets, the performance difference may be negligible")
    print("5. Memory vs. performance tradeoffs depend on your specific use case")

    # Recommendations
    print("\nRecommendations:")
    print("- For most use cases: Use the simple API with basic optimizations")
    print("- For large datasets: Use flatten_stream() for memory efficiency")
    print("- For fine-tuning: Access Processor class for advanced configuration")
    print("- For memory constraints: Use smaller chunk sizes and streaming")
    print("- For speed priority: Disable timestamp and natural ID detection")

    print("\nPerformance Optimization Tips:")
    print("- Set add_timestamp=False if you don't need timestamps")
    print("- Set natural_ids=False if you don't need natural ID detection")
    print("- Use flatten_stream() for datasets that don't fit in memory")
    print("- For advanced control, use the Processor class with custom configs")
    print("- Consider your memory vs. speed tradeoffs based on your infrastructure")


if __name__ == "__main__":
    main()
