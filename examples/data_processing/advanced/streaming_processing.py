"""Example Name: Streaming Processing v1.1.0.

Demonstrates: Memory-efficient streaming data processing with both simple and advanced APIs.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/advanced/streaming.html
- https://transmog.readthedocs.io/en/latest/tutorials/intermediate/streaming-large-datasets.html

Learning Objectives:
- How to use the new simple API for basic cases
- How to access advanced streaming features for large datasets
- How to configure streaming behavior for memory efficiency
- How to monitor performance metrics for streaming processes

Note: This example shows both the new v1.1.0 API and advanced streaming features.
For most use cases, the simple API is sufficient. For very large datasets or
direct-to-file streaming, use the advanced Processor API.
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


def generate_large_dataset(num_records=10000):
    """Generate a large dataset for testing memory usage."""
    print(f"Generating {num_records} records for testing...")

    records = []
    for i in range(num_records):
        record = {
            "id": i,
            "name": f"Record {i}",
            "details": {
                "created_at": "2023-05-15T12:00:00Z",
                "status": "active" if i % 2 == 0 else "inactive",
                "tags": ["test", "large", "dataset"],
                "score": i / 100.0,
            },
            "contacts": [
                {
                    "type": "email",
                    "value": f"user{i}@example.com",
                    "metadata": {
                        "verified": True,
                        "primary": True,
                        "subscription_status": "subscribed",
                    },
                },
                {
                    "type": "phone",
                    "value": f"555-{i:04d}",
                    "metadata": {
                        "verified": i % 3 == 0,
                        "primary": False,
                        "subscription_status": "unsubscribed",
                    },
                },
            ],
            "items": [
                {"id": j, "name": f"Item {j}", "quantity": j * 2} for j in range(5)
            ],
        }
        records.append(record)

    return records


def main():
    """Run the streaming processing example."""
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "output")
    os.makedirs(output_dir, exist_ok=True)

    # Create subdirectories for different streaming examples
    os.makedirs(os.path.join(output_dir, "streaming", "simple"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "streaming", "advanced_json"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "streaming", "advanced_csv"), exist_ok=True)
    os.makedirs(
        os.path.join(output_dir, "streaming", "advanced_parquet"), exist_ok=True
    )

    # Generate test data
    data = generate_large_dataset(num_records=5000)
    print(f"Generated {len(data)} records")

    # Measure initial memory usage
    initial_memory = get_memory_usage()
    print(f"Initial memory usage: {initial_memory:.2f} MB")

    # Example 1: Simple API processing (recommended for most use cases)
    print("\n=== Simple API Processing (v1.1.0) ===")
    print("Description: Uses the new simple API for easy processing")

    start_time = time.time()
    memory_before = get_memory_usage()

    # Use the simple API
    result = tm.flatten(data, name="records", low_memory=True)

    # Save results easily
    result.save(os.path.join(output_dir, "streaming", "simple", "output.json"))

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {memory_after:.2f} MB "
            f"(delta: {memory_after - memory_before:.2f} MB)"
        )
    print(f"Tables created: {len(result.all_tables)}")
    print(f"Total records: {sum(len(table) for table in result.all_tables.values())}")

    # Example 2: Advanced streaming to JSON (for very large datasets)
    print("\n=== Advanced Streaming to JSON ===")
    print("Description: Direct streaming to files for minimal memory usage")
    print("Use this for datasets too large to fit in memory")

    # Clean up memory first
    result = None
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # For advanced streaming, use the Processor directly
    from transmog.process import Processor

    processor = Processor()

    # Process with streaming API directly to JSON
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="json",
        output_destination=os.path.join(output_dir, "streaming", "advanced_json"),
        indent=2,  # Format JSON with indentation
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {memory_after:.2f} MB "
            f"(delta: {memory_after - memory_before:.2f} MB)"
        )

    # Count output files
    json_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "streaming", "advanced_json"))
        if f.endswith(".json")
    ]
    print(f"JSON files created: {len(json_files)}")

    # Example 3: Advanced streaming to CSV
    print("\n=== Advanced Streaming to CSV ===")
    print("Description: Direct streaming to CSV files")

    # Clean up memory first
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Process with streaming API directly to CSV
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="csv",
        output_destination=os.path.join(output_dir, "streaming", "advanced_csv"),
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {memory_after:.2f} MB "
            f"(delta: {memory_after - memory_before:.2f} MB)"
        )

    # Count output files
    csv_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "streaming", "advanced_csv"))
        if f.endswith(".csv")
    ]
    print(f"CSV files created: {len(csv_files)}")

    # Example 4: Advanced streaming to Parquet
    print("\n=== Advanced Streaming to Parquet ===")
    print("Description: Direct streaming to Parquet files with compression")

    # Clean up memory first
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Process with streaming API directly to Parquet with compression
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="parquet",
        output_destination=os.path.join(output_dir, "streaming", "advanced_parquet"),
        compression="snappy",  # Use compression
        row_group_size=1000,  # Optimize for memory usage
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {memory_after:.2f} MB "
            f"(delta: {memory_after - memory_before:.2f} MB)"
        )

    # Count output files
    parquet_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "streaming", "advanced_parquet"))
        if f.endswith(".parquet")
    ]
    print(f"Parquet files created: {len(parquet_files)}")

    # Example 5: File-based streaming
    print("\n=== File-based Streaming ===")
    print("Description: Stream process large files directly")

    # Create a large JSON file for testing
    large_file_path = os.path.join(output_dir, "large_test_data.json")

    # Only create if it doesn't exist (to save time on repeated runs)
    if not os.path.exists(large_file_path):
        print("Creating large test file...")
        import json

        with open(large_file_path, "w") as f:
            json.dump(data, f)
        print(f"Created test file: {large_file_path}")

    # Stream process the file
    start_time = time.time()
    memory_before = get_memory_usage()

    processor.stream_process_file(
        file_path=large_file_path,
        entity_name="file_records",
        output_format="json",
        output_destination=os.path.join(output_dir, "streaming", "file_based"),
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {memory_after:.2f} MB "
            f"(delta: {memory_after - memory_before:.2f} MB)"
        )

    print("\n=== Summary ===")
    print("Transmog v1.1.0 provides multiple approaches for different use cases:")
    print()
    print("1. Simple API (tm.flatten): Best for most use cases")
    print("   - Easy to use")
    print("   - Good memory efficiency with low_memory=True")
    print("   - Returns FlattenResult for easy manipulation")
    print()
    print("2. Advanced Streaming: Best for very large datasets")
    print("   - Direct file output (no intermediate memory)")
    print("   - Minimal memory footprint")
    print("   - Supports multiple output formats")
    print("   - Use when data doesn't fit in memory")
    print()
    print("Choose the approach that best fits your data size and use case!")


if __name__ == "__main__":
    main()
