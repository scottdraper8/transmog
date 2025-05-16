"""Example Name: Streaming Processing.

Demonstrates: Memory-efficient streaming data processing.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/advanced/streaming.html
- https://transmog.readthedocs.io/en/latest/tutorials/intermediate/streaming-large-datasets.html

Learning Objectives:
- How to process large datasets with minimal memory consumption
- How to use streaming APIs for direct file output
- How to configure streaming behavior
- How to monitor performance metrics for streaming processes
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
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "streaming"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Generate test data
    data = generate_large_dataset(num_records=5000)
    print(f"Generated {len(data)} records")

    # Measure initial memory usage
    initial_memory = get_memory_usage()
    print(f"Initial memory usage: {initial_memory:.2f} MB")

    # Example 1: Standard (in-memory) processing for comparison
    print("\n=== Standard (In-Memory) Processing ===")
    print("Description: Processes data in memory and then writes to outputs")

    start_time = time.time()
    memory_before = get_memory_usage()

    processor = tm.Processor()
    result = processor.process(data=data, entity_name="records")

    # Write results to files
    result.write_all_json(os.path.join(output_dir, "standard"))

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {memory_after:.2f} MB "
            f"(delta: {memory_after - memory_before:.2f} MB)"
        )
    print(f"Tables created: {len(result.get_table_names()) + 1}")  # +1 for main table
    print(f"Total records: {sum(len(table) for table in result.to_dict().values())}")

    # Example 2: Streaming processing to JSON
    print("\n=== Streaming Processing to JSON ===")
    print("Description: Processes data and writes directly to JSON files")

    # Clean up memory first
    result = None
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Create streaming processor
    processor = tm.Processor()

    # Process with streaming API directly to JSON
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="json",
        output_destination=os.path.join(output_dir, "streaming_json"),
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
        for f in os.listdir(os.path.join(output_dir, "streaming_json"))
        if f.endswith(".json")
    ]
    print(f"JSON files created: {len(json_files)}")

    # Example 3: Streaming processing to CSV
    print("\n=== Streaming Processing to CSV ===")
    print("Description: Processes data and writes directly to CSV files")

    # Clean up memory first
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Process with streaming API directly to CSV
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="csv",
        output_destination=os.path.join(output_dir, "streaming_csv"),
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
        for f in os.listdir(os.path.join(output_dir, "streaming_csv"))
        if f.endswith(".csv")
    ]
    print(f"CSV files created: {len(csv_files)}")

    # Example 4: Streaming to Parquet format
    print("\n=== Streaming Processing to Parquet ===")
    print("Description: Processes data and writes directly to Parquet files")

    # Clean up memory first
    gc.collect()

    try:
        # Try to stream to Parquet (requires PyArrow)
        start_time = time.time()
        memory_before = get_memory_usage()

        processor.stream_process(
            data=data,
            entity_name="records",
            output_format="parquet",
            output_destination=os.path.join(output_dir, "streaming_parquet"),
            compression="snappy",  # Parquet-specific option
            row_group_size=1000,  # Parquet-specific option
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
            for f in os.listdir(os.path.join(output_dir, "streaming_parquet"))
            if f.endswith(".parquet")
        ]
        print(f"Parquet files created: {len(parquet_files)}")

    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")

    # Example 5: Streaming with Custom Configuration
    print("\n=== Streaming with Custom Configuration ===")
    print("Description: Customized streaming process with specific configuration")

    # Clean up memory first
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Create custom configuration
    custom_config = (
        tm.TransmogConfig.default()
        .with_naming(
            separator=".",  # Use dot notation for nested fields
            abbreviate_table_names=True,  # Abbreviate table names
            max_table_component_length=5,  # Limit component length in table names
        )
        .with_processing(
            cast_to_string=False,  # Keep original data types
            skip_null=False,  # Include null values
            processing_mode=tm.ProcessingMode.MEMORY_OPTIMIZED,  # Memory-optimized mode
        )
        .with_metadata(
            id_field="record_id",  # Custom ID field name
            parent_field="parent_id",  # Custom parent field name
        )
    )

    processor = tm.Processor(config=custom_config)

    # Custom streaming process
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="json",
        output_destination=os.path.join(output_dir, "custom_streaming"),
        indent=2,  # JSON-specific option
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    if HAVE_PSUTIL:
        print(
            f"Memory usage: {memory_after:.2f} MB "
            f"(delta: {memory_after - memory_before:.2f} MB)"
        )

    # Count output files with abbreviated names
    custom_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "custom_streaming"))
        if f.endswith(".json")
    ]
    print(f"Custom streaming files created: {len(custom_files)}")
    print("Example filenames:")
    for filename in custom_files[:2]:  # Show just a couple examples
        print(f"- {filename}")

    print("\n=== Performance Comparison ===")
    print("Standard processing: Higher memory usage, files written after processing")
    print(
        "Streaming processing: Lower memory usage, continuous writing during processing"
    )
    print(f"\nOutput files written to: {output_dir}")


if __name__ == "__main__":
    main()
