"""Streaming example demonstrating Transmog's memory-efficient processing.

This example shows how to process data with minimal memory usage
using the streaming API with full feature parity with the non-streaming API.
"""

import json
import os
import sys
import time

import psutil

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog package
from transmog import ProcessingMode, Processor, TransmogConfig
from transmog.error import LENIENT


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


def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def main():
    """Run the example demonstrating streaming functionality."""
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "output", "streaming")
    os.makedirs(output_dir, exist_ok=True)

    # Generate test data
    data = generate_large_dataset(num_records=10000)
    print(f"Generated {len(data)} records")

    # Measure initial memory usage
    initial_memory = get_memory_usage()
    print(f"Initial memory usage: {initial_memory:.2f} MB")

    # Example 1: Standard (in-memory) processing
    print("\n=== Example 1: Standard Processing (in-memory) ===")
    start_time = time.time()
    memory_before = get_memory_usage()

    processor = Processor()
    result = processor.process(data=data, entity_name="records")

    # Write results to files
    result.write_all_csv(os.path.join(output_dir, "standard"))

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: "
        f"{memory_after - memory_before:.2f} MB)"
    )
    print(f"Tables created: {len(result.get_table_names()) + 1}")  # +1 for main table
    print(f"Total records: {sum(len(table) for table in result.to_dict().values())}")

    # Example 2: Streaming processing to CSV
    print("\n=== Example 2: Streaming Processing to CSV ===")
    # Clean up memory first
    result = None
    import gc

    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Create streaming processor
    processor = Processor()

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
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: "
        f"{memory_after - memory_before:.2f} MB)"
    )

    # Count output files
    streaming_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "streaming_csv"))
        if f.endswith(".csv")
    ]
    print(f"Files created: {len(streaming_files)}")

    # Example 3: Streaming to JSON format
    print("\n=== Example 3: Streaming to JSON Format ===")

    # Clean up memory first
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Process with streaming API directly to JSON
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="json",
        output_destination=os.path.join(output_dir, "streaming_json"),
        # Additional JSON format options
        indent=2,
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: "
        f"{memory_after - memory_before:.2f} MB)"
    )

    # Count output files
    json_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "streaming_json"))
        if f.endswith(".json")
    ]
    print(f"JSON files created: {len(json_files)}")

    # Example 4: Streaming to Parquet format
    print("\n=== Example 4: Streaming to Parquet Format ===")

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
            # Additional Parquet format options
            compression="snappy",
            row_group_size=1000,
        )

        end_time = time.time()
        memory_after = get_memory_usage()

        print(f"Time taken: {end_time - start_time:.2f} seconds")
        print(
            f"Memory usage: {memory_after:.2f} MB (delta: "
            f"{memory_after - memory_before:.2f} MB)"
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
    print("\n=== Example 5: Streaming with Custom Configuration ===")

    # Clean up memory first
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Create custom configuration
    custom_config = (
        TransmogConfig.default()
        .with_naming(
            separator=".", abbreviate_table_names=True, max_table_component_length=5
        )
        .with_processing(
            cast_to_string=False,
            skip_null=False,
            visit_arrays=True,
            processing_mode=ProcessingMode.MEMORY_OPTIMIZED,
        )
        .with_metadata(id_field="record_id", parent_field="parent_id")
        .with_error_handling(recovery_strategy=LENIENT, allow_malformed_data=True)
    )

    processor = Processor(config=custom_config)

    # Stream with custom configuration
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="csv",
        output_destination=os.path.join(output_dir, "streaming_custom"),
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: "
        f"{memory_after - memory_before:.2f} MB)"
    )

    # Count output files
    custom_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "streaming_custom"))
        if f.endswith(".csv")
    ]
    print(f"Files created with custom configuration: {len(custom_files)}")

    # Example 6: Processing from File
    print("\n=== Example 6: Streaming Process from File ===")

    # First save the data to a JSONL file
    jsonl_path = os.path.join(output_dir, "large_dataset.jsonl")
    with open(jsonl_path, "w") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")
    print(f"Wrote {len(data)} records to {jsonl_path}")

    # Clean up memory
    data = None
    gc.collect()

    # Process from the file
    start_time = time.time()
    memory_before = get_memory_usage()

    processor = Processor.memory_optimized()
    processor.stream_process_file(
        file_path=jsonl_path,
        entity_name="records",
        output_format="json",
        output_destination=os.path.join(output_dir, "from_file"),
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: "
        f"{memory_after - memory_before:.2f} MB)"
    )

    # Count output files
    file_output_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "from_file"))
        if f.endswith(".json")
    ]
    print(f"Files created from file processing: {len(file_output_files)}")

    print(f"\nAll streaming outputs written to: {output_dir}")


if __name__ == "__main__":
    main()
