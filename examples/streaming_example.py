"""
Streaming example demonstrating Transmog's memory-efficient processing.

This example shows how to process data with minimal memory usage
using the streaming API with full feature parity with the non-streaming API.
"""

import json
import os
import sys
import time
import psutil
from pprint import pprint

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from transmog import Processor, TransmogConfig


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
        f"Memory usage: {memory_after:.2f} MB (delta: {memory_after - memory_before:.2f} MB)"
    )
    print(f"Tables created: {len(result.get_table_names()) + 1}")  # +1 for main table

    # Example 2: Streaming processing
    print("\n=== Example 2: Streaming Processing ===")
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
        output_destination=os.path.join(output_dir, "streaming"),
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: {memory_after - memory_before:.2f} MB)"
    )

    # Count output files
    streaming_files = [
        f
        for f in os.listdir(os.path.join(output_dir, "streaming"))
        if f.endswith(".csv")
    ]
    print(f"Files created: {len(streaming_files)}")

    # Example 3: Streaming with Deterministic IDs
    print("\n=== Example 3: Streaming with Deterministic IDs ===")

    # Clean up memory first
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Create config with deterministic IDs
    config = TransmogConfig.with_deterministic_ids(
        {
            "": "id",  # Root level uses "id" field
            "contacts": "value",  # Contact records use "value" field
            "items": "id",  # Item records use "id" field
        }
    )

    processor = Processor(config=config)

    # Process with streaming API directly to CSV using deterministic IDs
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="csv",
        output_destination=os.path.join(output_dir, "deterministic_ids"),
        use_deterministic_ids=True,
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: {memory_after - memory_before:.2f} MB)"
    )

    # Example 4: Advanced Streaming with Custom Configuration
    print("\n=== Example 4: Advanced Streaming with Custom Configuration ===")

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
        .with_processing(cast_to_string=False, skip_null=False, visit_arrays=True)
        .with_metadata(id_field="record_id", parent_field="parent_id")
    )

    processor = Processor(config=custom_config)

    # Process with streaming API with custom configuration
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="json",
        output_destination=os.path.join(output_dir, "custom_config"),
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: {memory_after - memory_before:.2f} MB)"
    )

    # Example 5: Process a file with streaming
    print("\n=== Example 5: Stream Processing from File to File ===")

    # First save data to a JSON file
    json_file = os.path.join(output_dir, "large_data.json")
    with open(json_file, "w") as f:
        json.dump(data, f)

    print(f"Saved data to {json_file}")

    # Clean up memory
    data = None
    gc.collect()

    start_time = time.time()
    memory_before = get_memory_usage()

    # Process file directly
    processor.stream_process_file(
        file_path=json_file,
        entity_name="records",
        output_format="csv",
        output_destination=os.path.join(output_dir, "from_file"),
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: {memory_after - memory_before:.2f} MB)"
    )

    # Summary
    print("\n=== Summary ===")
    print(
        "The streaming API now provides full feature parity with the non-streaming API"
    )
    print("while maintaining a minimal memory footprint.")
    print("\nAdvantages:")
    print("- Lower memory footprint")
    print("- Ability to process datasets larger than available memory")
    print("- Direct file-to-file processing without intermediate storage")
    print("- Full support for all configuration options, including deterministic IDs")
    print("- Complete metadata handling identical to non-streaming mode")
    print("\nWhen to use streaming:")
    print("- For very large datasets")
    print("- When memory is constrained")
    print("- When you need direct file-to-file processing")
    print("- When you don't need to manipulate the result in memory")


if __name__ == "__main__":
    main()
