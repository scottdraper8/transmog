#!/usr/bin/env python
"""Example of streaming processing with Parquet output format.

This example demonstrates how to use Transmog for streaming processing with Parquet
    output,
showing memory-efficient processing of very large datasets.
"""

import importlib.util
import json
import os
import sys
import time
from pathlib import Path

import psutil

# Add the parent directory to the path to find transmog when running from the
# examples directory
sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

# Import from transmog package
from transmog import ProcessingMode, Processor, TransmogConfig

# Check if PyArrow is available using importlib.util.find_spec
HAS_PYARROW = importlib.util.find_spec("pyarrow") is not None


# Create a large sample dataset
def generate_sample_data(count=1000):
    """Generate a large sample dataset for demonstration."""
    return [
        {
            "id": i,
            "name": f"Record {i}",
            "status": "active" if i % 3 == 0 else "inactive",
            "score": i * 1.5,
            "metadata": {
                "created_at": f"2023-01-{(i % 28) + 1:02d}",
                "tags": ["sample", "test", f"tag{i % 5}"],
                "properties": {
                    "value": i * 10,
                    "code": f"CODE_{i:04d}",
                    "flag": i % 2 == 0,
                },
            },
            "items": [
                {"item_id": i * 100 + j, "value": j * 2.5, "name": f"Item {j}"}
                for j in range(1, (i % 5) + 1)
            ],
        }
        for i in range(1, count + 1)
    ]


def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def main():
    """Run the streaming Parquet example."""
    print("Streaming Parquet Example")
    print("------------------------")

    if not HAS_PYARROW:
        print("Error: PyArrow is required for this example.")
        print("Please install it with: pip install pyarrow")
        return

    # Create output directory
    output_dir = Path("./output/streaming_parquet")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate a large dataset (can process millions of records with low memory)
    print("Generating sample data...")
    total_records = 10000
    batch_size = 500

    # Measure initial memory
    initial_memory = get_memory_usage()
    print(f"Initial memory usage: {initial_memory:.2f} MB")

    # Configure Transmog for memory-efficient processing
    config = TransmogConfig.memory_optimized().with_processing(
        # Start with memory optimization
        visit_arrays=True,  # Ensure arrays are processed
        processing_mode=ProcessingMode.MEMORY_OPTIMIZED,
        batch_size=batch_size,  # Set batch size
        cache_enabled=True,  # Enable caching for performance
        cache_maxsize=5000,  # Set reasonable cache size
    )

    processor = Processor(config=config)

    # Example 1: Direct streaming to Parquet (most memory efficient)
    print("\n=== Example 1: Direct streaming to Parquet ===")

    # Create a fresh data sample
    data = generate_sample_data(total_records)
    print(f"Generated {len(data)} records")

    # Measure memory before streaming
    memory_before = get_memory_usage()
    start_time = time.time()

    # Stream process directly to Parquet
    processor.stream_process(
        data=data,
        entity_name="records",
        output_format="parquet",
        output_destination=str(output_dir / "direct_stream"),
        # Parquet-specific options
        compression="snappy",
        row_group_size=2000,
    )

    # Calculate elapsed time and memory usage
    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB "
        f"(delta: {memory_after - memory_before:.2f} MB)"
    )

    # Count output files
    parquet_files = list(Path(output_dir / "direct_stream").glob("*.parquet"))
    print(f"Created {len(parquet_files)} Parquet files")

    # Example 2: Batch streaming with progress reporting
    print("\n=== Example 2: Batch Streaming with Progress ===")

    # Process records in batches for better control
    print(f"Processing {total_records} records in batches of {batch_size}...")
    start_time = time.time()
    memory_before = get_memory_usage()
    processed_count = 0

    # Process each batch
    for i in range(0, total_records, batch_size):
        # Generate a batch of data
        batch_end = min(i + batch_size, total_records)
        batch = data[i:batch_end]

        # Stream the batch directly to Parquet
        processor.stream_process(
            data=batch,
            entity_name="records",
            output_format="parquet",
            output_destination=str(output_dir / "batch_stream"),
            # Parquet-specific options
            compression="snappy",
            row_group_size=1000,
        )

        processed_count += len(batch)
        if processed_count % 1000 == 0:
            elapsed = time.time() - start_time
            memory_current = get_memory_usage()
            print(
                f"Processed {processed_count}/{total_records} records "
                f"({processed_count / total_records * 100:.1f}%) in {elapsed:.2f}s. "
                f"Memory: {memory_current:.2f} MB"
            )

    # Calculate elapsed time and memory
    elapsed = time.time() - start_time
    memory_after = get_memory_usage()

    print(f"Processed {processed_count} records in {elapsed:.2f} seconds")
    print(f"Average: {processed_count / elapsed:.1f} records/second")
    print(
        f"Memory usage: {memory_after:.2f} MB "
        f"(delta: {memory_after - memory_before:.2f} MB)"
    )

    # Example 3: Processing from a file directly to Parquet
    print("\n=== Example 3: File-to-Parquet Streaming ===")

    # First save sample data to a JSONL file
    jsonl_path = output_dir / "sample_data.jsonl"
    with open(jsonl_path, "w") as f:
        for record in data[:1000]:  # Save 1000 records to JSONL
            f.write(json.dumps(record) + "\n")

    print(f"Saved 1000 records to {jsonl_path}")

    # Free memory from data
    data = None
    import gc

    gc.collect()

    # Measure memory before processing
    memory_before = get_memory_usage()
    start_time = time.time()

    # Stream process from file to Parquet
    processor.stream_process_file(
        file_path=str(jsonl_path),
        entity_name="records",
        output_format="parquet",
        output_destination=str(output_dir / "file_to_parquet"),
        # Parquet-specific options
        compression="snappy",
        row_group_size=500,
    )

    # Calculate elapsed time and memory
    elapsed = time.time() - start_time
    memory_after = get_memory_usage()

    print(f"Processed file in {elapsed:.2f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB "
        f"(delta: {memory_after - memory_before:.2f} MB)"
    )

    # List output files
    print("\nOutput files:")
    all_parquet_files = list(output_dir.glob("**/*.parquet"))
    total_size = sum(f.stat().st_size for f in all_parquet_files)

    # Print by directory
    for subdir in ["direct_stream", "batch_stream", "file_to_parquet"]:
        dir_path = output_dir / subdir
        if dir_path.exists():
            files = list(dir_path.glob("*.parquet"))
            dir_size = sum(f.stat().st_size for f in files)
            print(f"- {subdir}: {len(files)} files, {dir_size / (1024 * 1024):.2f} MB")

    print(f"Total: {len(all_parquet_files)} files, {total_size / (1024 * 1024):.2f} MB")

    # Summary
    print("\nKey Advantages:")
    print("1. Memory Efficiency: Low memory usage regardless of dataset size")
    print(
        "2. Streaming Architecture: Process data without loading everything into memory"
    )
    print(
        "3. PyArrow Integration: Efficient Parquet write with row groups and "
        "compression"
    )
    print("4. Schema Evolution: Automatically handles varying schemas")
    print("5. Progress Control: Batch processing enables progress reporting")
    print(f"\nAll output written to: {output_dir}")


if __name__ == "__main__":
    main()
