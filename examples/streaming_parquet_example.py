#!/usr/bin/env python
"""
Streaming Parquet example for Transmog.

This example demonstrates how to use Transmog to process data in a streaming
fashion and write directly to Parquet format with optimal memory usage.
"""

import os
import sys
import json
import time
from pathlib import Path

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog package
from transmog import Processor, TransmogConfig
from transmog.io.writers.parquet import ParquetStreamingWriter
from transmog.io import create_streaming_writer


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


def main():
    """Run the streaming Parquet example."""
    print("Streaming Parquet Example")
    print("------------------------")

    # Create output directory
    output_dir = Path("./output/streaming_parquet")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate a large dataset (can process millions of records with low memory)
    print("Generating sample data...")
    total_records = 10000
    batch_size = 500

    # Configure Transmog
    config = TransmogConfig()
    # Enable array extraction and include parent IDs for relationship tracking
    config.processing.array_handling = "extract_arrays"
    config.processing.include_parent_id = True

    processor = Processor(config=config)

    # Process records in batches
    print(f"Processing {total_records} records in batches of {batch_size}...")
    start_time = time.time()
    processed_count = 0

    for i in range(0, total_records, batch_size):
        # Generate a batch of data
        batch = generate_sample_data(min(batch_size, total_records - i))

        # Process the batch with Transmog
        result = processor.process(batch, entity_name="records")

        # Stream the result to Parquet files
        result.stream_to_parquet(
            base_path=str(output_dir), compression="snappy", row_group_size=2000
        )

        processed_count += len(batch)
        if processed_count % 1000 == 0:
            elapsed = time.time() - start_time
            print(
                f"Processed {processed_count}/{total_records} records ({processed_count / total_records * 100:.1f}%) in {elapsed:.2f}s"
            )

    # Calculate elapsed time
    elapsed = time.time() - start_time
    print(f"Processed {processed_count} records in {elapsed:.2f} seconds")
    print(f"Average: {processed_count / elapsed:.1f} records/second")

    # List output files
    print("\nOutput files:")
    for file in output_dir.glob("*.parquet"):
        size_mb = file.stat().st_size / (1024 * 1024)
        print(f"- {file.name} ({size_mb:.2f} MB)")

    # Summary
    print("\nSchema evolution and memory usage:")
    print("- Records with varying schema are handled automatically")
    print("- Parquet row groups optimize reading and compression")
    print("- Low memory footprint regardless of dataset size")
    print("- All records are efficiently processed in batches")


if __name__ == "__main__":
    main()
