"""Example demonstrating parallel processing with Transmog.

This example shows how to use concurrent.futures with Transmog
to process data in parallel for better performance.
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from transmog import ProcessingResult, Processor


def generate_sample_data(count: int = 100) -> list[dict]:
    """Generate sample data for testing."""
    data = []
    for i in range(count):
        data.append(
            {
                "id": i,
                "name": f"Customer {i}",
                "address": {
                    "street": f"{i} Main St",
                    "city": "Anytown",
                    "state": "CA",
                    "zip": f"{10000 + i}",
                },
                "orders": [
                    {
                        "id": f"order-{i}-1",
                        "amount": 100 + i,
                        "date": "2023-01-01",
                        "items": [
                            {"product": "Widget A", "quantity": 2, "price": 50},
                            {"product": "Widget B", "quantity": 1, "price": i},
                        ],
                    },
                    {
                        "id": f"order-{i}-2",
                        "amount": 200 + i,
                        "date": "2023-02-01",
                        "items": [
                            {"product": "Widget C", "quantity": 3, "price": 75},
                            {"product": "Widget D", "quantity": 2, "price": i * 2},
                        ],
                    },
                ],
                "contact_info": {
                    "email": f"customer{i}@example.com",
                    "phone": f"555-{1000 + i}",
                    "preferences": {"marketing": i % 2 == 0, "notifications": True},
                },
            }
        )
    return data


def process_chunk(
    processor: Processor, chunk: list[dict], entity_name: str
) -> ProcessingResult:
    """Process a chunk of data."""
    return processor.process_batch(batch_data=chunk, entity_name=entity_name)


def main():
    """Run the example."""
    # Generate sample data
    print("Generating sample data...")
    data = generate_sample_data(1000)

    # Create chunks
    chunk_size = 100
    chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
    print(f"Created {len(chunks)} chunks of {chunk_size} records each")

    # Initialize processor
    processor = Processor(cast_to_string=True, include_empty=False)

    # Process in parallel
    print("\nProcessing in parallel...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit chunks to process
        futures = {
            executor.submit(
                process_chunk, processor=processor, chunk=chunk, entity_name="customers"
            ): i
            for i, chunk in enumerate(chunks)
        }

        # Track progress
        completed = 0
        for future in as_completed(futures):
            chunk_index = futures[future]
            completed += 1
            try:
                # Get result without doing anything with it yet
                future.result()
                print(
                    f"Completed chunk {chunk_index + 1}/{len(chunks)} "
                    f"({completed / len(chunks) * 100:.1f}%)"
                )
            except Exception as e:
                print(f"Chunk {chunk_index} failed: {str(e)}")

    parallel_time = time.time() - start_time
    print(f"Parallel processing time: {parallel_time:.2f} seconds")

    # Process sequentially for comparison
    print("\nProcessing sequentially for comparison...")
    start_time = time.time()

    for i, chunk in enumerate(chunks):
        processor.process_batch(batch_data=chunk, entity_name="customers")
        print(
            f"Completed chunk {i + 1}/{len(chunks)} "
            f"({(i + 1) / len(chunks) * 100:.1f}%)"
        )

    sequential_time = time.time() - start_time
    print(f"Sequential processing time: {sequential_time:.2f} seconds")

    # Summary
    print(f"\nSpeedup: {sequential_time / parallel_time:.2f}x")


if __name__ == "__main__":
    main()
