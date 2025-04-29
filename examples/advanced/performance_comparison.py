"""
Performance comparison example demonstrating Transmog optimizations.

This example compares different configuration options and their impact
on performance for processing nested JSON data.
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from transmog import Processor, ProcessingResult


def generate_complex_data(count: int = 100) -> List[Dict]:
    """Generate complex nested data for performance testing."""
    data = []
    for i in range(count):
        # Create a record with multiple levels of nesting
        record = {
            "id": i,
            "name": f"Record {i}",
            "metadata": {
                "created_at": "2023-01-01T00:00:00Z",
                "updated_at": "2023-01-02T00:00:00Z",
                "status": "active" if i % 2 == 0 else "inactive",
                "tags": [f"tag{j}" for j in range(5)],
                "flags": {
                    "important": i % 3 == 0,
                    "verified": i % 5 == 0,
                    "featured": i % 7 == 0,
                },
            },
            "details": {
                "description": f"Description for record {i}",
                "attributes": {
                    "color": ["red", "green", "blue"][i % 3],
                    "size": ["small", "medium", "large"][i % 3],
                    "weight": i * 1.5,
                },
                "metrics": [
                    {"name": "views", "value": i * 10, "unit": "count"},
                    {"name": "score", "value": i * 0.5, "unit": "points"},
                ],
            },
            "related_items": [
                {
                    "id": f"related-{i}-{j}",
                    "name": f"Related {j}",
                    "type": ["reference", "similar", "alternative"][j % 3],
                    "strength": j * 0.1,
                    "tags": [f"rel_tag_{j}_{k}" for k in range(3)],
                    "sub_items": [
                        {
                            "id": f"sub-{i}-{j}-{k}",
                            "value": k * 0.01,
                            "properties": {
                                "enabled": k % 2 == 0,
                                "visible": True,
                                "rank": k,
                            },
                        }
                        for k in range(2)
                    ],
                }
                for j in range(3)
            ],
        }
        data.append(record)
    return data


def run_test(name: str, func, *args, **kwargs) -> float:
    """Run a test function and measure its execution time."""
    print(f"Running test: {name}...")
    start_time = time.time()
    result = func(*args, **kwargs)
    elapsed = time.time() - start_time
    print(f"  Time: {elapsed:.4f} seconds")
    # Print some basic stats about the result
    if isinstance(result, ProcessingResult):
        main_count = len(result.get_main_table())
        child_tables = result.get_table_names()
        print(f"  Main records: {main_count}")
        print(f"  Child tables: {len(child_tables)}")
        if child_tables:
            print(f"  Child table names: {', '.join(child_tables)}")
        total_child_records = sum(len(result.get_child_table(t)) for t in child_tables)
        print(f"  Total child records: {total_child_records}")
    return elapsed


def test_standard_processing(data: List[Dict]) -> ProcessingResult:
    """Test standard processing approach.

    This uses direct extraction without any optimizations.
    """
    from transmog.core.extractor import extract_arrays
    from transmog.core.flattener import flatten_json
    from transmog.core.metadata import (
        annotate_with_metadata,
        get_current_timestamp,
    )

    extract_time = get_current_timestamp()
    main_records = []
    all_arrays = {}

    print("  Processing records individually without optimizations...")

    # Process each record individually without optimizations
    for record in data:
        # Flatten the main record
        flat_record = flatten_json(
            record,
            separator="_",
            cast_to_string=True,
            include_empty=False,
            skip_arrays=True,
            skip_null=True,
        )

        # Add metadata
        annotated_record = annotate_with_metadata(
            flat_record,
            extract_time=extract_time,
        )

        # Extract arrays directly
        arrays = extract_arrays(
            record,
            parent_id=annotated_record.get("__extract_id"),
            entity_name="records",
            extract_time=extract_time,
            separator="_",
            cast_to_string=True,
            include_empty=False,
            skip_null=True,
        )

        main_records.append(annotated_record)

        # Combine arrays manually
        for array_name, array_items in arrays.items():
            if array_name not in all_arrays:
                all_arrays[array_name] = []
            all_arrays[array_name].extend(array_items)

    print(f"  Finished standard processing, found {len(all_arrays)} array types")
    if all_arrays:
        print(f"  Array types: {', '.join(all_arrays.keys())}")

    return ProcessingResult(
        main_table=main_records, child_tables=all_arrays, entity_name="records"
    )


def test_single_pass_processing(data: List[Dict]) -> ProcessingResult:
    """Test optimized single-pass processing."""
    # Use the high-level processor with use_single_pass=True for optimized processing
    processor = Processor(cast_to_string=True)
    return processor.process(
        data=data,
        entity_name="records",
        use_single_pass=True,  # Enable optimized single-pass processing
    )


def test_batch_processing(data: List[Dict], batch_size: int = 50) -> ProcessingResult:
    """Test batch processing for memory efficiency."""
    processor = Processor(
        cast_to_string=True,
        batch_size=batch_size,
        optimize_for_memory=True,
    )
    return processor.process_chunked(
        data=data,
        entity_name="records",
        chunk_size=batch_size,
    )


def test_parallel_processing(data: List[Dict], workers: int = 4) -> ProcessingResult:
    """Test parallel processing with multiple threads."""
    processor = Processor(cast_to_string=True)

    # Split data into chunks for parallel processing
    chunk_size = max(1, len(data) // workers)
    chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

    # Process chunks in parallel
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                processor.process_batch,
                batch_data=chunk,
                entity_name="records",
            )
            for chunk in chunks
        ]

        # Collect results
        results = [future.result() for future in futures]

    # Combine results
    return ProcessingResult.combine_results(results, entity_name="records")


def main():
    """Run the performance comparison tests."""
    # Generate test data
    print("Generating test data...")
    data_small = generate_complex_data(100)
    data_medium = generate_complex_data(500)
    data_large = generate_complex_data(1000)

    print("\n=== Small Dataset (100 records) ===")
    time_standard = run_test(
        "Standard processing", test_standard_processing, data_small
    )
    time_single_pass = run_test(
        "Single-pass processing", test_single_pass_processing, data_small
    )
    time_batch = run_test("Batch processing", test_batch_processing, data_small, 25)
    time_parallel = run_test(
        "Parallel processing", test_parallel_processing, data_small
    )

    print(f"\nSingle-pass speedup: {time_standard / time_single_pass:.2f}x")
    print(f"Batch processing speedup: {time_standard / time_batch:.2f}x")
    print(f"Parallel processing speedup: {time_standard / time_parallel:.2f}x")

    print("\n=== Medium Dataset (500 records) ===")
    time_standard = run_test(
        "Standard processing", test_standard_processing, data_medium
    )
    time_single_pass = run_test(
        "Single-pass processing", test_single_pass_processing, data_medium
    )
    time_batch = run_test("Batch processing", test_batch_processing, data_medium, 100)
    time_parallel = run_test(
        "Parallel processing", test_parallel_processing, data_medium
    )

    print(f"\nSingle-pass speedup: {time_standard / time_single_pass:.2f}x")
    print(f"Batch processing speedup: {time_standard / time_batch:.2f}x")
    print(f"Parallel processing speedup: {time_standard / time_parallel:.2f}x")

    print("\n=== Large Dataset (1000 records) ===")
    time_standard = run_test(
        "Standard processing", test_standard_processing, data_large
    )
    time_single_pass = run_test(
        "Single-pass processing", test_single_pass_processing, data_large
    )
    time_batch = run_test("Batch processing", test_batch_processing, data_large, 200)
    time_parallel = run_test(
        "Parallel processing", test_parallel_processing, data_large
    )

    print(f"\nSingle-pass speedup: {time_standard / time_single_pass:.2f}x")
    print(f"Batch processing speedup: {time_standard / time_batch:.2f}x")
    print(f"Parallel processing speedup: {time_standard / time_parallel:.2f}x")


if __name__ == "__main__":
    main()
