"""Flattener Modes Example.

This example demonstrates the unified flattener API,
showing how to use the mode parameter to switch between standard
and streaming (memory-optimized) modes.
"""

import os
import sys
import time

import psutil

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from transmog.core.flattener import flatten_json


def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def main():
    """Run the example."""
    # Create a nested test structure
    data = {
        "id": 12345,
        "name": "Example Company",
        "status": "active",
        "created_at": "2023-05-15T10:30:00Z",
        "details": {
            "industry": "Technology",
            "size": "Medium",
            "founded": 2010,
            "public": False,
            "headquarters": {
                "address": "123 Tech Lane",
                "city": "San Francisco",
                "state": "CA",
                "country": "USA",
                "coordinates": {"latitude": 37.7749, "longitude": -122.4194},
            },
            "subsidiaries": [
                {"name": "TechDivision", "location": "New York"},
                {"name": "DataLabs", "location": "Austin"},
            ],
        },
        "products": [
            {
                "id": "p-001",
                "name": "Product A",
                "version": "1.0",
                "pricing": {
                    "base": 99.99,
                    "discount": 0.1,
                    "tiers": [
                        {"quantity": 10, "discount": 0.15},
                        {"quantity": 50, "discount": 0.25},
                    ],
                },
            },
            {
                "id": "p-002",
                "name": "Product B",
                "version": "2.3",
                "pricing": {
                    "base": 149.99,
                    "discount": 0.05,
                    "tiers": [
                        {"quantity": 5, "discount": 0.1},
                        {"quantity": 20, "discount": 0.2},
                    ],
                },
            },
        ],
    }

    print("\n=== Example 1: Standard Mode ===")
    memory_before = get_memory_usage()
    start_time = time.time()

    # Use the default standard mode
    result1 = flatten_json(data, separator=".", cast_to_string=True, skip_arrays=True)

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.6f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: "
        f"{memory_after - memory_before:.2f} MB)"
    )
    print(f"Result keys: {len(result1)} fields")
    print("Sample fields:")
    for key in list(result1.keys())[:5]:
        print(f"  {key}: {result1[key]}")

    print("\n=== Example 2: Streaming Mode ===")
    # Force garbage collection to get a clean memory measurement
    import gc

    gc.collect()

    memory_before = get_memory_usage()
    start_time = time.time()

    # Use the streaming mode for better memory efficiency
    result2 = flatten_json(
        data,
        separator=".",
        cast_to_string=True,
        skip_arrays=True,
        mode="streaming",  # Use streaming mode
    )

    end_time = time.time()
    memory_after = get_memory_usage()

    print(f"Time taken: {end_time - start_time:.6f} seconds")
    print(
        f"Memory usage: {memory_after:.2f} MB (delta: "
        f"{memory_after - memory_before:.2f} MB)"
    )
    print(f"Result keys: {len(result2)} fields")
    print("Sample fields:")
    for key in list(result2.keys())[:5]:
        print(f"  {key}: {result2[key]}")

    print("\n=== Example 3: Deep Nesting Handling ===")
    # Using the mode parameter with additional options
    result3 = flatten_json(
        data,
        separator=".",
        cast_to_string=True,
        skip_arrays=True,
        deep_nesting_threshold=3,
        max_field_component_length=5,
        preserve_leaf_component=True,
        mode="streaming",
    )

    print(f"Result keys with deep nesting threshold (3): {len(result3)} fields")
    print("Sample fields (deep nesting handling):")
    for key in list(result3.keys())[:5]:
        print(f"  {key}: {result3[key]}")

    print("\n=== Example 4: Comparison of Results ===")
    # Verify that both modes produce identical results
    are_equal = result1 == result2
    print(f"Standard and streaming mode results are identical: {are_equal}")

    # Compare keys
    standard_keys = set(result1.keys())
    streaming_keys = set(result2.keys())

    print(f"Keys in standard but not streaming: {standard_keys - streaming_keys}")
    print(f"Keys in streaming but not standard: {streaming_keys - standard_keys}")

    # Compare a few values
    if are_equal:
        print("Both modes produce functionally equivalent results!")
    else:
        print("Differences found in results:")
        for key in standard_keys.intersection(streaming_keys):
            if result1[key] != result2[key]:
                print(f"  {key}: Standard: {result1[key]}, Streaming: {result2[key]}")


if __name__ == "__main__":
    main()
