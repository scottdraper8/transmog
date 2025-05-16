#!/usr/bin/env python
"""Benchmark script for transmog library.

This script runs benchmarks for the transmog library to measure performance
of different processing modes and configurations.
"""

import json
import os
import secrets
import sys
import time
import traceback
from pathlib import Path

# Try to import from installed package first, fall back to development setup
try:
    from transmog import Processor
except ImportError:
    # Add parent directory to path for development mode
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, os.path.join(parent_dir, "src"))
    from transmog import Processor

# Set up base directory relative to script location
BASE_DIR = Path(__file__).parent.parent


def generate_random_nested_data(count, levels=3, breadth=3):
    """Generate random nested data structure for benchmarking.

    Uses cryptographically secure random number generation via the secrets module.
    """
    result = []

    for i in range(count):
        item = {
            "id": i,
            "name": f"Item {i}",
            "value": secrets.randbelow(1000) + float(secrets.randbelow(1000)) / 1000,
            "tags": [f"tag{j}" for j in range(secrets.randbelow(5) + 1)],
        }

        # Add nested levels with random data
        current = item
        for level in range(levels):
            nested = {}
            for j in range(breadth):
                nested[f"field_{level}_{j}"] = (
                    secrets.randbelow(100) + float(secrets.randbelow(100)) / 1000
                )

            # Add array in nested level
            if (
                secrets.randbelow(2) == 1
            ):  # 50% chance, equivalent to random.random() > 0.5
                array_items = []
                array_count = secrets.randbelow(5) + 1
                for _k in range(array_count):
                    array_item = {}
                    for m in range(breadth):
                        array_item[f"array_field_{m}"] = (
                            secrets.randbelow(10) + float(secrets.randbelow(10)) / 1000
                        )
                    array_items.append(array_item)
                nested["items"] = array_items

            # Link nested level to parent
            level_name = f"level_{level}"
            current[level_name] = nested
            current = nested

        result.append(item)

    return result


def benchmark_processing(data, count, label, mode=None, **kwargs):
    """Run a benchmark for a specific processing mode."""
    config = None
    if mode:
        # This will use the processor's default mode
        pass

    processor = Processor(config=config)

    start_time = time.time()
    try:
        result = processor.process(data=data, entity_name="benchmark", **kwargs)
        end_time = time.time()

        # Calculate memory used by result
        memory_size = sys.getsizeof(result)
        # Add size of values
        for _key, value in result.items():
            if isinstance(value, (bytes, bytearray)):
                memory_size += len(value)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    memory_size += sys.getsizeof(item)

        processing_info = {
            "success": True,
            "time": end_time - start_time,
            "records": count,
            "tables": len(result.get_table_names()) + 1,  # +1 for main table
            "memory_kb": memory_size / 1024,
            "records_per_second": count / (end_time - start_time),
        }

    except Exception as e:
        end_time = time.time()
        processing_info = {
            "success": False,
            "time": end_time - start_time,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }

    print(f"Completed {label}")
    return processing_info


def run_benchmarks(record_counts, modes):
    """Run all benchmarks for different record counts and modes."""
    results = {}

    for count in record_counts:
        print(f"\nGenerating benchmark data for {count} records...")
        data = generate_random_nested_data(count)

        results[str(count)] = {}
        for mode in modes:
            print(f"Running benchmark with {count} records in {mode} mode...")
            results[str(count)][mode] = benchmark_processing(
                data=data,
                count=count,
                label=f"{count} records in {mode} mode",
                mode=mode,
            )

    return results


def main():
    """Run benchmarks and save results to file."""
    # Configuration
    record_counts = [10, 100, 1000, 10000]
    modes = ["default", "memory_optimized", "performance_optimized"]

    # Run benchmarks for all combinations
    results = run_benchmarks(record_counts, modes)

    # Save results to file
    result_file = BASE_DIR / "benchmark_results.json"
    with open(result_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nBenchmark results saved to {result_file}")


if __name__ == "__main__":
    main()
