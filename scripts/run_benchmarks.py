#!/usr/bin/env python3
"""Benchmark script for transmog library.

This script runs benchmarks for the transmog library to measure performance
of different processing modes and configurations, including memory optimizations.
"""

import gc
import json
import os
import secrets
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict

# Try to import from installed package first, fall back to development setup
try:
    import transmog as tm
    from transmog import Processor
except ImportError:
    # Add parent directory to path for development mode
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, os.path.join(parent_dir, "src"))
    import transmog as tm
    from transmog import Processor

# Base directory relative to script location
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

            # Add array in nested level (50% chance)
            if secrets.randbelow(2) == 1:
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


def create_test_data(num_records: int = 1000, depth: int = 4) -> list[Dict[str, Any]]:
    """Create test data with configurable complexity for memory optimization tests."""
    records = []
    for i in range(num_records):
        record = {
            "id": i,
            "name": f"Record_{i}",
            "status": "active" if i % 2 == 0 else "inactive",
            "metadata": {
                "created_at": f"2023-01-{(i % 28) + 1:02d}T00:00:00Z",
                "tags": [f"tag_{j}" for j in range(i % 5 + 1)],
                "nested": {
                    "level_2": {
                        "level_3": {
                            "level_4": f"deep_value_{i}",
                            "array_data": [f"item_{i}_{j}" for j in range(3)],
                        }
                    }
                },
            },
            "large_text": "x" * 100,  # Add some bulk to test memory usage
        }
        records.append(record)
    return records


def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    try:
        import os

        import psutil

        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    except ImportError:
        # Fallback estimation
        return gc.get_count()[0] * 0.001


def benchmark_processing(data, count, label, mode=None, **kwargs):
    """Run a benchmark for a specific processing mode."""
    config = None
    if mode:
        # Uses processor's default mode when mode is specified
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


def benchmark_configuration(config_name: str, data: list, **config_options) -> dict:
    """Benchmark a specific configuration with memory optimizations."""
    print(f"\n=== Benchmarking {config_name} ===")

    # Clear caches and collect garbage before test
    try:
        tm.core.flattener.clear_caches()
    except AttributeError:
        pass
    gc.collect()

    start_time = time.time()
    start_memory = get_memory_usage()

    try:
        result = tm.flatten(data, name="benchmark", **config_options)

        end_time = time.time()
        end_memory = get_memory_usage()

        processing_time = end_time - start_time
        memory_used = end_memory - start_memory

        metrics = {
            "config": config_name,
            "processing_time": processing_time,
            "memory_used_mb": memory_used,
            "records_processed": len(result.main),
            "throughput": len(result.main) / processing_time
            if processing_time > 0
            else 0,
            "tables_created": len(result.all_tables),
        }

        print(f"  Processing time: {processing_time:.3f}s")
        print(f"  Memory used: {memory_used:.1f} MB")
        print(f"  Throughput: {metrics['throughput']:.0f} records/sec")
        print(f"  Tables created: {metrics['tables_created']}")

        return metrics

    except Exception as e:
        print(f"  ERROR: {e}")
        return {
            "config": config_name,
            "error": str(e),
            "processing_time": float("inf"),
            "memory_used_mb": float("inf"),
            "throughput": 0,
        }


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


def run_memory_benchmarks():
    """Run memory optimization benchmarks."""
    print("\n" + "=" * 50)
    print("Memory Optimization Benchmark")
    print("=" * 50)

    # Test different dataset sizes
    test_sizes = [100, 500, 1000]

    for size in test_sizes:
        print(f"\n🔬 Testing with {size} records")
        print("-" * 30)

        data = create_test_data(size)
        results = []

        # Test different configurations
        configs = [
            ("Standard", {}),
            ("Low Memory", {"low_memory": True}),
            ("Small Batches", {"batch_size": 50, "low_memory": True}),
            ("Large Batches", {"batch_size": 2000, "low_memory": False}),
            ("Performance Optimized", {"batch_size": 1000, "low_memory": False}),
        ]

        for config_name, config_options in configs:
            metrics = benchmark_configuration(config_name, data, **config_options)
            results.append(metrics)

        # Find best configuration for this dataset size
        valid_results = [r for r in results if "error" not in r]
        if valid_results:
            best_throughput = max(valid_results, key=lambda x: x["throughput"])
            best_memory = min(valid_results, key=lambda x: x["memory_used_mb"])

            print(f"\n📊 Summary for {size} records:")
            print(
                f"  Best throughput: {best_throughput['config']} ({best_throughput['throughput']:.0f} rec/sec)"
            )
            print(
                f"  Best memory: {best_memory['config']} ({best_memory['memory_used_mb']:.1f} MB)"
            )

    # Test memory optimization features
    print("\n🧠 Memory Optimization Features")
    print("-" * 40)

    try:
        # Test adaptive cache sizing
        from transmog.core.flattener import get_adaptive_cache_size

        cache_size = get_adaptive_cache_size()
        print(f"  Adaptive cache size: {cache_size}")
    except (ImportError, AttributeError):
        print("  Adaptive cache sizing: Not available")

    try:
        # Test memory monitoring
        from transmog.core.memory import get_global_memory_monitor

        monitor = get_global_memory_monitor()
        memory_pressure = monitor.get_memory_pressure()
        print(f"  Current memory pressure: {memory_pressure:.1%}")
    except (ImportError, AttributeError):
        print("  Memory monitoring: Not available")

    try:
        # Test adaptive batch sizing
        from transmog.core.memory import get_global_batch_sizer

        batch_sizer = get_global_batch_sizer()
        adaptive_batch_size = batch_sizer.get_batch_size()
        print(f"  Adaptive batch size: {adaptive_batch_size}")
    except (ImportError, AttributeError):
        print("  Adaptive batch sizing: Not available")

    print("\n✅ Memory optimization features tested!")
    print("Available optimizations:")
    print("  • Efficient path building reduces string operations")
    print("  • In-place modifications reduce memory allocations")
    print("  • Memory-aware caching adapts to available memory")
    print("  • Adaptive batch sizing responds to memory pressure")
    print("  • Strategic garbage collection reduces memory pressure")


def main():
    """Run benchmarks and save results to file."""
    import argparse

    parser = argparse.ArgumentParser(description="Run transmog benchmarks")
    parser.add_argument(
        "--memory", action="store_true", help="Run memory optimization benchmarks"
    )
    parser.add_argument(
        "--standard", action="store_true", help="Run standard benchmarks"
    )

    args = parser.parse_args()

    # Default to standard if no specific option is chosen
    if not (args.memory or args.standard):
        args.standard = True

    if args.standard:
        print("Running standard benchmarks...")
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

    if args.memory:
        print("\nRunning memory optimization benchmarks...")
        run_memory_benchmarks()


if __name__ == "__main__":
    main()
