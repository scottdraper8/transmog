#!/usr/bin/env python3
"""Benchmark script for transmog library.

This script runs comprehensive benchmarks for the transmog library to measure
performance across different configurations, data sizes, and processing modes.
"""

import gc
import json
import os
import secrets
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Union

# Try to import from installed package first, fall back to development setup
try:
    import transmog as tm
except ImportError:
    # Add parent directory to path for development mode
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, os.path.join(parent_dir, "src"))
    import transmog as tm

# Base directory relative to script location
BASE_DIR = Path(__file__).parent.parent


def generate_random_nested_data(
    count: int, levels: int = 3, breadth: int = 3
) -> list[dict[str, Any]]:
    """Generate random nested data structure for benchmarking.

    Uses cryptographically secure random number generation via the secrets module.

    Args:
        count: Number of top-level records to generate
        levels: Number of nested levels to create
        breadth: Number of fields per nested level

    Returns:
        List of nested dictionaries for benchmarking
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


def create_test_data(num_records: int = 1000, depth: int = 4) -> list[dict[str, Any]]:
    """Create test data with configurable complexity for memory optimization tests.

    Args:
        num_records: Number of records to generate
        depth: Nesting depth for the data structure

    Returns:
        List of test records
    """
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
        # Fallback estimation using garbage collection stats
        return gc.get_count()[0] * 0.001


def benchmark_configuration(
    config_name: str, data: list[dict[str, Any]], **config_options: Any
) -> dict[str, Union[str, float, int]]:
    """Benchmark a specific configuration.

    Args:
        config_name: Name of the configuration being tested
        data: Test data to process
        **config_options: Configuration options for tm.flatten()

    Returns:
        Dictionary containing benchmark metrics
    """
    print(f"\n=== Benchmarking {config_name} ===")

    # Clear caches and collect garbage before test
    try:
        from transmog.core.flattener import clear_caches

        clear_caches()
    except (ImportError, AttributeError):
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
            "success": True,
        }

        print(f"  Processing time: {processing_time:.3f}s")
        print(f"  Memory used: {memory_used:.1f} MB")
        print(f"  Throughput: {metrics['throughput']:.0f} records/sec")
        print(f"  Tables created: {metrics['tables_created']}")

        return metrics

    except Exception as e:
        end_time = time.time()
        processing_time = end_time - start_time

        print(f"  ERROR: {e}")
        return {
            "config": config_name,
            "error": str(e),
            "processing_time": processing_time,
            "memory_used_mb": float("inf"),
            "throughput": 0,
            "success": False,
            "traceback": traceback.format_exc(),
        }


def run_standard_benchmarks(record_counts: list[int]) -> dict[str, dict[str, Any]]:
    """Run standard benchmarks across different record counts.

    Args:
        record_counts: List of record counts to test

    Returns:
        Nested dictionary of results organized by record count and configuration
    """
    results = {}

    # Define configurations to test
    configurations = [
        ("Default", {}),
        ("Low Memory", {"low_memory": True}),
        ("Large Batches", {"batch_size": 5000}),
        ("Small Batches", {"batch_size": 100}),
        ("Preserve Types", {"preserve_types": True}),
        ("Skip Empty", {"skip_empty": True, "skip_null": True}),
        ("Inline Arrays", {"arrays": "inline"}),
        ("Skip Arrays", {"arrays": "skip"}),
        ("Custom Separator", {"separator": "."}),
        ("Deep Nesting", {"nested_threshold": 8}),
        ("Shallow Nesting", {"nested_threshold": 2}),
    ]

    for count in record_counts:
        print(f"\n{'=' * 60}")
        print(f"Generating benchmark data for {count} records...")
        print(f"{'=' * 60}")

        data = generate_random_nested_data(count)
        results[str(count)] = {}

        for config_name, config_options in configurations:
            try:
                result = benchmark_configuration(config_name, data, **config_options)
                results[str(count)][config_name] = result
            except Exception as e:
                print(f"  Failed to benchmark {config_name}: {e}")
                results[str(count)][config_name] = {
                    "config": config_name,
                    "error": str(e),
                    "success": False,
                }

    return results


def run_memory_benchmarks() -> None:
    """Run memory optimization benchmarks."""
    print("\n" + "=" * 60)
    print("Memory Optimization Benchmark")
    print("=" * 60)

    # Test different dataset sizes
    test_sizes = [100, 500, 1000, 2000]

    for size in test_sizes:
        print(f"\n🔬 Testing with {size} records")
        print("-" * 40)

        data = create_test_data(size)
        results = []

        # Test different memory configurations
        configs = [
            ("Standard", {}),
            ("Low Memory", {"low_memory": True}),
            ("Small Batches", {"batch_size": 50, "low_memory": True}),
            ("Large Batches", {"batch_size": 2000, "low_memory": False}),
            ("Performance Optimized", {"batch_size": 1000, "low_memory": False}),
            ("Memory + Types", {"low_memory": True, "preserve_types": False}),
            (
                "Memory + Skip Empty",
                {"low_memory": True, "skip_empty": True, "skip_null": True},
            ),
        ]

        for config_name, config_options in configs:
            metrics = benchmark_configuration(config_name, data, **config_options)
            results.append(metrics)

        # Find best configuration for this dataset size
        valid_results = [r for r in results if r.get("success", False)]
        if valid_results:
            best_throughput = max(valid_results, key=lambda x: x["throughput"])
            best_memory = min(valid_results, key=lambda x: x["memory_used_mb"])

            print(f"\n📊 Summary for {size} records:")
            print(
                f"  Best throughput: {best_throughput['config']} "
                f"({best_throughput['throughput']:.0f} rec/sec)"
            )
            print(
                f"  Best memory: {best_memory['config']} "
                f"({best_memory['memory_used_mb']:.1f} MB)"
            )

    # Test memory optimization features
    print("\n🧠 Memory Optimization Features")
    print("-" * 40)

    try:
        from transmog.core.flattener import get_adaptive_cache_size

        cache_size = get_adaptive_cache_size()
        print(f"  Adaptive cache size: {cache_size}")
    except (ImportError, AttributeError):
        print("  Adaptive cache sizing: Not available")

    try:
        from transmog.core.memory import get_global_memory_monitor

        monitor = get_global_memory_monitor()
        memory_pressure = monitor.get_memory_pressure()
        print(f"  Current memory pressure: {memory_pressure:.1%}")
    except (ImportError, AttributeError):
        print("  Memory monitoring: Not available")

    try:
        from transmog.core.memory import get_global_batch_sizer

        batch_sizer = get_global_batch_sizer()
        adaptive_batch_size = batch_sizer.get_batch_size()
        print(f"  Adaptive batch size: {adaptive_batch_size}")
    except (ImportError, AttributeError):
        print("  Adaptive batch sizing: Not available")

    print("\n✅ Memory optimization features tested!")
    print("Available optimizations:")
    print("  • Low memory mode with adaptive batch sizing")
    print("  • Memory-aware caching adapts to available memory")
    print("  • Strategic garbage collection reduces memory pressure")
    print("  • Efficient data processing with in-place modifications")


def run_streaming_benchmarks() -> None:
    """Run streaming processing benchmarks."""
    print("\n" + "=" * 60)
    print("Streaming Processing Benchmark")
    print("=" * 60)

    test_sizes = [1000, 5000, 10000]
    output_dir = BASE_DIR / "benchmark_output"

    for size in test_sizes:
        print(f"\n🚀 Testing streaming with {size} records")
        print("-" * 40)

        data = create_test_data(size)

        # Test streaming vs in-memory processing
        print("  Testing in-memory processing...")
        start_time = time.time()
        start_memory = get_memory_usage()

        tm.flatten(data, name=f"memory_{size}")

        memory_time = time.time() - start_time
        memory_usage = get_memory_usage() - start_memory

        print(f"    In-memory: {memory_time:.3f}s, {memory_usage:.1f} MB")

        # Test streaming processing
        print("  Testing streaming processing...")
        start_time = time.time()
        start_memory = get_memory_usage()

        try:
            # Create output directory
            stream_output = output_dir / f"stream_{size}"
            stream_output.mkdir(parents=True, exist_ok=True)

            tm.flatten_stream(
                data,
                output_path=stream_output,
                name=f"stream_{size}",
                output_format="json",
                low_memory=True,
            )

            stream_time = time.time() - start_time
            stream_memory = get_memory_usage() - start_memory

            print(f"    Streaming: {stream_time:.3f}s, {stream_memory:.1f} MB")
            memory_savings = (memory_usage - stream_memory) / memory_usage * 100
            print(f"    Memory savings: {memory_savings:.1f}%")

        except Exception as e:
            print(f"    Streaming failed: {e}")

    # Clean up output directory
    import shutil

    if output_dir.exists():
        shutil.rmtree(output_dir)


def run_array_handling_benchmarks() -> None:
    """Run benchmarks for different array handling modes."""
    print("\n" + "=" * 60)
    print("Array Handling Benchmark")
    print("=" * 60)

    # Create data with many arrays
    array_heavy_data = []
    for i in range(1000):
        record = {
            "id": i,
            "name": f"Record {i}",
            "tags": [f"tag_{j}" for j in range(10)],
            "categories": [{"id": j, "name": f"Cat {j}"} for j in range(5)],
            "metrics": [
                {"value": j * 10, "timestamp": f"2023-01-{j + 1:02d}"} for j in range(8)
            ],
            "nested": {
                "items": [{"nested_id": j, "data": f"data_{j}"} for j in range(6)]
            },
        }
        array_heavy_data.append(record)

    array_modes = [
        ("Separate Tables", {"arrays": "separate"}),
        ("Inline JSON", {"arrays": "inline"}),
        ("Skip Arrays", {"arrays": "skip"}),
    ]

    print(f"Testing array handling with {len(array_heavy_data)} records...")

    for mode_name, config in array_modes:
        result = benchmark_configuration(mode_name, array_heavy_data, **config)

        if result.get("success", False):
            # Get the actual result to analyze table structure
            try:
                tm_result = tm.flatten(array_heavy_data, name="array_test", **config)
                print(f"    Tables created: {len(tm_result.all_tables)}")

                for table_name, table_data in tm_result.all_tables.items():
                    print(f"      {table_name}: {len(table_data)} records")

            except Exception as e:
                print(f"    Could not analyze result: {e}")


def save_results(
    results: dict[str, Any], filename: str = "benchmark_results.json"
) -> None:
    """Save benchmark results to a JSON file.

    Args:
        results: Results dictionary to save
        filename: Output filename
    """
    result_file = BASE_DIR / filename
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nBenchmark results saved to {result_file}")


def print_summary(results: dict[str, Any]) -> None:
    """Print a summary of benchmark results.

    Args:
        results: Results dictionary to summarize
    """
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)

    for record_count, configs in results.items():
        print(f"\n📊 {record_count} records:")

        successful_configs = {
            k: v for k, v in configs.items() if v.get("success", False)
        }

        if successful_configs:
            # Find fastest configuration
            fastest = max(successful_configs.items(), key=lambda x: x[1]["throughput"])
            print(
                f"  Fastest: {fastest[0]} ({fastest[1]['throughput']:.0f} records/sec)"
            )

            # Find most memory efficient
            most_efficient = min(
                successful_configs.items(), key=lambda x: x[1]["memory_used_mb"]
            )
            memory_mb = most_efficient[1]["memory_used_mb"]
            print(f"  Most memory efficient: {most_efficient[0]} ({memory_mb:.1f} MB)")

            # Calculate average throughput
            avg_throughput = sum(
                c["throughput"] for c in successful_configs.values()
            ) / len(successful_configs)
            print(f"  Average throughput: {avg_throughput:.0f} records/sec")
        else:
            print("  No successful configurations")


def main() -> None:
    """Run benchmarks and save results to file."""
    import argparse

    parser = argparse.ArgumentParser(description="Run transmog benchmarks")
    parser.add_argument(
        "--memory", action="store_true", help="Run memory optimization benchmarks"
    )
    parser.add_argument(
        "--standard", action="store_true", help="Run standard benchmarks"
    )
    parser.add_argument(
        "--streaming", action="store_true", help="Run streaming benchmarks"
    )
    parser.add_argument(
        "--arrays", action="store_true", help="Run array handling benchmarks"
    )
    parser.add_argument("--all", action="store_true", help="Run all benchmark suites")
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        default=[10, 100, 1000, 5000],
        help="Record counts to test (default: 10 100 1000 5000)",
    )

    args = parser.parse_args()

    # Default to standard if no specific option is chosen
    if not any([args.memory, args.standard, args.streaming, args.arrays, args.all]):
        args.standard = True

    print(f"Transmog Benchmark Suite v{tm.__version__}")
    print("=" * 60)

    all_results = {}

    if args.standard or args.all:
        print("\n🚀 Running standard benchmarks...")
        standard_results = run_standard_benchmarks(args.sizes)
        all_results["standard"] = standard_results
        print_summary(standard_results)

    if args.memory or args.all:
        print("\n🧠 Running memory optimization benchmarks...")
        run_memory_benchmarks()

    if args.streaming or args.all:
        print("\n🌊 Running streaming benchmarks...")
        run_streaming_benchmarks()

    if args.arrays or args.all:
        print("\n📊 Running array handling benchmarks...")
        run_array_handling_benchmarks()

    # Save results if we have any
    if all_results:
        save_results(all_results)

    print("\n✅ Benchmark suite completed!")


if __name__ == "__main__":
    main()
