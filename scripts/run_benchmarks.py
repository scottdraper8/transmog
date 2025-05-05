#!/usr/bin/env python
"""
Performance benchmarking script for Transmog output formats.

This script runs benchmarks for the various output format methods to
compare their performance characteristics.
"""

import argparse
import json
import os
import sys
import time
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# Try to import from installed package first, fall back to development setup
try:
    from transmog import Processor, TransmogConfig, ProcessingMode
except ImportError:
    # Add parent directory to path for development mode
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from transmog import Processor, TransmogConfig, ProcessingMode


def generate_test_data(
    num_records: int = 1000, complexity: str = "medium"
) -> List[Dict[str, Any]]:
    """
    Generate synthetic test data with the specified complexity.

    Args:
        num_records: Number of records to generate
        complexity: Complexity level (simple, medium, complex)

    Returns:
        List of test records
    """
    data = []

    # Define complexity levels
    if complexity == "simple":
        # Simple flat structure
        for i in range(num_records):
            data.append(
                {
                    "id": f"record-{i}",
                    "name": f"Record {i}",
                    "value": i * 10,
                    "active": i % 2 == 0,
                }
            )

    elif complexity == "medium":
        # Medium complexity with some nesting
        for i in range(num_records):
            data.append(
                {
                    "id": f"record-{i}",
                    "metadata": {
                        "created": "2023-01-01",
                        "modified": "2023-03-15",
                        "source": "benchmark",
                    },
                    "details": {
                        "name": f"Record {i}",
                        "description": f"Test record {i} for benchmarking",
                        "value": i * 10,
                        "tags": ["test", "benchmark", f"tag-{i}"],
                    },
                    "status": {
                        "active": i % 2 == 0,
                        "approved": i % 3 == 0,
                        "visible": i % 5 != 0,
                    },
                }
            )

    else:  # complex
        # Complex deeply nested structure with arrays
        for i in range(num_records):
            data.append(
                {
                    "id": f"record-{i}",
                    "metadata": {
                        "created": "2023-01-01",
                        "modified": "2023-03-15",
                        "source": "benchmark",
                        "authors": [
                            {"id": "author-1", "name": "Alice", "role": "primary"},
                            {"id": "author-2", "name": "Bob", "role": "contributor"},
                        ],
                    },
                    "details": {
                        "name": f"Record {i}",
                        "description": f"Test record {i} for benchmarking",
                        "value": i * 10,
                        "tags": ["test", "benchmark", f"tag-{i}"],
                        "categories": [
                            {
                                "id": "cat-1",
                                "name": "Category 1",
                                "subcategories": [
                                    {"id": "subcat-1", "name": "Subcategory 1"},
                                    {"id": "subcat-2", "name": "Subcategory 2"},
                                ],
                            },
                            {
                                "id": "cat-2",
                                "name": "Category 2",
                                "subcategories": [
                                    {"id": "subcat-3", "name": "Subcategory 3"}
                                ],
                            },
                        ],
                    },
                    "status": {
                        "active": i % 2 == 0,
                        "approved": i % 3 == 0,
                        "visible": i % 5 != 0,
                        "history": [
                            {"date": "2023-01-01", "status": "created"},
                            {"date": "2023-02-15", "status": "reviewed"},
                            {"date": "2023-03-01", "status": "approved"},
                        ],
                    },
                }
            )

    return data


def benchmark_method(
    method_name: str, func, *args, **kwargs
) -> Tuple[float, float, Any]:
    """
    Benchmark a single method execution time and memory usage.

    Args:
        method_name: Name of the method being benchmarked
        func: The function to benchmark
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function

    Returns:
        Tuple of (execution time in seconds, approximate memory size in MB, result)
    """
    # Measure execution time
    start_time = time.time()
    result = func(*args, **kwargs)
    execution_time = time.time() - start_time

    # Approximate memory size
    memory_size = 0

    if isinstance(result, dict):
        # Estimate dictionary size
        memory_size = sys.getsizeof(result)
        # Add size of values
        for key, value in result.items():
            if isinstance(value, (bytes, bytearray)):
                memory_size += len(value)
            else:
                memory_size += sys.getsizeof(value)

    # Convert to MB
    memory_size_mb = memory_size / (1024 * 1024)

    return execution_time, memory_size_mb, result


def get_processor_for_strategy(strategy: str) -> Processor:
    """
    Get processor configured for a specific strategy.

    Args:
        strategy: The strategy to use (standard, memory, performance)

    Returns:
        Configured Processor instance
    """
    if strategy == "memory":
        return Processor.memory_optimized()
    elif strategy == "performance":
        return Processor.performance_optimized()
    else:  # standard
        return Processor()


def run_benchmarks(
    num_records: int = 1000,
    complexity: str = "medium",
    mode: str = "standard",
    strategy: str = "standard",
) -> Dict[str, Dict[str, float]]:
    """
    Run benchmarks for all output format methods.

    Args:
        num_records: Number of records to use for benchmarking
        complexity: Complexity level of the test data
        mode: Processing mode (standard or streaming)
        strategy: Processing strategy (standard, memory, or performance)

    Returns:
        Dictionary of benchmark results
    """
    print(
        f"Running benchmarks with {num_records} records of {complexity} complexity..."
    )
    print(f"Mode: {mode}, Strategy: {strategy}")

    # Generate test data
    data = generate_test_data(num_records, complexity)

    # Get processor based on strategy
    processor = get_processor_for_strategy(strategy)

    benchmarks = {}

    if mode == "streaming":
        # Benchmark streaming processing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Measure execution time for streaming to JSON
            print("\nBenchmarking streaming to JSON...")
            start_time = time.time()
            processor.stream_process(
                data=data,
                entity_name="benchmark",
                output_format="json",
                output_destination=os.path.join(temp_dir, "output.json"),
            )
            json_time = time.time() - start_time
            benchmarks["stream_to_json"] = {"execution_time": json_time}
            print(f"  Execution time: {json_time:.4f} seconds")

            # Measure execution time for streaming to CSV
            print("\nBenchmarking streaming to CSV...")
            start_time = time.time()
            processor.stream_process(
                data=data,
                entity_name="benchmark",
                output_format="csv",
                output_destination=os.path.join(temp_dir, "output.csv"),
            )
            csv_time = time.time() - start_time
            benchmarks["stream_to_csv"] = {"execution_time": csv_time}
            print(f"  Execution time: {csv_time:.4f} seconds")

            # Measure execution time for streaming to Parquet
            try:
                print("\nBenchmarking streaming to Parquet...")
                start_time = time.time()
                processor.stream_process(
                    data=data,
                    entity_name="benchmark",
                    output_format="parquet",
                    output_destination=os.path.join(temp_dir, "output"),
                )
                parquet_time = time.time() - start_time
                benchmarks["stream_to_parquet"] = {"execution_time": parquet_time}
                print(f"  Execution time: {parquet_time:.4f} seconds")
            except ImportError:
                print("PyArrow not available. Skipping Parquet streaming benchmark.")

    else:  # standard mode
        # Process the data
        result = processor.process(data, entity_name="benchmark")

        # Dictionary to store benchmark results
        benchmarks = {}

        # Benchmark to_dict()
        print("\nBenchmarking to_dict()...")
        exec_time, memory_mb, _ = benchmark_method("to_dict", result.to_dict)
        benchmarks["to_dict"] = {"execution_time": exec_time, "memory_mb": memory_mb}
        print(f"  Execution time: {exec_time:.4f} seconds")
        print(f"  Memory usage: {memory_mb:.2f} MB")

        # Benchmark to_json_objects()
        print("\nBenchmarking to_json_objects()...")
        exec_time, memory_mb, _ = benchmark_method(
            "to_json_objects", result.to_json_objects
        )
        benchmarks["to_json_objects"] = {
            "execution_time": exec_time,
            "memory_mb": memory_mb,
        }
        print(f"  Execution time: {exec_time:.4f} seconds")
        print(f"  Memory usage: {memory_mb:.2f} MB")

        # Benchmark to_json_bytes()
        print("\nBenchmarking to_json_bytes()...")
        exec_time, memory_mb, _ = benchmark_method(
            "to_json_bytes", result.to_json_bytes
        )
        benchmarks["to_json_bytes"] = {
            "execution_time": exec_time,
            "memory_mb": memory_mb,
        }
        print(f"  Execution time: {exec_time:.4f} seconds")
        print(f"  Memory usage: {memory_mb:.2f} MB")

        # Benchmark to_csv_bytes()
        print("\nBenchmarking to_csv_bytes()...")
        exec_time, memory_mb, _ = benchmark_method("to_csv_bytes", result.to_csv_bytes)
        benchmarks["to_csv_bytes"] = {
            "execution_time": exec_time,
            "memory_mb": memory_mb,
        }
        print(f"  Execution time: {exec_time:.4f} seconds")
        print(f"  Memory usage: {memory_mb:.2f} MB")

        try:
            # Benchmark to_pyarrow_tables()
            print("\nBenchmarking to_pyarrow_tables()...")
            exec_time, memory_mb, _ = benchmark_method(
                "to_pyarrow_tables", result.to_pyarrow_tables
            )
            benchmarks["to_pyarrow_tables"] = {
                "execution_time": exec_time,
                "memory_mb": memory_mb,
            }
            print(f"  Execution time: {exec_time:.4f} seconds")
            print(f"  Memory usage: {memory_mb:.2f} MB")

            # Benchmark to_parquet_bytes()
            print("\nBenchmarking to_parquet_bytes()...")
            exec_time, memory_mb, _ = benchmark_method(
                "to_parquet_bytes", result.to_parquet_bytes
            )
            benchmarks["to_parquet_bytes"] = {
                "execution_time": exec_time,
                "memory_mb": memory_mb,
            }
            print(f"  Execution time: {exec_time:.4f} seconds")
            print(f"  Memory usage: {memory_mb:.2f} MB")
        except ImportError:
            print("PyArrow not available. Skipping PyArrow-dependent benchmarks.")

    # Add benchmark metadata
    benchmarks["__metadata__"] = {
        "num_records": num_records,
        "complexity": complexity,
        "mode": mode,
        "strategy": strategy,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    return benchmarks


def save_results(results: Dict[str, Dict[str, float]], output_path: str) -> None:
    """
    Save benchmark results to a JSON file.

    Args:
        results: Benchmark results dictionary
        output_path: Path to save the results file
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nBenchmark results saved to {output_path}")


def main() -> None:
    """Run the benchmark suite."""
    parser = argparse.ArgumentParser(
        description="Run performance benchmarks for Transmog output formats"
    )
    parser.add_argument(
        "--records",
        type=int,
        default=1000,
        help="Number of records to use (default: 1000)",
    )
    parser.add_argument(
        "--complexity",
        choices=["simple", "medium", "complex"],
        default="medium",
        help="Complexity of test data (default: medium)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_results.json",
        help="Output file path (default: benchmark_results.json)",
    )
    parser.add_argument(
        "--mode",
        choices=["standard", "streaming"],
        default="standard",
        help="Processing mode (default: standard)",
    )
    parser.add_argument(
        "--strategy",
        choices=["standard", "memory", "performance"],
        default="standard",
        help="Processing strategy (default: standard)",
    )
    args = parser.parse_args()

    # Run benchmarks
    results = run_benchmarks(args.records, args.complexity, args.mode, args.strategy)

    # Save results
    save_results(results, args.output)

    # Print summary
    print("\nBenchmark Summary:")
    for method, metrics in results.items():
        if method == "__metadata__":
            continue
        print(f"  {method}:")
        for metric_name, metric_value in metrics.items():
            print(f"    {metric_name}: {metric_value:.4f}")


if __name__ == "__main__":
    main()
