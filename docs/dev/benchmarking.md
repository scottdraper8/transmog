# Benchmarking Guide

This guide explains how to use the benchmarking tools available in Transmog to measure performance and
identify optimization opportunities.

## Overview

Transmog provides two complementary benchmarking approaches:

1. **Command-line benchmarking script** (`scripts/run_benchmarks.py`): A standalone tool for end-to-end benchmarks
2. **Pytest benchmark tests** (`tests/benchmarks/`): Fine-grained benchmarks of specific components

Each approach serves different purposes and is useful in different contexts.

## Command-Line Benchmarking Script

The `scripts/run_benchmarks.py` script provides a convenient way to perform end-to-end benchmarks of
Transmog. This is ideal for:

- Evaluating overall performance with different configurations
- Testing performance on different datasets
- Monitoring performance changes between versions
- Experimenting with different processing modes and strategies

### Running the Script

```bash
# Basic benchmark with default settings
python scripts/run_benchmarks.py

# Configure the benchmark
python scripts/run_benchmarks.py --records 5000 --complexity complex

# Test different processing modes
python scripts/run_benchmarks.py --mode streaming
python scripts/run_benchmarks.py --mode chunked --chunk-size 1000

# Test different strategies
python scripts/run_benchmarks.py --strategy memory
python scripts/run_benchmarks.py --strategy performance

# Enable deterministic IDs
python scripts/run_benchmarks.py --deterministic-ids

# Test error recovery strategies
python scripts/run_benchmarks.py --recovery partial

# Save results to a file
python scripts/run_benchmarks.py --output benchmark_results.json
```

### Command-Line Options

- `--records`: Number of records to process (default: 1000)
- `--complexity`: Data complexity - simple, medium, or complex (default: medium)
- `--output`: Output file path for benchmark results (default: none)
- `--mode`: Processing mode - standard, streaming, or chunked (default: standard)
- `--strategy`: Processing strategy - standard, memory, or performance (default: standard)
- `--deterministic-ids`: Enable deterministic ID generation (flag)
- `--recovery`: Recovery strategy - strict, skip, or partial
- `--chunk-size`: Chunk size for chunked processing mode (default: 500)

### Example Results

The benchmark script outputs results to the console and optionally to a JSON file:

```json
{
  "to_dict": {
    "execution_time": 0.0002,
    "memory_mb": 0.0151
  },
  "to_json_objects": {
    "execution_time": 0.0005,
    "memory_mb": 0.0143
  },
  "to_json_bytes": {
    "execution_time": 0.0016,
    "memory_mb": 0.3966
  },
  "to_csv_bytes": {
    "execution_time": 0.0035,
    "memory_mb": 0.1751
  },
  "to_pyarrow_tables": {
    "execution_time": 0.0029,
    "memory_mb": 0.2096
  },
  "to_parquet_bytes": {
    "execution_time": 0.0022,
    "memory_mb": 0.0848
  },
  "__metadata__": {
    "num_records": 100,
    "complexity": "complex",
    "mode": "standard",
    "strategy": "standard",
    "deterministic_ids": false,
    "recovery_strategy": null,
    "chunk_size": null,
    "timestamp": "2023-09-10 14:45:30"
  }
}
```

## Pytest Benchmark Tests

Transmog includes benchmark tests built with pytest and the pytest-benchmark plugin. These tests provide
more detailed performance metrics for specific components and are ideal for:

- Comparing different implementations of the same functionality
- Identifying performance bottlenecks in specific components
- Evaluating performance impact of code changes
- Generating detailed statistical reports

### Test Structure

Benchmark tests are located in the `tests/benchmarks/` directory:

- `test_output_format_benchmarks.py`: Benchmarks for output format conversions
- `test_csv_reader_benchmarks.py`: Benchmarks for CSV reading performance

### Running Benchmark Tests

```bash
# Run all benchmark tests
pytest tests/benchmarks/

# Run specific benchmark test
pytest tests/benchmarks/test_output_format_benchmarks.py

# Run a specific benchmark
pytest tests/benchmarks/test_output_format_benchmarks.py::test_benchmark_to_dict
```

### Examples of Pytest Benchmark Tests

Here's an example of a benchmark test from `test_output_format_benchmarks.py`:

```python
def test_benchmark_to_dict(processed_result, benchmark):
    """Benchmark the to_dict method."""
    benchmark(processed_result.to_dict)
```

And a more complex benchmark from `test_csv_reader_benchmarks.py`:

```python
@pytest.mark.benchmark
def test_pyarrow_vs_builtin_performance(benchmark):
    """Benchmark PyArrow vs built-in CSV reader performance."""
    # Generate test data (moderate size - 1000 rows, 5 columns)
    csv_path = self.create_large_csv(rows=1000, cols=5)

    try:
        # Benchmark the PyArrow implementation
        result = benchmark.pedantic(
            lambda: read_csv_file(csv_path),
            iterations=3,
            rounds=5,
        )
    finally:
        # Clean up
        os.unlink(csv_path)
```

## When to Use Each Approach

### Use the Command-Line Script When

- You want to benchmark complete package workflows
- You need to compare different processing modes and strategies
- You want to test performance with different data types and sizes
- You want to track performance changes between versions
- You need a simple tool that doesn't require pytest knowledge

### Use Pytest Benchmark Tests When

- You need detailed statistics about performance
- You want to benchmark specific components in isolation
- You're comparing different implementations of the same functionality
- You want to run benchmarks as part of your development workflow
- You need to integrate benchmarks with your test suite

## Performance Optimization Tips

When optimizing Transmog performance, consider these strategies:

1. **Choose the right processing mode**:
   - Use `standard` mode for small datasets
   - Use `chunked` mode for large datasets that fit in memory
   - Use `streaming` mode for very large datasets

2. **Configure processing strategies**:
   - Use `memory_optimized()` when memory usage is a concern
   - Use `performance_optimized()` when speed is the priority

3. **Optimize caching**:
   - Enable caching for datasets with many repeated values
   - Configure cache size based on your memory constraints
   - Consider clearing the cache between processing batches

4. **Use appropriate output formats**:
   - Use `to_dict()` for in-memory processing
   - Use `to_bytes()` methods for serialization
   - Use `streaming` writers for memory-efficient output

## Contributing Performance Improvements

When contributing performance improvements to Transmog:

1. **Benchmark before and after your changes** to quantify improvements
2. **Document the performance impact** in your pull request
3. **Add benchmark tests** for new optimization techniques
4. **Consider trade-offs** between memory usage, speed, and code complexity

## Related Documentation

- [Testing Guide](testing.md): Learn about the testing framework
- [Strategies](../user/strategies.md): Learn about processing strategies
- [In-Memory Processing](../user/in-memory-processing.md): Learn about memory optimization
- [Streaming](../user/streaming.md): Learn about streaming processing
