# Performance Optimization

This guide covers techniques for optimizing Transmog's performance when processing large datasets, including
caching strategies and parallel processing.

## Part 1: Value Processing Cache

Transmog includes a value processing cache to improve performance when processing large datasets with
repeated values.

### Cache Configuration

The cache system can be configured through the `TransmogConfig` object:

```python
import transmog as tm

# Default configuration (enabled with default settings)
processor = tm.Processor()

# Configure with disabled cache for memory-sensitive applications
processor = tm.Processor(
    tm.TransmogConfig.default().with_caching(enabled=False)
)

# Configure with larger cache for performance-critical applications
processor = tm.Processor(
    tm.TransmogConfig.default().with_caching(maxsize=50000)
)

# Configure to clear cache after batch processing to prevent memory growth
processor = tm.Processor(
    tm.TransmogConfig.default().with_caching(clear_after_batch=True)
)
```

### Configuration Options

The cache configuration has the following options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `True` | Whether caching is enabled |
| `maxsize` | int | `10000` | Maximum number of entries in the LRU cache |
| `clear_after_batch` | bool | `False` | Whether to clear cache after batch processing |

### Predefined Configurations

Transmog provides predefined configurations for common use cases:

- **Default Configuration**:
  - Cache enabled
  - Cache size: 10,000 entries
  - No automatic clearing

- **Memory-Optimized Configuration** (`Processor.memory_optimized()`):
  - Cache enabled
  - Cache size: 1,000 entries (smaller to save memory)
  - Automatic clearing after batch processing

- **Performance-Optimized Configuration** (`Processor.performance_optimized()`):
  - Cache enabled
  - Cache size: 50,000 entries (larger for better hit rate)
  - No automatic clearing

### Manual Cache Management

You can manually clear the cache at any point:

```python
# Clear cache manually
processor.clear_cache()
```

### When to Adjust Cache Settings

Consider adjusting cache settings in these scenarios:

- **Large Datasets with High Value Diversity**: Increase cache size to improve hit rate
- **Limited Memory Environments**: Reduce cache size or disable caching
- **Long-Running Processes**: Enable `clear_after_batch` to prevent memory accumulation
- **High Value Repetition**: Increase cache size to maximize performance benefit

## Part 2: Concurrency and Parallel Processing

Transmog can be used with various concurrency methods to effectively process large datasets in parallel.

### Basic Parallel Processing

For simple parallel processing, you can use Python's `concurrent.futures` module with Transmog:

```python
import transmog as tm
from concurrent.futures import ThreadPoolExecutor
import json

# Load your data
with open("large_dataset.json", "r") as f:
    data = json.load(f)

# Split data into chunks
def split_into_chunks(data, chunk_size=1000):
    """Split a list into chunks of specified size."""
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

chunks = split_into_chunks(data, chunk_size=1000)

# Process in parallel
processor = tm.Processor()

with ThreadPoolExecutor(max_workers=4) as executor:
    # Submit processing jobs
    futures = [
        executor.submit(
            processor.process_batch,
            batch_data=chunk,
            entity_name="customers"
        )
        for chunk in chunks
    ]

    # Collect results
    results = [future.result() for future in futures]

# Combine results
combined_result = tm.ProcessingResult.combine_results(results)

# Write to Parquet
combined_result.write_all_parquet(base_path="output/data")
```

### Chunked Processing

For very large datasets that don't fit in memory, use chunked processing:

```python
import transmog as tm
import json

# Initialize processor
processor = tm.Processor.memory_optimized()

# Process a large file in chunks
result = processor.process_chunked(
    "very_large_dataset.json",
    entity_name="records",
    chunk_size=500  # Process 500 records at a time
)
```

### Hybrid Approach: Parallel Chunked Processing

For the best performance with very large datasets, combine chunking with parallelism:

```python
import transmog as tm
import json
from concurrent.futures import ProcessPoolExecutor
import os

# Function to process a file chunk
def process_file_chunk(file_path, start_line, num_lines, entity_name):
    # Read specific chunk from file
    records = []
    with open(file_path, 'r') as f:
        # Skip to start position
        for _ in range(start_line):
            next(f)
        # Read chunk
        for _ in range(num_lines):
            try:
                line = next(f).strip()
                if line:
                    records.append(json.loads(line))
            except StopIteration:
                break

    # Process chunk
    processor = tm.Processor()
    return processor.process_batch(records, entity_name=entity_name)

# Count total lines in file
def count_lines(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)

# Process large file in parallel chunks
def process_large_file(file_path, entity_name, chunk_size=10000, workers=4):
    total_lines = count_lines(file_path)
    chunks = [(i, min(chunk_size, total_lines - i))
              for i in range(0, total_lines, chunk_size)]

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                process_file_chunk,
                file_path,
                start_line,
                num_lines,
                entity_name
            )
            for start_line, num_lines in chunks
        ]

        # Collect results
        results = [future.result() for future in futures]

    # Combine results
    return tm.ProcessingResult.combine_results(results, entity_name=entity_name)

# Usage
result = process_large_file(
    "huge_dataset.jsonl",
    entity_name="events",
    chunk_size=50000,
    workers=os.cpu_count()
)
```

### Error Handling in Parallel Processing

When using parallel processing, handle errors properly:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

results = []
failed_chunks = []

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {
        executor.submit(processor.process_batch, chunk, "records"): i
        for i, chunk in enumerate(chunks)
    }

    for future in as_completed(futures):
        chunk_index = futures[future]
        try:
            result = future.result()
            results.append(result)
        except Exception as e:
            print(f"Chunk {chunk_index} failed: {str(e)}")
            failed_chunks.append(chunk_index)

# Handle any failed chunks
if failed_chunks:
    print(f"Failed chunks: {failed_chunks}")

# Combine successful results
if results:
    combined_result = tm.ProcessingResult.combine_results(results)
else:
    print("All chunks failed")
```

## Part 3: Performance Tuning Best Practices

When optimizing Transmog for large-scale data processing, consider these best practices:

### Memory Optimization

1. **Use Appropriate Processing Mode**:
   - `processor = tm.Processor.memory_optimized()` for memory-constrained environments
   - `processor = tm.Processor.performance_optimized()` for speed-critical applications

2. **Configure Cache Appropriately**:
   - Increase cache size for high repeat value datasets
   - Disable cache for low repeat, high variety datasets
   - Enable `clear_after_batch` for long-running processes

3. **Process in Chunks**:
   - Use `process_chunked()` for datasets larger than available memory
   - Choose an appropriate chunk size (typically 500-5000 records)

### Parallel Processing Optimization

1. **Select Appropriate Concurrency Method**:
   - ThreadPoolExecutor for I/O-bound operations
   - ProcessPoolExecutor for CPU-bound operations

2. **Choose Worker Count Wisely**:
   - Start with `os.cpu_count()` for CPU-bound work
   - Use 2-4× `os.cpu_count()` for I/O-bound work
   - Benchmark to find optimal worker count for your specific hardware

3. **Balance Chunk Size**:
   - Too small: excessive overhead from worker management
   - Too large: poor load balancing and memory pressure
   - Aim for chunks that process in 0.5-2 seconds for good balance

### Output Format Selection

1. **In-Memory Processing**:
   - Use `to_dict()` or `to_json_objects()` for fastest in-memory processing
   - Use `conversion_mode=ConversionMode.LAZY` to defer conversions until needed

2. **File Output**:
   - Use Parquet for most efficient storage and fastest reading
   - Use CSV for widest compatibility
   - Use JSON for human readability and preservation of types

3. **Streaming Output**:
   - Use streaming writers for very large outputs
   - `stream_process_file()` for direct file-to-file processing

### Combining Strategies

For optimal performance with very large datasets:

1. Use memory-optimized configuration
2. Process in parallel with appropriate worker count
3. Use chunked processing with reasonable chunk size
4. Stream directly to output when possible
5. Use Parquet output format with compression

```python
import transmog as tm
import os

# Create optimized processor
processor = tm.Processor.memory_optimized()

# Stream process directly to parquet with parallel workers
from concurrent.futures import ProcessPoolExecutor

def process_chunk(file_path, start_idx, end_idx, output_dir):
    # Create a processor for this worker
    proc = tm.Processor.memory_optimized()

    # Process chunk and write directly to parquet
    proc.stream_process_file(
        file_path,
        entity_name="records",
        output_format="parquet",
        output_destination=f"{output_dir}/chunk_{start_idx}_{end_idx}",
        start_line=start_idx,
        end_line=end_idx,
        compression="snappy"
    )

    return f"Processed lines {start_idx}-{end_idx}"

# Split file processing across workers
file_size = 1000000  # Assume 1M lines
chunk_size = 100000  # 100K per chunk
chunks = [(i, min(i + chunk_size, file_size))
          for i in range(0, file_size, chunk_size)]

# Process in parallel
with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
    futures = [
        executor.submit(process_chunk,
                       "huge_file.jsonl",
                       start,
                       end,
                       "output/data")
        for start, end in chunks
    ]

    for future in futures:
        print(future.result())
```
