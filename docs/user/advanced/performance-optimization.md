# Performance Optimization

This guide covers techniques for optimizing Transmog's performance when processing large datasets, including
memory optimization, chunking strategies, and parallel processing.

## Memory Optimization

Transmog provides several options to optimize memory usage when processing large datasets.

### Low Memory Mode

The `low_memory` parameter enables memory-optimized processing:

```python
import transmog as tm

# Process with low memory usage
result = tm.flatten(
    data,
    name="records",
    low_memory=True
)
```

When `low_memory=True` is enabled:

1. Internal caching is minimized
2. Intermediate data structures are optimized for memory usage
3. Garbage collection is more aggressive
4. Batch sizes are optimized for memory efficiency

### Chunked Processing

For large datasets, you can process data in chunks to reduce memory usage:

```python
import transmog as tm

# Process data in chunks
result = tm.flatten(
    large_data,
    name="records",
    chunk_size=1000  # Process 1000 records at a time
)
```

The `chunk_size` parameter controls:
- How many records are processed at a time
- Memory usage during processing
- Processing efficiency

### Streaming Processing

For very large datasets that don't fit in memory, use streaming processing:

```python
import transmog as tm

# Stream process a large file
tm.flatten_stream(
    file_path="very_large_dataset.json",
    name="records",
    output_path="output_dir",
    output_format="parquet",
    chunk_size=500,  # Process 500 records at a time
    low_memory=True  # Enable memory optimization
)
```

## Performance Optimization Techniques

### Optimizing for Speed

To optimize for processing speed:

```python
import transmog as tm

# Process with performance optimization
result = tm.flatten(
    data,
    name="records",
    chunk_size=5000,  # Larger chunks for better performance
    low_memory=False  # Disable memory optimization for better speed
)
```

### Optimizing for Memory Usage

To optimize for minimal memory usage:

```python
import transmog as tm

# Process with memory optimization
result = tm.flatten(
    data,
    name="records",
    chunk_size=500,    # Smaller chunks for lower memory usage
    low_memory=True    # Enable memory optimization
)
```

### Balancing Speed and Memory Usage

For a balance between speed and memory usage:

```python
import transmog as tm

# Process with balanced optimization
result = tm.flatten(
    data,
    name="records",
    chunk_size=2000,   # Medium chunk size
    low_memory=True    # Enable memory optimization
)
```

## Parallel Processing

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
results = []

with ThreadPoolExecutor(max_workers=4) as executor:
    # Submit processing jobs
    futures = [
        executor.submit(
            tm.flatten,
            data=chunk,
            name="customers"
        )
        for chunk in chunks
    ]

    # Collect results
    results = [future.result() for future in futures]

# Combine results manually
main_records = []
child_tables = {}

for result in results:
    # Combine main table records
    main_records.extend(result.main)
    
    # Combine child table records
    for table_name, records in result.tables.items():
        if table_name not in child_tables:
            child_tables[table_name] = []
        child_tables[table_name].extend(records)

# Create a combined result (for demonstration - in practice you might write directly to files)
print(f"Total main records: {len(main_records)}")
for table_name, records in child_tables.items():
    print(f"Total {table_name} records: {len(records)}")
```

### Parallel File Processing

For processing multiple files in parallel:

```python
import transmog as tm
from concurrent.futures import ProcessPoolExecutor
import os

# Function to process a single file
def process_file(file_path, output_dir):
    file_name = os.path.basename(file_path)
    output_name = os.path.splitext(file_name)[0]
    
    # Process file
    result = tm.flatten_file(
        file_path,
        name=output_name,
        low_memory=True
    )
    
    # Save result
    result.save(os.path.join(output_dir, output_name))
    
    return f"Processed {file_name}"

# Process multiple files in parallel
file_paths = ["data1.json", "data2.json", "data3.json"]
output_dir = "output"

with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
    futures = [
        executor.submit(process_file, file_path, output_dir)
        for file_path in file_paths
    ]
    
    # Print results as they complete
    for future in concurrent.futures.as_completed(futures):
        print(future.result())
```

### Hybrid Approach: Parallel Chunked Processing

For the best performance with very large datasets, combine chunking with parallelism:

```python
import transmog as tm
import json
from concurrent.futures import ProcessPoolExecutor
import os

# Function to process a file chunk
def process_file_chunk(file_path, start_line, num_lines, name):
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
    return tm.flatten(records, name=name, low_memory=True)

# Count total lines in file
def count_lines(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)

# Process large file in parallel chunks
def process_large_file(file_path, name, output_dir, chunk_size=10000, workers=4):
    total_lines = count_lines(file_path)
    chunks = [(i, min(chunk_size, total_lines - i))
              for i in range(0, total_lines, chunk_size)]

    os.makedirs(output_dir, exist_ok=True)
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                process_file_chunk,
                file_path,
                start_line,
                num_lines,
                name
            )
            for start_line, num_lines in chunks
        ]

        # Save results as they complete
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            chunk_dir = os.path.join(output_dir, f"chunk_{i}")
            result.save(chunk_dir)
            print(f"Saved chunk {i} to {chunk_dir}")

# Usage
process_large_file(
    "huge_dataset.jsonl",
    name="events",
    output_dir="output/events",
    chunk_size=50000,
    workers=os.cpu_count()
)
```

### Error Handling in Parallel Processing

When using parallel processing, handle errors properly:

```python
import transmog as tm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Split data into chunks
chunks = split_into_chunks(data, chunk_size=1000)

results = []
failed_chunks = []

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {
        executor.submit(tm.flatten, data=chunk, name="records"): i
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
    # Optionally retry with different settings
    for i in failed_chunks:
        try:
            # Try again with more lenient error handling
            result = tm.flatten(
                chunks[i], 
                name="records",
                error_handling="skip"
            )
            results.append(result)
            print(f"Successfully reprocessed chunk {i}")
        except Exception as e:
            print(f"Chunk {i} failed again: {str(e)}")
```

## Performance Tuning Guidelines

### Optimizing for Different Scenarios

| Scenario | Recommended Configuration |
|----------|---------------------------|
| **Large dataset, limited memory** | `low_memory=True`, small `chunk_size` (500-1000) |
| **Large dataset, fast processing** | Larger `chunk_size` (5000+), parallel processing |
| **Streaming large files** | `flatten_stream()` with `low_memory=True` |
| **Many small files** | Parallel processing with `ProcessPoolExecutor` |
| **Complex nested structures** | Medium `chunk_size` (1000-2000), `low_memory=True` |

### Memory Usage Monitoring

Monitor memory usage to find the optimal configuration:

```python
import psutil
import os
from memory_profiler import memory_usage

# Function to monitor
def process_with_settings(chunk_size, low_memory):
    tm.flatten(
        large_data,
        name="records",
        chunk_size=chunk_size,
        low_memory=low_memory
    )

# Test different configurations
settings = [
    (500, True),   # Small chunks with memory optimization
    (2000, True),  # Medium chunks with memory optimization
    (5000, False), # Large chunks without memory optimization
]

for chunk_size, low_memory in settings:
    print(f"Testing chunk_size={chunk_size}, low_memory={low_memory}")
    mem_usage = memory_usage(
        lambda: process_with_settings(chunk_size, low_memory)
    )
    print(f"Peak memory usage: {max(mem_usage)} MiB")
```

## Best Practices

1. **Start with default settings** and adjust based on performance monitoring
2. **For very large datasets**, use `flatten_stream()` with `low_memory=True`
3. **For parallel processing**, use appropriate chunk sizes to balance memory and CPU usage
4. **Monitor memory usage** to find optimal settings for your specific data
5. **Consider data characteristics** - deeply nested data may require different settings than flat data
6. **Use appropriate error handling** in parallel processing scenarios
7. **Balance workers** with available CPU cores - too many workers can cause thrashing
