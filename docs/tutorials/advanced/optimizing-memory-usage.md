# Optimizing Memory Usage

This tutorial demonstrates advanced techniques for minimizing memory usage when processing large datasets with Transmog.

## Why Memory Optimization Matters

Processing large, nested JSON datasets can consume significant memory due to:

- Multiple copies of data during transformation
- In-memory caching of intermediate results
- Array extraction creating additional data structures
- Output format conversion requiring additional memory

## Memory Usage Patterns in Transmog

Transmog's memory usage is affected by several factors:

1. **Input Size**: The size and complexity of input data
2. **Processing Strategy**: How data is loaded and processed
3. **Chunk Size**: How many records are processed at once
4. **Low Memory Mode**: Whether memory optimization is enabled
5. **Output Management**: How results are stored and returned

## Basic Memory Optimization

Start with the built-in low memory mode:

```python
import transmog as tm

# Process a file with memory optimization
result = tm.flatten_file(
    file_path="large_file.json",
    name="records",
    low_memory=True  # Enable memory optimization
)

# Write results to files
result.save("output_directory", format="parquet")
```

## Streaming for Memory Efficiency

For very large datasets, streaming is the most memory-efficient approach:

```python
import transmog as tm
import json

# Stream process directly to files
tm.flatten_stream(
    file_path="very_large_file.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    low_memory=True  # Enable memory optimization
)
```

## Controlling Chunk Size

Adjust the chunk size to control memory usage:

```python
# Process with smaller chunk size for lower memory usage
tm.flatten_stream(
    file_path="very_large_file.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    chunk_size=50,  # Process 50 records at a time
    low_memory=True
)
```

## Memory-Efficient File Processing

Process large files in chunks:

```python
# Process large file in chunks
result = tm.flatten_file(
    file_path="large_file.json",
    name="records",
    chunk_size=100,  # Process 100 records at a time
    low_memory=True
)
```

## Memory Profiling and Monitoring

Here's how to monitor memory usage:

```python
import psutil
import os
import tracemalloc
import transmog as tm

# Start memory tracking
tracemalloc.start()
process = psutil.Process(os.getpid())
start_memory = process.memory_info().rss / (1024 * 1024)  # MB

# Process a large file with memory optimization
result = tm.flatten_file(
    file_path="large_file.json",
    name="records",
    low_memory=True
)

# Check memory usage
current_memory = process.memory_info().rss / (1024 * 1024)  # MB
memory_increase = current_memory - start_memory
print(f"Memory usage increased by {memory_increase:.2f} MB")

# Get detailed memory statistics
snapshot = tracemalloc.take_snapshot()
top_stats = snapshot.statistics('lineno')
print("Top 10 memory allocations:")
for stat in top_stats[:10]:
    print(stat)
```

## Advanced Memory Optimization Strategies

### 1. Selective Field Processing with Transforms

Process only needed fields by using transforms to filter data:

```python
# Define a transform function to filter fields
def filter_fields(record):
    """Keep only essential fields"""
    return {
        "id": record.get("id"),
        "name": record.get("name"),
        "order_id": record.get("orders", {}).get("id"),
        "order_total": record.get("orders", {}).get("total")
    }

# Process with field filtering
result = tm.flatten(
    data=large_data,
    name="record",
    transforms={"": filter_fields},  # Apply to root level
    low_memory=True
)
```

### 2. Output Format Selection

Choose memory-efficient output formats:

```python
# Process and output directly to Parquet
# (Parquet is more memory-efficient than JSON)
result = tm.flatten_file(
    file_path="large_file.json",
    name="records",
    low_memory=True
)
result.save("output_directory", format="parquet", compression="snappy")
```

### 3. Combining Streaming with Direct Output

For ultimate memory efficiency:

```python
import transmog as tm
import os

# Ensure output directory exists
os.makedirs("output_directory", exist_ok=True)

# Process stream with direct file output and maximum memory efficiency
tm.flatten_stream(
    file_path="very_large_file.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    chunk_size=25,
    low_memory=True,
    compression="snappy"  # Efficient compression for Parquet
)
```

### 4. Processing Line-Delimited JSON

For line-delimited JSON files (one JSON object per line):

```python
import transmog as tm

# Process a line-delimited JSON file
tm.flatten_stream(
    file_path="line_delimited.jsonl",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    line_delimited=True,  # Process one JSON object per line
    low_memory=True
)
```

### 5. Iterative Processing with Result Iterator

Use the result iterator for memory-efficient access to results:

```python
import transmog as tm

# Process data with memory optimization
result = tm.flatten_file(
    file_path="large_file.json",
    name="records",
    low_memory=True
)

# Process tables one at a time without loading all into memory
for table_name, records in result.iter_tables():
    print(f"Processing table {table_name} with {len(records)} records")

    # Process records in batches
    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        # Process batch...
        print(f"  Processed batch {i//batch_size + 1}")
```

## Memory Usage Comparison

Here's a comparison of memory usage for different configurations:

| Configuration | Relative Memory Usage | Processing Speed |
|---------------|------------------------|-----------------|
| Default (`low_memory=False`) | 100% | Fast |
| `low_memory=True` | 60-70% | Medium |
| `low_memory=True` with `chunk_size=100` | 30-50% | Medium-Slow |
| `flatten_stream()` with `low_memory=True` | 10-20% | Slow |

## Best Practices for Memory Optimization

1. **Use `flatten_stream()` for very large datasets**
   - Direct-to-file processing avoids keeping results in memory

2. **Enable `low_memory=True` for all large data processing**
   - Reduces intermediate data caching

3. **Adjust `chunk_size` based on your data**
   - Smaller chunks use less memory but process more slowly
   - Larger chunks use more memory but process faster

4. **Choose efficient output formats**
   - Parquet is more memory-efficient than JSON or CSV
   - Use compression like "snappy" for a good balance of speed and size

5. **Monitor memory usage**
   - Use tools like `tracemalloc` or `memory_profiler` to identify bottlenecks

6. **Pre-filter data when possible**
   - Use transforms to extract only needed fields
   - Filter out unnecessary records before processing

## Next Steps

- Explore [error handling strategies](../../user/advanced/error-handling.md)
- Learn about [streaming large datasets](../intermediate/streaming-large-datasets.md)
- Try [customizing ID generation](../intermediate/customizing-id-generation.md)
