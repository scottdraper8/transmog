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
3. **Conversion Mode**: How and when data is converted to output formats
4. **Caching Behavior**: What intermediate results are kept in memory
5. **Output Management**: How results are stored and returned

## Basic Memory Optimization

Start with the built-in memory-optimized mode:

```python
from transmog import TransmogProcessor, TransmogConfig

# Configure for memory optimization
config = TransmogConfig().memory_optimized()
processor = TransmogProcessor(config)

# Process a file with memory optimization
result = processor.process_file("large_file.json")

# Write results to files
result.write_all_parquet("output_directory")
```

## Understanding Conversion Modes

Transmog offers three conversion modes:

1. **EAGER** - Converts data immediately, caches results (faster, higher memory)
2. **LAZY** - Converts on demand, minimal caching (balanced)
3. **MEMORY_EFFICIENT** - Minimal memory usage, may recompute (slower, lower memory)

```python
from transmog.types import ConversionMode

# Configure with memory-efficient conversion mode
config = TransmogConfig().with_conversion(
    mode=ConversionMode.MEMORY_EFFICIENT
)
processor = TransmogProcessor(config)
```

## Streaming for Memory Efficiency

For very large datasets, streaming is the most memory-efficient approach:

```python
# Configure for streaming with memory optimization
config = TransmogConfig().memory_optimized()
processor = TransmogProcessor(config)

# Function to stream records from file
def stream_records(file_path):
    with open(file_path, 'r') as f:
        # Assuming file contains one JSON object per line
        for line in f:
            yield json.loads(line.strip())

# Process stream with direct file output
streaming_result = processor.process_stream(
    stream_records("very_large_file.json"),
    streaming_output=True
)

# Write directly to files without keeping results in memory
streaming_result.write_streaming_parquet("output_directory")
```

## Controlling Batch Size

Adjust the batch size to control memory usage:

```python
# Configure with smaller batch size for lower memory usage
config = TransmogConfig().memory_optimized().with_processing(
    batch_size=50  # Process 50 records at a time
)
processor = TransmogProcessor(config)

# Process stream with controlled batch size
streaming_result = processor.process_stream(
    stream_records("very_large_file.json"),
    streaming_output=True
)
```

## Disabling Caching

For extreme memory constraints, disable caching entirely:

```python
# Configure with caching disabled
config = TransmogConfig().with_processing(
    enable_caching=False
)
processor = TransmogProcessor(config)
```

## Memory-Efficient File Processing

Process large files in chunks:

```python
# Configure chunked file processing
config = TransmogConfig().memory_optimized().with_processing(
    file_chunk_size=10000  # Read 10,000 bytes at a time
)
processor = TransmogProcessor(config)

# Process large file in chunks
result = processor.process_file("large_file.json")
```

## Memory Profiling and Monitoring

Here's how to monitor memory usage:

```python
import psutil
import os
import tracemalloc

# Start memory tracking
tracemalloc.start()
process = psutil.Process(os.getpid())
start_memory = process.memory_info().rss / (1024 * 1024)  # MB

# Configure for memory optimization
config = TransmogConfig().memory_optimized().with_conversion(
    mode=ConversionMode.MEMORY_EFFICIENT
)
processor = TransmogProcessor(config)

# Process a large file
result = processor.process_file("large_file.json")

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

### 1. Selective Field Processing

Process only needed fields to reduce memory usage:

```python
# Configure to process only specific fields
config = TransmogConfig().memory_optimized().with_flattening(
    include_fields=["id", "name", "orders.id", "orders.total"],
    exclude_fields=["metadata", "description", "tags"]
)
processor = TransmogProcessor(config)
```

### 2. Output Format Selection

Choose memory-efficient output formats:

```python
# Process and output directly to Parquet
# (Parquet is more memory-efficient than JSON)
result = processor.process_file("large_file.json")
result.write_all_parquet("output_directory")
```

### 3. Combining Streaming with Direct Output

For ultimate memory efficiency:

```python
import json
import os

# Configure for maximum memory efficiency
config = TransmogConfig().memory_optimized().with_conversion(
    mode=ConversionMode.MEMORY_EFFICIENT
).with_processing(
    batch_size=25,
    enable_caching=False
)
processor = TransmogProcessor(config)

# Ensure output directory exists
os.makedirs("output_directory", exist_ok=True)

# Process stream with direct file output
def process_in_batches(input_file, output_dir):
    def record_generator():
        with open(input_file, 'r') as f:
            for line in f:
                yield json.loads(line.strip())

    # Process with streaming output
    streaming_result = processor.process_stream(
        record_generator(),
        streaming_output=True
    )

    # Write directly to parquet files
    streaming_result.write_streaming_parquet(output_dir)

    # Return stats
    return {
        "record_count": streaming_result.total_records,
        "error_count": streaming_result.error_count
    }

# Process the file
stats = process_in_batches("very_large_file.json", "output_directory")
print(f"Processed {stats['record_count']} records with {stats['error_count']} errors")
```

## Memory Usage Comparison

Here's a comparison of memory usage for different configurations:

| Configuration | Relative Memory Usage | Processing Speed |
|---------------|------------------------|-----------------|
| Default | 100% | Fast |
| `memory_optimized()` | 60-70% | Medium |
| `ConversionMode.LAZY` | 50-60% | Medium |
| `ConversionMode.MEMORY_EFFICIENT` | 30-50% | Slow |
| Streaming + `MEMORY_EFFICIENT` | 10-20% | Slow |
| Streaming + Direct Output | 5-10% | Slow |

## Best Practices for Memory Optimization

1. **Start Simple**: Begin with `.memory_optimized()` before manual tuning
2. **Measure First**: Profile memory usage to identify bottlenecks
3. **Batch Appropriately**: Find the optimal batch size for your data
4. **Choose Formats Wisely**: Parquet uses less memory than JSON
5. **Stream Large Datasets**: Always use streaming for very large files
6. **Control Field Selection**: Process only the fields you need
7. **Consider Hardware**: Adjust strategies based on available RAM

## Example Implementation

For a complete implementation example, see the performance_optimization.py
file at [GitHub](https://github.com/scottdraper8/transmog/blob/main/examples/data_processing/advanced/performance_optimization.py).

Key aspects demonstrated in the example:

- Comparison of default, memory-optimized, and performance-optimized configurations
- Memory usage tracking with psutil
- Benchmarking of different configurations on various data sizes
- Real-world performance metrics for different optimization strategies
- Custom configuration with balanced memory and performance trade-offs

## Next Steps

- Explore [streaming large datasets](../intermediate/streaming-large-datasets.md)
- Learn about [error recovery strategies](./error-recovery-strategies.md)
- Read about [performance optimization](../../user/advanced/performance-optimization.md) in depth
