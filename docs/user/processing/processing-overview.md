# Processing Guide

> **API Reference**: For detailed API documentation, see the
> [Processor API Reference](../../api/processor.md) and
> [Process API Reference](../../api/process.md).
>
> **Related Guides**:
>
> - [Streaming Guide](../../user/advanced/streaming.md) - For streaming processing techniques
> - [File Processing Guide](file-processing.md) - For file-specific processing options

This guide provides a comprehensive overview of Transmog's processing capabilities, combining core concepts
from various processing approaches and memory optimization techniques.

## Overview of Processing Methods

Transmog offers several methods for processing data, each optimized for different scenarios:

| Processing Method | Best For | Memory Usage | Performance |
|-------------------|----------|--------------|-------------|
| Standard Processing | Medium datasets, balanced approach | Medium | Medium |
| Batch Processing | Collections of similar records | Medium | High |
| Chunked Processing | Large datasets | Low | Medium |
| Streaming Processing | Very large datasets, direct output | Lowest | Medium |
| In-Memory Processing | Small datasets, fast processing | High | Highest |

## Decision Tree for Choosing the Right Approach

Use this decision tree to determine which processing approach is best for your scenario:

1. **Is your dataset small enough to fit in memory?**
   - Yes → Use standard in-memory processing
   - No → Continue to next question

2. **Do you need to keep the entire result in memory after processing?**
   - Yes → Use chunked processing
   - No → Continue to next question

3. **Do you need to write directly to an output format?**
   - Yes → Use streaming processing
   - No → Use chunked processing with a memory-optimized configuration

## Standard Processing

For datasets that fit comfortably in memory, use the standard `process` method:

```python
from transmog import Processor

processor = Processor()

# Process a single JSON object
data = {
    "id": 1,
    "name": "John Smith",
    "age": 30,
    "address": {
        "street": "123 Main St",
        "city": "Boston"
    }
}

result = processor.process(data, entity_name="customers")

# Process a list of JSON objects
data_list = [
    {"id": 1, "name": "John Smith"},
    {"id": 2, "name": "Jane Doe"}
]

result = processor.process(data_list, entity_name="customers")
```

## Batch Processing

For processing collections of similar records:

```python
from transmog import Processor

processor = Processor()

# Process a batch of records
batch_data = [
    {"id": 1, "name": "Record 1"},
    {"id": 2, "name": "Record 2"},
    {"id": 3, "name": "Record 3"}
]

result = processor.process_batch(
    batch_data=batch_data,
    entity_name="records"
)
```

## Chunked Processing

For large datasets that may not fit entirely in memory:

```python
from transmog import Processor

processor = Processor()

# Process a large list of records in chunks
large_list = [{"id": i, "data": f"data_{i}"} for i in range(10000)]

result = processor.process_chunked(
    large_list,
    entity_name="large_dataset",
    chunk_size=1000  # Process 1000 records at a time
)

# Process a generator of records
def record_generator(count):
    for i in range(count):
        yield {"id": i, "value": f"Item {i}"}

result = processor.process_chunked(
    record_generator(100000),  # Generator that yields 100,000 records
    entity_name="streamed_data",
    chunk_size=500
)
```

## Memory Optimization Techniques

### Memory Optimization Modes

Transmog supports different processing modes to optimize memory usage and performance:

```python
from transmog import Processor

# Create a processor optimized for memory usage
processor = Processor.memory_optimized()

# Create a processor optimized for performance
processor = Processor.performance_optimized()

# Process data with the optimized processor
result = processor.process(data, entity_name="customers")
```

### Customize Batch and Chunk Sizes

Adjust batch and chunk sizes based on your data characteristics:

```python
# For batch processing
processor.with_processing(batch_size=1000)

# For chunked processing
result = processor.process_chunked(data, entity_name="records", chunk_size=500)
```

### Use Lazy Conversion Mode

Use the lazy conversion mode to delay data conversions until needed:

```python
from transmog import ConversionMode

# Process data
result = processor.process(data, entity_name="customers")

# Use lazy conversion mode for output
tables = result.to_dict(conversion_mode=ConversionMode.LAZY)
```

### Stream Directly to Output

For very large datasets, stream directly to output formats:

```python
# Stream process to output format
processor.stream_process(
    data=large_data_source,
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir"
)
```

## Managing Memory for Very Large Datasets

When working with extremely large datasets:

1. **Use Chunked Processing**: Process data in manageable chunks
2. **Release Processed Results**: Set processed results to `None` after use
3. **Free Unused Resources**: Call `gc.collect()` after processing large batches
4. **Monitor Memory Usage**: Use tools like `psutil` to monitor memory consumption

```python
import gc
import psutil
from transmog import Processor

processor = Processor.memory_optimized()

def print_memory_usage():
    process = psutil.Process()
    print(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

# Process large dataset in chunks with memory monitoring
print_memory_usage()  # Before processing

for i, chunk in enumerate(data_chunks):
    # Process chunk
    result = processor.process_batch(chunk, entity_name="records")

    # Use the result
    output = result.to_dict()

    # Free memory
    del result
    gc.collect()

    print(f"Chunk {i} completed")
    print_memory_usage()  # After processing chunk
```

## Combining Multiple Processing Results

For scenarios where you process data in parts:

```python
from transmog import ProcessingResult

# Process multiple batches
results = []
for batch in data_batches:
    result = processor.process_batch(batch, entity_name="records")
    results.append(result)

# Combine all results
combined_result = ProcessingResult.combine_results(results)

# Access the combined data
all_records = combined_result.get_main_table()
```

## Processing Strategy Selection

Transmog uses the Strategy pattern to handle different data processing scenarios. The library automatically
selects the appropriate strategy based on the method called and the input data type:

| Method | Strategy Used |
|--------|--------------|
| `process()` | InMemoryStrategy for dict/list, FileStrategy for str paths |
| `process_file()` | FileStrategy |
| `process_batch()` | BatchStrategy |
| `process_chunked()` | ChunkedStrategy |
| `process_csv()` | CSVStrategy |

## Conclusion

Choosing the right processing approach in Transmog depends on your dataset size, memory constraints,
and processing needs. By following the guidelines in this document, you can optimize your data processing
workflow for both performance and memory efficiency.
