# Processing Guide

> **API Reference**: For detailed API documentation, see the
> [Core API Reference](../../api/core.md) and
> [Processing Result API Reference](../../api/processing-result.md).
>
> **Related Guides**:
>
> - [Streaming Guide](../../user/advanced/streaming.md) - For streaming processing techniques
> - [File Processing Guide](file-processing.md) - For file-specific processing options

This guide provides a comprehensive overview of Transmog's processing capabilities, combining core concepts
from various processing approaches and memory optimization techniques.

## Overview of Processing Methods

Transmog v1.1.0 offers several methods for processing data, each optimized for different scenarios:

| Processing Method | Best For | Memory Usage | Performance | API Function |
|-------------------|----------|--------------|-------------|--------------|
| Standard Processing | Medium datasets, balanced approach | Medium | Medium | `tm.flatten()` |
| File Processing | Processing files directly | Medium | Medium | `tm.flatten_file()` |
| Streaming Processing | Very large datasets, direct output | Lowest | Medium | `tm.flatten_stream()` |
| Memory-Optimized Processing | Large datasets | Low | Medium | `tm.flatten(..., low_memory=True)` |

## Decision Tree for Choosing the Right Approach

Use this decision tree to determine which processing approach is best for your scenario:

1. **Are you processing a file?**
   - Yes → Use `tm.flatten_file(path)`
   - No → Continue to next question

2. **Is your dataset small enough to fit in memory?**
   - Yes → Use standard processing with `tm.flatten(data)`
   - No → Continue to next question

3. **Do you need to keep the entire result in memory after processing?**
   - Yes → Use `tm.flatten(data, low_memory=True)`
   - No → Use `tm.flatten_stream(data, output_path)`

## Standard Processing

For datasets that fit comfortably in memory, use the standard `flatten` function:

```python
import transmog as tm

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

result = tm.flatten(data, name="customers")

# Process a list of JSON objects
data_list = [
    {"id": 1, "name": "John Smith"},
    {"id": 2, "name": "Jane Doe"}
]

result = tm.flatten(data_list, name="customers")
```

## File Processing

For processing files directly:

```python
import transmog as tm

# Process a JSON file
result = tm.flatten_file("data.json", name="records")

# Process a CSV file
result = tm.flatten_file("data.csv", name="records")

# Specify format explicitly if needed
result = tm.flatten_file("data.txt", name="records", format="json")
```

## Streaming Processing

For very large datasets that should be processed directly to output files:

```python
import transmog as tm

# Stream process directly to output files
tm.flatten_stream(
    large_data,  # Can be a list, generator, or file path
    output_path="output/",
    name="records",
    format="json",
    batch_size=1000  # Process 1000 records at a time
)

# Stream from a file to output files
tm.flatten_stream(
    "large_data.json",
    output_path="output/",
    name="records",
    format="parquet",
    compression="snappy"  # Format-specific options
)

# Stream from a generator
def record_generator(count):
    for i in range(count):
        yield {"id": i, "value": f"Item {i}"}

tm.flatten_stream(
    record_generator(100000),  # Generator that yields 100,000 records
    output_path="output/",
    name="streamed_data",
    format="csv",
    batch_size=500
)
```

## Memory Optimization Techniques

### Low Memory Mode

For processing larger datasets that still need to be kept in memory:

```python
import transmog as tm

# Process with low memory optimization
result = tm.flatten(
    large_data,
    name="customers",
    low_memory=True,
    batch_size=1000  # Process in batches of 1000
)
```

### Customize Batch Sizes

Adjust batch sizes based on your data characteristics:

```python
# For standard processing with memory optimization
result = tm.flatten(data, name="records", batch_size=500)

# For streaming processing
tm.flatten_stream(data, output_path="output/", name="records", batch_size=1000)
```

### Iterative Processing with Result Objects

Process results iteratively to reduce memory pressure:

```python
# Process data
result = tm.flatten(large_data, name="customers")

# Iterate through main table records one at a time
for record in result:  # FlattenResult is iterable
    process_record(record)

# Iterate through all tables
for table_name, records in result.items():
    for record in records:
        process_record(record, table_name)
```

### Stream Directly to Output

For very large datasets, stream directly to output formats:

```python
# Stream process to output format
tm.flatten_stream(
    large_data_source,
    output_path="output/",
    name="records",
    format="parquet",
    compression="snappy"
)
```

## Managing Memory for Very Large Datasets

When working with extremely large datasets:

1. **Use Streaming Processing**: Process data directly to output files
2. **Use Low Memory Mode**: Enable memory optimization with `low_memory=True`
3. **Release Processed Results**: Set processed results to `None` after use
4. **Free Unused Resources**: Call `gc.collect()` after processing large batches
5. **Monitor Memory Usage**: Use tools like `psutil` to monitor memory consumption

```python
import gc
import psutil
import transmog as tm

def print_memory_usage():
    process = psutil.Process()
    print(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

# Process large dataset with memory monitoring
print_memory_usage()  # Before processing

# Stream directly to output (lowest memory usage)
tm.flatten_stream(
    large_data,
    output_path="output/",
    name="records",
    format="json",
    batch_size=1000
)

print_memory_usage()  # After processing

# Or process with low memory mode if you need the result in memory
result = tm.flatten(
    large_data,
    name="records",
    low_memory=True,
    batch_size=1000
)

# Use the result


# Free memory
del result
gc.collect()

print_memory_usage()  # After processing and cleanup
```

## Converting Results to Different Formats

The `FlattenResult` class provides convenient methods for converting to different formats:

```python
# Process data
result = tm.flatten(data, name="customers")

# Save to different formats
result.save("output.json")  # Save as JSON
result.save("output.csv")   # Save as CSV
result.save("output.parquet")  # Save as Parquet



# Access tables directly
main_table = result.main  # Get main table
child_tables = result.tables  # Get all child tables
```

## Advanced Processing (Internal API)

For advanced use cases, Transmog still provides access to the full processing API through the internal interface:

```python
from transmog.process import Processor
from transmog.config import TransmogConfig

# Create a custom configuration
config = (
    TransmogConfig.default()
    .with_naming(separator=".")
    .with_processing(batch_size=1000)
    .with_error_handling(recovery_strategy="skip")
)

# Create a processor with custom configuration
processor = Processor(config=config)

# Use advanced processing methods
result = processor.process(data, entity_name="records")
```

## Processing Strategy Selection

Internally, Transmog uses the Strategy pattern to handle different data processing scenarios. The library automatically
selects the appropriate strategy based on the method called and the input data type:

| Input Type | Strategy Used |
|------------|--------------|
| Dict/List in memory | InMemoryStrategy |
| File path | FileStrategy |
| Generator | ChunkedStrategy |
| CSV file | CSVStrategy |
