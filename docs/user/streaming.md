# Streaming Data Processing

Transmog provides powerful streaming capabilities for processing large datasets efficiently with minimal memory usage. This guide explains how to use Transmog's streaming features to process data from various sources.

## Overview

Streaming data processing allows you to:

- Process datasets larger than available memory
- Maintain a low and predictable memory footprint
- Process data from various sources (files, iterators, generators)
- Output directly to different formats without storing intermediate results
- Handle real-time data streams efficiently

## Streaming API

Transmog's streaming API provides direct processing from input to output:

```python
import transmog as tm

processor = tm.Processor()

# Process data directly to CSV files
processor.stream_process(
    data=large_data_object,
    entity_name="records",
    output_format="csv",
    output_destination="output_dir/csv"
)

# Process a file directly to JSON files
processor.stream_process_file(
    file_path="large_data.json",
    entity_name="records",
    output_format="json",
    output_destination="output_dir/json"
)

# Process a CSV file directly to Parquet files
processor.stream_process_csv(
    file_path="data.csv",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet"
)
```

## Processing Strategies

Transmog uses different processing strategies based on input type and configuration:

```python
from transmog import Processor, ProcessingStrategy

# Different strategies are used internally based on the input
processor = Processor()

# InMemoryStrategy - For processing data in memory
result = processor.process(data)

# FileStrategy - For processing data from files
result = processor.process_file("data.json", entity_name="records")

# BatchStrategy - For processing data in batches
result = processor.process_batch(batch_data, entity_name="records")

# ChunkedStrategy - For processing large datasets in chunks
result = processor.process_chunked(
    "large_data.jsonl", 
    entity_name="records", 
    chunk_size=1000
)

# CSVStrategy - For processing CSV data
result = processor.process_csv("data.csv", entity_name="records")
```

## Processing Modes

Transmog supports different processing modes that determine the memory/performance tradeoff:

```python
from transmog import Processor, TransmogConfig, ProcessingMode

# Standard mode (default) - balances memory and performance
processor = Processor(
    config=TransmogConfig.default()
    .with_processing(processing_mode=ProcessingMode.STANDARD)
)

# Low memory mode - minimizes memory usage for very large datasets
processor = Processor(
    config=TransmogConfig.default()
    .with_processing(processing_mode=ProcessingMode.LOW_MEMORY)
)

# High performance mode - emphasizes speed for smaller datasets
processor = Processor(
    config=TransmogConfig.default()
    .with_processing(processing_mode=ProcessingMode.HIGH_PERFORMANCE)
)

# Simplified factory methods
processor = Processor.memory_optimized()
processor = Processor.performance_optimized()
```

## Streaming Writers

Transmog utilizes streaming writers to process data directly to output formats:

```python
from transmog.io import create_streaming_writer, get_supported_streaming_formats

# Check available streaming formats
formats = get_supported_streaming_formats()
print(f"Supported streaming formats: {formats}")

# Create a streaming writer for a specific format
writer = create_streaming_writer(
    format_name="csv",
    output_path="output_dir",
    options={"delimiter": ",", "include_header": True}
)

# Write data using the streaming writer
writer.write_table("main", main_records)
writer.write_table("child_items", child_records)
writer.close()
```

## Streaming from Files

### JSON and JSONL Files

Process JSON and JSONL (JSON Lines) files with streaming:

```python
from transmog import Processor

processor = Processor()

# Process a JSON file with streaming
processor.stream_process_file(
    file_path="large_data.json",
    entity_name="records",
    output_format="csv",
    output_destination="output_dir"
)

# Process a JSONL file with streaming
processor.stream_process_file_with_format(
    file_path="large_data.jsonl",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir",
    format_type="jsonl"
)
```

### CSV Files

Process CSV files with streaming:

```python
processor.stream_process_csv(
    file_path="data.csv",
    entity_name="records",
    output_format="json",
    output_destination="output_dir",
    delimiter=",",
    has_header=True,
    infer_types=True,
    sanitize_column_names=True
)
```

## Streaming to Memory or File Objects

You can stream directly to file objects or memory buffers:

```python
import io

# Create a memory buffer
buffer = io.StringIO()  # For text formats like CSV/JSON
# or
buffer = io.BytesIO()   # For binary formats like Parquet

# Stream directly to the buffer
processor.stream_process(
    data=large_data,
    entity_name="records",
    output_format="json",
    output_destination=buffer,
    indent=2  # Format-specific option
)

# Get the data from the buffer
buffer.seek(0)
output_data = buffer.read()
```

## Memory-Efficient Data Iterators

Transmog provides data iterators for efficient processing:

```python
from transmog.process.data_iterators import (
    get_json_file_iterator,
    get_jsonl_file_iterator,
    get_csv_file_iterator
)

# Create an iterator for a JSON file
for record in get_json_file_iterator("data.json"):
    # Process each record
    process_record(record)

# Create an iterator for a JSONL file
for record in get_jsonl_file_iterator("data.jsonl"):
    # Process each record
    process_record(record)

# Create an iterator for a CSV file
for record in get_csv_file_iterator(
    "data.csv", 
    has_header=True, 
    delimiter=",",
    infer_types=True
):
    # Process each record
    process_record(record)
```

## Python Iterators and Generators

Process data directly from any iterator or generator:

```python
def record_generator():
    for i in range(10000):
        yield {"id": i, "name": f"Record {i}"}

# Process the generator with chunking
result = processor.process_chunked(
    record_generator(),
    entity_name="generated_data",
    chunk_size=100
)

# Or stream directly to output
processor.stream_process(
    record_generator(),
    entity_name="generated_data",
    output_format="csv",
    output_destination="output_dir"
)
```

## Memory Optimization for Processing Results

The ProcessingResult class offers memory optimization options:

```python
from transmog import Processor, ConversionMode

processor = Processor()

# Standard processing (keeps data in memory)
result = processor.process(data, entity_name="records")

# Convert to memory-efficient mode
efficient_result = result.with_conversion_mode(ConversionMode.MEMORY_EFFICIENT)

# Write to output - will clear intermediate data after writing
efficient_result.write_all_csv("output_dir")
```

## Memory-Optimized Bytes Output

For memory-efficient handling of large outputs:

```python
# Get bytes without storing intermediate data
json_bytes = result.to_json_bytes(conversion_mode=ConversionMode.MEMORY_EFFICIENT)
csv_bytes = result.to_csv_bytes(conversion_mode=ConversionMode.MEMORY_EFFICIENT)
parquet_bytes = result.to_parquet_bytes(conversion_mode=ConversionMode.MEMORY_EFFICIENT)

# Write directly to files
with open("output.json", "wb") as f:
    f.write(json_bytes["main"])
```

## Best Practices for Memory-Efficient Processing

1. **Use streaming API for large datasets**
   ```python
   processor.stream_process_file(
       file_path="large_data.json",
       entity_name="records",
       output_format="csv",
       output_destination="output_dir"
   )
   ```

2. **Process in chunks for better control**
   ```python
   result = processor.process_chunked(
       "large_data.jsonl",
       entity_name="records",
       chunk_size=1000  # Adjust based on record size
   )
   ```

3. **Use memory-optimized configuration**
   ```python
   processor = Processor.memory_optimized()
   ```

4. **Stream directly to the desired output format**
   ```python
   processor.stream_process_file_with_format(
       file_path="input.json",
       entity_name="records",
       output_format="parquet",
       output_destination="output_dir"
   )
   ```

5. **Use memory-efficient conversion mode**
   ```python
   result.write_all_csv(
       "output_dir", 
       conversion_mode=ConversionMode.MEMORY_EFFICIENT
   )
   ```

6. **Consider batched processing for consistent memory usage**
   ```python
   for i, batch in enumerate(get_data_batches(source, batch_size=1000)):
       result = processor.process_batch(batch, entity_name="records")
       result.write_all_csv(f"output_dir/batch_{i}")
   ```

## Performance vs. Memory Tradeoffs

| Approach | Memory Usage | Performance | Best For |
|----------|--------------|-------------|----------|
| `process()` | High | Fastest | Small-medium datasets |
| `process_chunked()` | Medium | Fast | Medium-large datasets |
| `stream_process()` | Lowest | Slower | Very large datasets |
| Memory-optimized | Low | Medium | Large datasets with memory constraints |
| Performance-optimized | High | Fastest | Speed-critical applications |

## Automatic Mode Detection

Transmog can automatically detect the best processing mode based on data size:

```python
from transmog import Processor

processor = Processor()

# Direct processing with automatic mode detection
result = processor.process_to_format(
    data="large_file.json",  # Will detect file size
    entity_name="records",
    output_format="csv",
    output_path="output_dir",
    auto_detect_mode=True  # Enable automatic mode detection
)
```

For large datasets, Transmog will automatically switch to streaming mode without storing the entire dataset in memory.

## Direct Output Format Specification

You can specify the output format directly when processing data:

```python
from transmog import Processor

processor = Processor()

# Process directly to a specific format
result = processor.process_to_format(
    data=my_data,
    entity_name="records",
    output_format="json",
    output_path="output_dir"
)

# Process a file directly to a specific format
result = processor.process_file_to_format(
    file_path="input.json",
    entity_name="records",
    output_format="csv",
    output_path="output_dir"
)
```

## Memory Optimization

### Customizing Memory Thresholds

You can customize the memory threshold for automatic mode switching:

```python
from transmog import Processor

# Set a custom memory threshold (in bytes)
processor = Processor(memory_threshold=50 * 1024 * 1024)  # 50MB threshold
```

## In-Place Transformations

For better memory efficiency, you can use in-place transformations when flattening data:

```python
from transmog.core.flattener import flatten_json

# Standard flattening (creates new objects)
flattened = flatten_json(data)

# In-place flattening (modifies original object)
flattened = flatten_json(data, in_place=True)
```

## Memory Usage Monitoring

You can enable memory usage tracking to help optimize your processing:

```python
from transmog import Processor

# Enable memory tracking
processor = Processor(memory_tracking_enabled=True)

# Process data
result = processor.process(data, entity_name="records")

# Memory usage information will be logged during processing
```

## Input Format Detection

Transmog can automatically detect input formats:

```python
# Automatic format detection
result = processor.process_chunked(data_source, entity_name="auto_detected")

# Explicit format specification
result = processor.process_chunked(
    data_source,
    entity_name="explicit_format",
    input_format="jsonl",  # "json", "jsonl", "csv", or "dict"
    **format_options       # Format-specific options
)
```

## Unified Data Iterator Interface

Under the hood, Transmog uses a unified data iterator interface that handles different data sources consistently:

```python
# Get an iterator for any data source
data_iterator = processor._get_data_iterator(
    data_source,
    input_format="auto"  # "auto", "json", "jsonl", "csv", or "dict"
)

# Process the iterator in chunks
result = processor._process_in_chunks(
    data_iterator,
    entity_name="streamed_data",
    chunk_size=1000
)
```

## Combining Results

When processing data in chunks, results are automatically combined:

```python
# Results from multiple chunks are automatically combined
result = processor.process_chunked(large_data, entity_name="combined")

# You can also manually combine results
from transmog import ProcessingResult

result1 = processor.process_batch(batch1, entity_name="entity")
result2 = processor.process_batch(batch2, entity_name="entity")

combined = ProcessingResult.combine_results([result1, result2], entity_name="entity")
```

## Performance Considerations

- **Chunk Size**: Larger chunks reduce overhead but increase memory usage
- **Format**: JSONL is more memory-efficient than JSON for large datasets
- **Processing Mode**: Use LOW_MEMORY mode for very large datasets
- **Dependencies**: Install optional dependencies like `orjson` for better performance

## Examples

### Process a Large JSONL File

```python
from transmog import Processor

processor = Processor(optimize_for_memory=True)
result = processor.process_file(
    "very_large_data.jsonl",
    entity_name="large_dataset"
)

# Write results directly to files without keeping everything in memory
result.write_all_parquet("output/parquet")
```

### Process Data from a Generator with Chunking

```python
def generate_records(count):
    for i in range(count):
        yield {
            "id": i,
            "data": {
                "value": i * 10,
                "items": [{"item_id": j, "name": f"Item {j}"} for j in range(3)]
            }
        }

processor = Processor()
result = processor.process_chunked(
    generate_records(1000000),  # Generate a million records
    entity_name="generated",
    chunk_size=10000
)
```

### Process a Stream of JSON Objects

```python
import json
from transmog import Processor

# Create a processor with memory optimization
processor = Processor(optimize_for_memory=True)

# Open a file for streaming JSON objects
with open("data.jsonl", "r") as f:
    # Process the stream
    result = processor.process_chunked(
        f,
        entity_name="stream",
        input_format="jsonl",
        chunk_size=1000
    )
    
    # Write the results
    result.write_all_parquet("output/parquet")
``` 