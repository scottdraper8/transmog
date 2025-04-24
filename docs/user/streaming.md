# Streaming Data Processing

Transmogrify provides powerful streaming capabilities for processing large datasets efficiently with minimal memory usage. This guide explains how to use Transmogrify's streaming features to process data from various sources.

## Overview

Streaming data processing allows you to:

- Process datasets larger than available memory
- Maintain a low and predictable memory footprint
- Process data from various sources (files, iterators, generators)
- Handle real-time data streams efficiently

## Processing Modes

Transmogrify supports different processing modes that determine the memory/performance tradeoff:

```python
from transmogrify import Processor, ProcessingMode

# Standard mode (default) - balances memory and performance
processor = Processor()

# Low memory mode - minimizes memory usage for very large datasets
processor = Processor(optimize_for_memory=True)

# High performance mode - emphasizes speed for smaller datasets
processor = Processor()
result = processor._process_data(
    data, 
    entity_name="entity",
    memory_mode=ProcessingMode.HIGH_PERFORMANCE
)
```

## Streaming from Files

### JSONL Files

[JSONL (JSON Lines)](https://jsonlines.org/) files contain one JSON object per line, making them ideal for streaming:

```python
from transmogrify import Processor

processor = Processor()
result = processor.process_file("data.jsonl", entity_name="records")
```

For large JSONL files, you can specify a chunk size to control memory usage:

```python
result = processor.process_chunked(
    "large_data.jsonl", 
    entity_name="records",
    chunk_size=1000  # Process 1000 records at a time
)
```

### CSV Files

CSV files are naturally processed as streams:

```python
result = processor.process_csv(
    "data.csv",
    entity_name="records",
    delimiter=",",
    has_header=True
)
```

## Streaming from Other Sources

### Python Iterators and Generators

You can process data directly from any iterator or generator:

```python
def record_generator():
    for i in range(10000):
        yield {"id": i, "name": f"Record {i}"}

# Process the generator directly
result = processor.process_chunked(
    record_generator(),
    entity_name="generated_data",
    chunk_size=100
)
```

### File Objects

Process data directly from file objects:

```python
with open("data.jsonl", "r") as f:
    result = processor.process_chunked(f, entity_name="file_data")
```

### API Response Streams

Process data from API responses:

```python
import requests

# Get a streaming response from an API
response = requests.get("https://api.example.com/data", stream=True)

# Process the data stream
result = processor.process_chunked(
    response.iter_lines(),
    entity_name="api_data",
    input_format="jsonl"  # Specify the format if known
)
```

## Memory Optimization

To optimize memory usage during streaming:

1. Use the `process_chunked` method with an appropriate `chunk_size`
2. Set `optimize_for_memory=True` when creating the processor
3. Process directly from file sources rather than loading into memory first
4. Use the JSONL format for large files instead of a single large JSON array

## Input Format Detection

Transmogrify can automatically detect input formats:

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

Under the hood, Transmogrify uses a unified data iterator interface that handles different data sources consistently:

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
from transmogrify import ProcessingResult

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
from transmogrify import Processor

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
from transmogrify import Processor

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