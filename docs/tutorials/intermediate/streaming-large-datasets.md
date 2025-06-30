# Streaming Large Datasets

This tutorial demonstrates how to efficiently process and transform large datasets using Transmog's streaming
capabilities.

## When to Use Streaming

Streaming is ideal for:

- Processing datasets too large to fit in memory
- Handling continuous data flows
- Improving memory efficiency
- Reducing processing latency

## Prerequisites

- Basic understanding of Transmog concepts
- Completed the [Transform Nested JSON](../basic/transform-nested-json.md) tutorial

## Setting Up Streaming Processing

First, import the necessary components:

```python
import transmog as tm
import json
```

## Simple Streaming Example

Here's a basic example of streaming a large JSON file:

```python
# Example function to stream records from a file
def stream_records_from_file(file_path):
    with open(file_path, 'r') as file:
        # Assuming file contains one JSON object per line
        for line in file:
            yield json.loads(line.strip())

# Process data in streaming mode
file_path = 'large_dataset.json'

# Stream process directly to output files
tm.flatten_stream(
    file_path=file_path,
    name="records",
    output_path="output_directory",
    output_format="json",
    low_memory=True  # Enable memory optimization
)
```

## Direct File Streaming

Files can be processed directly without creating a generator:

```python
# Stream process a JSON file directly
tm.flatten_stream(
    file_path="large_dataset.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    compression="snappy"  # Use Snappy compression for Parquet files
)
```

## Memory Management with Chunk Size

Control memory usage by adjusting the chunk size:

```python
# Stream with specific chunk size
tm.flatten_stream(
    file_path="large_dataset.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    chunk_size=100,  # Process 100 records at a time
    low_memory=True  # Enable additional memory optimizations
)
```

## Processing Very Large CSV Files

Here's how to stream and process a large CSV file:

```python
import transmog as tm

# Stream process a large CSV file
tm.flatten_stream(
    file_path="large_file.csv",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    has_header=True,      # CSV has a header row
    delimiter=",",        # CSV delimiter
    null_values=["NA", ""], # Values to treat as null
    low_memory=True       # Enable memory optimization
)
```

## Parallel Processing with Streaming

For even larger datasets, streaming can be combined with parallel processing:

```python
from concurrent.futures import ThreadPoolExecutor
import os

# Function to process a chunk of the file
def process_chunk(chunk_file):
    # Process the chunk
    output_path = f"output/{os.path.basename(chunk_file).split('.')[0]}"
    
    tm.flatten_stream(
        file_path=chunk_file,
        name="records",
        output_path=output_path,
        output_format="parquet",
        low_memory=True
    )
    
    return f"Processed {chunk_file}"

# List of chunk files (previously split large file)
chunk_files = [f"chunks/chunk_{i}.json" for i in range(10)]

# Process chunks in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(process_chunk, chunk_file) for chunk_file in chunk_files]
    for future in concurrent.futures.as_completed(futures):
        print(future.result())
```

## Handling Errors in Streaming Mode

Configure error handling for streaming:

```python
# Stream process with error handling
tm.flatten_stream(
    file_path="data_with_errors.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    error_handling="skip",  # Skip records with errors
    error_log="errors.log"  # Log errors to a file
)
```

## In-memory Streaming with Data Source

Data sources that generate records can be used with streaming:

```python
# Create a generator function
def generate_records():
    for i in range(1000000):  # Generate a million records
        yield {
            "id": i,
            "name": f"Record {i}",
            "value": i * 10
        }

# Stream the generated data directly to output
tm.flatten(
    data=generate_records(),  # Pass the generator
    name="records",
    stream=True,              # Enable streaming
    output_path="output_directory",
    output_format="parquet"
)
```

## Format-Specific Streaming Options

The output format can be customized with specific options:

```python
# Stream to CSV with specific options
tm.flatten_stream(
    file_path="large_dataset.json",
    name="records",
    output_path="output_directory",
    output_format="csv",
    include_header=True,     # Include column headers
    quotechar='"',           # Character for quoting fields
    encoding="utf-8"         # Output file encoding
)

# Stream to Parquet with specific options
tm.flatten_stream(
    file_path="large_dataset.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    compression="zstd",      # Use zstd compression for better ratio
    row_group_size=10000     # Number of rows per row group
)
```

## Performance Considerations

When streaming large datasets:

1. **Chunk Size**: Adjust based on your memory constraints (smaller for less memory, larger for better performance)
2. **Output Format**: Parquet is generally more efficient than JSON or CSV
3. **Compression**: Use "snappy" for speed or "zstd" for better compression ratio
4. **Error Handling**: Use "skip" for production systems to continue despite errors
5. **Low Memory Mode**: Enable `low_memory=True` for very large datasets

## Next Steps

- Learn about [optimizing memory usage](../../user/advanced/performance-optimization.md)
- Try [customizing ID generation](./customizing-id-generation.md)
- Explore [error handling strategies](../../user/advanced/error-handling.md)
