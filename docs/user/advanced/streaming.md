---
title: Streaming
---

> For API details, see [Core API](../../api/core.md) and [IO API](../../api/io.md).

# Streaming

## Introduction

Streaming processing enables the handling of large datasets that exceed available memory, or the continuous
processing of data as it arrives.

**Related Guides:**

- [Processing Overview](../processing/processing-overview.md)
- [File Processing](../processing/file-processing.md)

Transmog implements streaming processing capabilities for handling large datasets with efficient memory usage.
This document describes these capabilities and their appropriate applications.

## Streaming Processing Applications

Streaming processing is applicable for:

- Processing large datasets that exceed available memory
- Reducing memory usage during processing
- Processing data incrementally
- Direct output to files without intermediate memory structures
- Converting data to analytical formats like Parquet

## Available Streaming Functions

The following functions are available for streaming data processing:

| Function | Description | Applicable For |
|----------|-------------|----------------|
| `flatten_stream` | Stream any data source to output | Files, in-memory data |
| `flatten` with `stream=True` | Process and stream directly to output | In-memory data with direct output |

## Basic Streaming Usage

### flatten_stream

The primary function for streaming processing is `flatten_stream`:

```python
import transmog as tm

# Stream a large JSON file directly to Parquet format
tm.flatten_stream(
    file_path="large_data.json",
    name="records",
    output_path="output_dir/parquet",
    output_format="parquet",
    compression="snappy"  # Format-specific option
)

# Stream a large CSV file directly to JSON format
tm.flatten_stream(
    file_path="large_data.csv",
    name="records",
    output_path="output_dir/json",
    output_format="json",
    delimiter=",",  # CSV-specific option
    has_header=True,  # CSV-specific option
    indent=2  # JSON-specific option
)

# Stream a JSONL file with memory optimization
tm.flatten_stream(
    file_path="large_data.jsonl",
    name="records",
    output_path="output_dir/csv",
    output_format="csv",
    chunk_size=10000,  # Process 10,000 records at a time
    low_memory=True,   # Optimize for minimal memory usage
    include_header=True  # CSV-specific option
)
```

### flatten with stream=True

For in-memory data that you want to stream directly to output:

```python
import transmog as tm

# Generate or load a large dataset
large_data = [{"id": i, "name": f"Record {i}"} for i in range(1, 1000001)]

# Stream in-memory data directly to output files
tm.flatten(
    data=large_data,
    name="records",
    stream=True,  # Enable streaming
    output_path="output_dir/parquet",
    output_format="parquet",
    compression="snappy"
)
```

## Format-Specific Streaming Options

### Streaming to CSV

```python
# Stream to CSV with specific options
tm.flatten_stream(
    file_path="large_data.json",
    name="records",
    output_path="output_dir/csv",
    output_format="csv",
    include_header=True,      # Include column headers
    delimiter=",",            # Column delimiter
    quotechar='"',            # Character for quoting fields
    sanitize_header=True,     # Clean up column names
    encoding="utf-8"          # Output file encoding
)
```

### Streaming to Parquet

```python
# Stream to Parquet with specific options
tm.flatten_stream(
    file_path="large_data.json",
    name="records",
    output_path="output_dir/parquet",
    output_format="parquet",
    compression="snappy",     # Compression algorithm (snappy, zstd, gzip, etc.)
    row_group_size=10000,     # Number of rows per row group
    write_statistics=True     # Include column statistics
)
```

### Streaming to JSON

```python
# Stream to JSON with specific options
tm.flatten_stream(
    file_path="large_data.csv",
    name="records",
    output_path="output_dir/json",
    output_format="json",
    indent=2,                # Pretty-print with indentation
    ensure_ascii=False,      # Allow non-ASCII characters
    sort_keys=False          # Don't sort keys alphabetically
)
```

## Memory Optimization

The `low_memory` parameter can be used to further optimize memory usage:

```python
# Stream with maximum memory optimization
tm.flatten_stream(
    file_path="very_large_data.jsonl",
    name="records",
    output_path="output_dir/parquet",
    output_format="parquet",
    chunk_size=5000,        # Smaller chunks for lower memory usage
    low_memory=True,        # Enable additional memory optimizations
    compression="zstd"      # Efficient compression algorithm
)
```

When `low_memory=True` is specified:

1. Intermediate data structures are minimized
2. Garbage collection is more aggressive
3. Batch sizes are optimized for memory usage
4. Reference counting is optimized

## Chunked Processing

Control the chunk size for streaming processing:

```python
# Stream with specific chunk size
tm.flatten_stream(
    file_path="large_data.json",
    name="records",
    output_path="output_dir/csv",
    output_format="csv",
    chunk_size=10000  # Process 10,000 records at a time
)
```

The `chunk_size` parameter controls:

1. How many records are processed at once
2. Memory usage during processing
3. Size of individual write operations

## Streaming with Custom ID Fields

You can use natural IDs or custom ID generation with streaming:

```python
# Stream with natural ID field
tm.flatten_stream(
    file_path="users.json",
    name="users",
    output_path="output_dir/parquet",
    output_format="parquet",
    id_field="user_id"  # Use user_id field as the ID
)

# Stream with different ID fields for different tables
tm.flatten_stream(
    file_path="orders.json",
    name="orders",
    output_path="output_dir/parquet",
    output_format="parquet",
    id_field={
        "": "order_id",                # Main table uses order_id field
        "orders_items": "item_id"      # Items table uses item_id field
    }
)
```

## Streaming with Error Handling

Control how errors are handled during streaming:

```python
# Stream with error handling
tm.flatten_stream(
    file_path="data_with_errors.json",
    name="records",
    output_path="output_dir/parquet",
    output_format="parquet",
    error_handling="skip",  # Skip records with errors
    error_log="errors.log"  # Log errors to a file
)
```

Available error handling options:

- `"raise"`: Raise an exception on the first error (default)
- `"skip"`: Skip records with errors and continue processing
- `"warn"`: Log a warning for errors but continue processing

## Advanced Streaming Use Cases

### Converting Between Formats

Streaming is ideal for converting between formats:

```python
# Convert CSV to Parquet
tm.flatten_stream(
    file_path="large_data.csv",
    name="data",
    output_path="output_dir/parquet",
    output_format="parquet",
    compression="zstd"
)

# Convert JSON to CSV
tm.flatten_stream(
    file_path="large_data.json",
    name="data",
    output_path="output_dir/csv",
    output_format="csv"
)
```

### Processing Multiple Files

Process multiple files by calling `flatten_stream` for each file:

```python
import os

# Process all JSON files in a directory
json_files = [f for f in os.listdir("data_dir") if f.endswith(".json")]

for file_name in json_files:
    input_path = os.path.join("data_dir", file_name)
    output_name = os.path.splitext(file_name)[0]
    
    tm.flatten_stream(
        file_path=input_path,
        name=output_name,
        output_path=f"output_dir/{output_name}",
        output_format="parquet"
    )
```

### Streaming with Data Transformation

Apply transformations during streaming:

```python
# Define a transformation function
def transform_price(price):
    return float(price) * 1.1  # Increase price by 10%

# Stream with transformation
tm.flatten_stream(
    file_path="products.json",
    name="products",
    output_path="output_dir/parquet",
    output_format="parquet",
    transforms={
        "price": transform_price  # Apply transformation to price field
    }
)
```

## Performance Considerations

### Optimizing Streaming Performance

1. **Chunk Size**: Adjust the `chunk_size` parameter to balance memory usage and performance
   ```python
   # Larger chunks for better performance (if memory allows)
   tm.flatten_stream(file_path="data.json", name="data", chunk_size=50000, ...)
   
   # Smaller chunks for lower memory usage
   tm.flatten_stream(file_path="data.json", name="data", chunk_size=1000, ...)
   ```

2. **Compression**: Choose the appropriate compression algorithm
   ```python
   # Snappy for better speed
   tm.flatten_stream(..., compression="snappy")
   
   # Zstd for better compression ratio
   tm.flatten_stream(..., compression="zstd")
   ```

3. **Low Memory Mode**: Enable for very large datasets
   ```python
   tm.flatten_stream(..., low_memory=True)
   ```

### Monitoring Memory Usage

You can monitor memory usage during streaming:

```python
import psutil
import os
from memory_profiler import memory_usage

# Function to monitor
def stream_large_file():
    tm.flatten_stream(
        file_path="very_large_data.json",
        name="data",
        output_path="output_dir/parquet",
        output_format="parquet",
        chunk_size=10000,
        low_memory=True
    )

# Monitor memory usage
mem_usage = memory_usage(stream_large_file)
print(f"Peak memory usage: {max(mem_usage)} MiB")
```

## Streaming to Different Destinations

### Streaming to Directory Structure

```python
# Stream to a directory structure
tm.flatten_stream(
    file_path="data.json",
    name="records",
    output_path="output_dir",  # Directory to store output files
    output_format="parquet"
)
```

### Streaming to Single File

```python
# Stream to a single file (for formats that support it)
tm.flatten_stream(
    file_path="data.json",
    name="records",
    output_path="output_dir/data.parquet",  # Specific file path
    output_format="parquet"
)
```

## Conclusion

Streaming processing is a powerful feature for handling large datasets efficiently. By using `flatten_stream` or `flatten` with `stream=True`, you can process data that exceeds available memory and output directly to various formats without intermediate storage.
