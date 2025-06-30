---
title: File Processing
---

For API details, see [Core API](../../api/core.md) and [IO API](../../api/io.md).

# File Processing

**Related Guides:**

- [Processing Overview](./processing-overview.md)
- [Streaming](../advanced/streaming.md)

This document describes file processing capabilities for various formats in Transmog.

## Supported File Formats

Transmog processes data from the following file formats:

| Format | Description | Example Files |
|--------|-------------|---------------|
| JSON | Single JSON object or array | `data.json` |
| JSONL | JSON Lines (one object per line) | `data.jsonl` |
| CSV | Comma-separated values | `data.csv` |
| Custom Delimited | Custom delimiter-separated values | `data.tsv` (tab-delimited) |

## Processing JSON Files

### Single JSON Object/Array Files

Files containing a single JSON object or an array of objects are processed as follows:

```python
import transmog as tm

# Process a file containing a JSON object or array
result = tm.flatten_file(
    file_path="data.json",
    name="customers"
)

# Access the processed data
main_table = result.main
print(f"Processed {len(main_table)} records")
```

### JSON Lines (JSONL) Files

JSONL files contain one JSON object per line and are suitable for large datasets:

```python
# Process a JSONL file (one JSON object per line)
result = tm.flatten_file(
    file_path="data.jsonl",
    name="logs"
)
```

For large JSONL files, chunked processing improves memory efficiency:

```python
# Process a large JSONL file in chunks
result = tm.flatten_file(
    file_path="large_data.jsonl",
    name="logs",
    chunk_size=1000  # Process 1000 records at a time
)
```

## Processing CSV Files

Transmog includes specialized handling for CSV files:

```python
# Process a CSV file
result = tm.flatten_file(
    file_path="data.csv",
    name="products",
    delimiter=",",        # Comma delimiter (default)
    has_header=True,      # First row contains headers
    infer_types=True,     # Try to infer data types
    sanitize_column_names=True  # Clean up column names
)
```

### CSV Processing Options

| Option | Description | Default |
|--------|-------------|---------|
| `delimiter` | Field separator character | `,` |
| `has_header` | Whether the first row contains headers | `True` |
| `infer_types` | Attempt to infer data types | `True` |
| `sanitize_column_names` | Clean up column names | `True` |
| `skip_rows` | Number of rows to skip at the start | `0` |
| `quote_char` | Character used for quoting fields | `"` |
| `null_values` | Values to interpret as NULL | `["", "NULL", "null"]` |
| `encoding` | File encoding | `"utf-8"` |

### Custom Delimited Files

Files using delimiters other than commas are processed as follows:

```python
# Process a tab-delimited file
result = tm.flatten_file(
    file_path="data.tsv",
    name="data",
    delimiter="\t",    # Tab delimiter
    has_header=True
)
```

## Memory-Efficient File Processing

For large files that exceed available memory, several processing options are available:

### Chunked Processing

Files can be processed in manageable chunks:

```python
# Process a large file in chunks
result = tm.flatten_file(
    file_path="large_file.jsonl",
    name="data",
    chunk_size=1000,  # Process 1000 records at a time
    input_format="jsonl"  # Explicitly specify the format
)
```

### Streaming Processing

Data can be streamed directly to output formats without storing intermediate results in memory:

```python
# Stream process a file directly to an output format
tm.flatten_stream(
    file_path="large_file.json",
    name="data",
    output_format="parquet",
    output_path="output_directory"
)
```

## File Format Detection

The format is automatically detected based on file extension:

| Extension | Format Detected |
|-----------|----------------|
| `.json` | JSON |
| `.jsonl`, `.ndjson` | JSONL (JSON Lines) |
| `.csv` | CSV |
| `.tsv` | Tab-separated values |

Explicit format specification overrides automatic detection:

```python
# Explicitly specify the format
result = tm.flatten_file(
    file_path="data.txt",  # File with non-standard extension
    name="records",
    input_format="jsonl"   # Specify it's JSONL format
)
```

## Error Handling During File Processing

Multiple error recovery strategies are available for handling problematic files:

```python
import transmog as tm

# Process a file that may contain errors with skip error handling
try:
    result = tm.flatten_file(
        file_path="problematic_data.json",
        name="records",
        error_handling="skip"  # Skip records with errors
    )

    # Records with errors will be skipped
    print(f"Successfully processed {len(result.main)} records")
    print(f"Errors: {result.errors}")
except Exception as e:
    print(f"Processing failed: {e}")
```

## Writing Results to Files

Processed results can be written to various file formats:

```python
# Process a file
result = tm.flatten_file("input.json", name="data")

# Write to JSON files
result.save(
    path="output/json",
    format="json",
    indent=2  # Pretty-print the JSON
)

# Write to CSV files
result.save(
    path="output/csv",
    format="csv",
    include_header=True
)

# Write to Parquet files
result.save(
    path="output/parquet",
    format="parquet",
    compression="snappy"
)
```

## Advanced File Processing

### Multi-Format Processing Pipeline

Data can be processed across multiple formats:

```python
import transmog as tm

# Process CSV to JSON
result = tm.flatten_file("data.csv", name="data")
result.save("output/json", format="json")

# Process JSON to Parquet
result = tm.flatten_file("data.json", name="data")
result.save("output/parquet", format="parquet")
```

### Processing Files with Custom Transformations

Field transformations can be applied during processing:

```python
import transmog as tm

# Define custom transformations
def increase_price(price):
    return float(price) * 1.1  # Increase prices by 10%

def extract_date(date_str):
    return date_str.split("T")[0]  # Extract date part only

# Process with transformations
result = tm.flatten_file(
    "products.json",
    name="products",
    transforms={
        "price": increase_price,
        "date": extract_date
    }
)
```

### Direct Streaming to Output

For very large files, you can stream directly to output files without keeping data in memory:

```python
import transmog as tm

# Stream process a large file directly to Parquet files
tm.flatten_stream(
    file_path="very_large_file.jsonl",
    name="big_data",
    output_path="output/big_data",
    output_format="parquet",
    chunk_size=5000,  # Process in chunks of 5000 records
    low_memory=True   # Optimize for memory usage
)
```

### Auto-Format Detection for Output

The output format can be automatically detected from the file extension:

```python
# Save to JSON (detected from .json extension)
result.save("output/data.json")

# Save to CSV (detected from .csv extension)
result.save("output/data.csv")

# Save to Parquet (detected from .parquet extension)
result.save("output/data.parquet")
```

## Performance Considerations

When processing large files, the following factors affect performance:

- Chunk size determines the memory usage during processing
- Format selection impacts processing speed and output file size
- Error recovery strategy selection affects resilience and throughput
- File encoding and compression settings influence I/O performance

## Limitations

- Processing extremely large files (multi-GB) requires chunked or streaming methods
- CSV files with complex nested data may require pre-processing
- File format auto-detection relies on file extensions and may require explicit format specification for non-standard extensions
- Memory constraints apply when processing files without chunking or streaming
