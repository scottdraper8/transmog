---
title: File Processing
---

For API details, see [Processor API](../../api/processor.md) and [Process API](../../api/process.md).

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
from transmog import Processor

processor = Processor()

# Process a file containing a JSON object or array
result = processor.process_file(
    file_path="data.json",
    entity_name="customers"
)

# Access the processed data
main_table = result.get_main_table()
print(f"Processed {len(main_table)} records")
```

### JSON Lines (JSONL) Files

JSONL files contain one JSON object per line and are suitable for large datasets:

```python
# Process a JSONL file (one JSON object per line)
result = processor.process_file(
    file_path="data.jsonl",
    entity_name="logs"
)
```

For large JSONL files, chunked processing improves memory efficiency:

```python
# Process a large JSONL file in chunks
result = processor.process_chunked(
    "large_data.jsonl",
    entity_name="logs",
    chunk_size=1000  # Process 1000 records at a time
)
```

## Processing CSV Files

Transmog includes specialized handling for CSV files:

```python
# Process a CSV file
result = processor.process_csv(
    file_path="data.csv",
    entity_name="products",
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
result = processor.process_csv(
    file_path="data.tsv",
    entity_name="data",
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
result = processor.process_chunked(
    "large_file.jsonl",
    entity_name="data",
    chunk_size=1000,  # Process 1000 records at a time
    input_format="jsonl"  # Explicitly specify the format
)
```

### Streaming Processing

Data can be streamed directly to output formats without storing intermediate results in memory:

```python
# Stream process a file directly to an output format
processor.stream_process_file(
    file_path="large_file.json",
    entity_name="data",
    output_format="parquet",
    output_destination="output_directory"
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
result = processor.process_file(
    file_path="data.txt",  # File with non-standard extension
    entity_name="records",
    input_format="jsonl"   # Specify it's JSONL format
)
```

## Error Handling During File Processing

Multiple error recovery strategies are available for handling problematic files:

```python
from transmog import Processor

# Create a processor with skip-and-log error recovery
processor = Processor.with_error_handling(recovery_strategy="skip")

# Process a file that may contain errors
try:
    result = processor.process_file(
        file_path="problematic_data.json",
        entity_name="records"
    )

    # Records with errors will be skipped
    print(f"Successfully processed {len(result.get_main_table())} records")
    print(f"Errors: {result.get_errors()}")
except Exception as e:
    print(f"Processing failed: {e}")
```

## Writing Results to Files

Processed results can be written to various file formats:

```python
# Process a file
result = processor.process_file("input.json", entity_name="data")

# Write to JSON files
json_files = result.write_all_json(
    base_path="output/json",
    indent=2  # Pretty-print the JSON
)

# Write to CSV files
csv_files = result.write_all_csv(
    base_path="output/csv",
    include_header=True
)

# Write to Parquet files
parquet_files = result.write_all_parquet(
    base_path="output/parquet",
    compression="snappy"
)
```

## Advanced File Processing

### Multi-Format Processing Pipeline

Data can be processed across multiple formats:

```python
# Process CSV to JSON
result = processor.process_csv("data.csv", entity_name="data")
result.write_all_json("output/json")

# Process JSON to Parquet
result = processor.process_file("data.json", entity_name="data")
result.write_all_parquet("output/parquet")
```

### Processing Files with Custom Schema

Schema transformations can be applied during processing:

```python
from transmog import Processor, TransmogConfig

# Create a configuration with schema transformations
config = (
    TransmogConfig.default()
    .with_transforms(
        field_transforms={
            "price": lambda x: float(x) * 1.1,  # Increase prices by 10%
            "date": lambda x: x.split("T")[0]   # Extract date part only
        }
    )
)

processor = Processor(config)

# Process file with transformations
result = processor.process_file(
    file_path="products.json",
    entity_name="products"
)
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
