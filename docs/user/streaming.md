# Streaming Processing

Transmog provides streaming processing capabilities for handling large datasets with minimal memory usage.
This guide explains these features.

## When to Use Streaming Processing

Streaming processing is useful for:

- Processing large datasets that don't fit in memory
- Minimizing memory usage during processing
- Processing data incrementally
- Direct output to files without intermediate memory structures

## Streaming Processing Methods

Transmog provides methods for streaming processing:

### stream_process

Process any data source and stream directly to an output format:

```python
import transmog as tm

processor = tm.Processor()

# Stream process in-memory data
processor.stream_process(
    data=large_data_dict,  # Dictionary, list, or generator of records
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet",
    compression="snappy"  # Format-specific option
)

# Stream process a string containing JSON
processor.stream_process(
    data='{"id": 1, "items": [{"id": 101}, {"id": 102}]}',
    entity_name="records",
    output_format="json",
    output_destination="output_dir/json",
    indent=2  # Format-specific option
)
```

### stream_process_file

Stream process a file directly to an output format:

```python
# Stream process a JSON file
processor.stream_process_file(
    file_path="data.json",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet",
    compression="snappy"  # Format-specific option
)

# Stream process a JSONL file
processor.stream_process_file(
    file_path="data.jsonl",
    entity_name="records",
    output_format="csv",
    output_destination="output_dir/csv",
    include_header=True  # Format-specific option
)
```

### stream_process_csv

Stream process a CSV file with specialized options:

```python
# Stream process a CSV file
processor.stream_process_csv(
    file_path="data.csv",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet",
    delimiter=",",        # CSV-specific option
    has_header=True,      # CSV-specific option
    null_values=["", "NULL", "null"],  # CSV-specific option
    sanitize_column_names=True,  # CSV-specific option
    infer_types=True,     # CSV-specific option
    skip_rows=0,          # CSV-specific option
    quote_char='"',       # CSV-specific option
    encoding="utf-8",     # CSV-specific option
    compression="snappy"  # Parquet-specific option
)
```

### stream_process_file_with_format

Stream process a file with an explicitly specified format:

```python
# Stream process a file with explicit format
processor.stream_process_file_with_format(
    file_path="data.txt",
    entity_name="records",
    output_format="json",
    format_type="jsonl",  # Explicitly specify input format
    output_destination="output_dir/json",
    indent=2             # JSON-specific option
)
```

## Streaming to Memory

You can stream to in-memory objects like StringIO or BytesIO:

```python
import io

# Create a BytesIO buffer
buffer = io.BytesIO()

# Stream to buffer
processor.stream_process_file(
    file_path="data.json",
    entity_name="records",
    output_format="parquet",
    output_destination=buffer
)

# Get the bytes
parquet_bytes = buffer.getvalue()

# Or use StringIO for text formats
csv_buffer = io.StringIO()
processor.stream_process_file(
    file_path="data.json",
    entity_name="records",
    output_format="csv",
    output_destination=csv_buffer
)
csv_string = csv_buffer.getvalue()
```

## Advanced Streaming with StreamingWriter

For more control over the streaming process, you can use the `StreamingWriter` interface:

```python
from transmog.io import create_streaming_writer

# Create a streaming writer
with create_streaming_writer(
    "parquet",
    destination="output_dir/parquet",
    compression="snappy",
    row_group_size=10000
) as writer:
    # Process data in batches
    for batch in data_batches:
        # Process each batch
        batch_result = processor.process_batch(batch, entity_name="records")

        # Write main table records
        writer.write_main_records(batch_result.get_main_table())

        # Initialize and write child tables
        for table_name in batch_result.get_table_names():
            writer.initialize_child_table(table_name)
            writer.write_child_records(
                table_name,
                batch_result.get_child_table(table_name)
            )
```

## Available StreamingWriter Implementations

Transmog provides the following StreamingWriter implementations:

- `ParquetStreamingWriter` - For streaming to Parquet files

```python
from transmog.io.writers.parquet import ParquetStreamingWriter

# Create a Parquet streaming writer directly
with ParquetStreamingWriter(
    destination="output_dir/parquet",
    compression="snappy",
    row_group_size=10000
) as writer:
    # Use the writer
    writer.write_main_records(records)
```

## Checking Available Streaming Formats

You can check which streaming formats are available:

```python
from transmog.io import get_supported_streaming_formats, is_streaming_format_available

# Get all supported streaming formats
formats = get_supported_streaming_formats()
print(f"Supported streaming formats: {formats}")

# Check if a specific format is available
if is_streaming_format_available("parquet"):
    print("Parquet streaming is available")
else:
    print("Parquet streaming is not available")
```

## Memory Optimized Processing with Streaming

You can combine chunked processing with streaming output:

```python
# Memory-optimized processor
processor = tm.Processor.memory_optimized()

# Process a large file in chunks and stream to output
processor.stream_process_file(
    file_path="large_data.jsonl",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet",
    compression="snappy"
)
```

## Processing Data Streams

You can process data streams or generators:

```python
# Create a data generator
def generate_records():
    for i in range(1000):
        yield {"id": i, "value": f"Item {i}"}

# Stream process the generator
processor.stream_process(
    data=generate_records(),
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet"
)
```

## Error Handling in Streaming

Streaming processing supports error recovery strategies:

```python
# Create a processor with error handling for streaming
processor = tm.Processor.with_partial_recovery()

# Stream process with error handling
processor.stream_process_file(
    file_path="problematic_data.json",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet"
)
```

## Memory Usage Controls

Control memory usage during streaming processing:

```python
# Configure chunk size for streaming
processor.stream_process_file(
    file_path="large_data.jsonl",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet",
    chunk_size=1000,  # Process 1000 records at a time
    row_group_size=5000  # Parquet-specific row group size
)
```

## Performance Considerations

When using streaming processing:

1. **Buffer Size**: Set appropriate chunk sizes based on your data and available memory
2. **Row Groups**: For Parquet output, adjust row group size for optimal read/write performance
3. **Compression**: Choose a compression algorithm with a good balance of speed and compression ratio
4. **Error Recovery**: For large datasets, use `SkipAndLogRecovery` to continue processing even if some records fail

## Combining Strategies

You can combine different strategies for optimal results:

```python
# Memory-optimized processor with partial recovery
processor = tm.Processor.memory_optimized().with_partial_recovery()

# Stream process a large JSONL file in chunks
processor.stream_process_file(
    file_path="large_data.jsonl",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet",
    chunk_size=1000,
    compression="snappy"
)
```

## Streaming vs. Normal Processing

This table summarizes when to use streaming vs. normal processing:

| Aspect | Streaming Processing | Normal Processing |
|--------|---------------------|-------------------|
| Memory Usage | Lower | Higher |
| Processing Speed | Potentially slower | Potentially faster |
| Result Access | Write-only | Random access |
| Use Case | Large datasets | Small to medium datasets |
| Memory Control | Fine-grained | Less control |
| Output Flexibility | Limited to available writers | All formats |
| Error Recovery | Supported | Supported |

Choose streaming processing when memory efficiency is more important than processing speed or when you
need to process datasets that don't fit in memory.
