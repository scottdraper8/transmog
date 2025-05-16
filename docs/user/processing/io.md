# Input/Output Operations

> **API Reference**: For detailed API documentation, see the [IO API Reference](../../api/io.md).

This document describes how to work with input and output operations in Transmog.

## Input Formats

Transmog can process data from various input formats:

### Python Objects

```python
import transmog as tm

# Process a Python dictionary
data = {
    "id": 123,
    "name": "Example",
    "items": [{"id": 1}, {"id": 2}]
}
processor = tm.Processor()
result = processor.process(data, entity_name="record")
```

### JSON Files

```python
# Process a JSON file
processor = tm.Processor()
result = processor.process("data.json", entity_name="records")
```

### JSONL Files

```python
# Process a JSONL (line-delimited JSON) file
processor = tm.Processor()
result = processor.process("data.jsonl", entity_name="records")
```

### CSV Files

```python
# Process a CSV file
processor = tm.Processor()
result = processor.process_csv(
    "data.csv",
    entity_name="records",
    delimiter=",",
    has_header=True
)
```

## Output Formats

Transmog provides several output options:

### In-Memory Output

```python
# Get as Python dictionaries
dict_output = result.to_dict()

# Get as JSON-serializable objects
json_objects = result.to_json_objects()

# Get as PyArrow Tables (if PyArrow is installed)
pa_tables = result.to_pyarrow_tables()
```

### Bytes Output

```python
# Get as JSON bytes
json_bytes = result.to_json_bytes(indent=2)

# Get as CSV bytes
csv_bytes = result.to_csv_bytes()

# Get as Parquet bytes (if PyArrow is installed)
parquet_bytes = result.to_parquet_bytes(compression="snappy")
```

### File Output

```python
# Write to JSON files
result.write_all_json("output/json")

# Write to CSV files
result.write_all_csv("output/csv")

# Write to Parquet files (if PyArrow is installed)
result.write_all_parquet("output/parquet", compression="snappy")
```

## Streaming I/O

For large datasets, streaming I/O can be used:

```python
# Stream process a file directly to Parquet output
processor.stream_process_file(
    "large_data.jsonl",
    entity_name="records",
    output_format="parquet",
    output_destination="output/stream"
)
```

### Creating a Streaming Writer

```python
from transmog.io import create_streaming_writer

# Create a streaming writer
writer = create_streaming_writer(
    "parquet",
    destination="output_dir",
    compression="snappy",
    row_group_size=10000
)

# Use the writer with a processor
with writer:
    for batch in batches:
        result = processor.process_batch(batch, entity_name="records")
        writer.write_main_records(result.get_main_table())
```

## Direct File Conversion

Convert directly from one format to another:

```python
# Process a file and write to a specific format
processor.process_file_to_format(
    "data.json",
    entity_name="records",
    output_format="parquet",
    output_path="output/data.parquet"
)
```

## Format Detection

Automatically detect input formats:

```python
from transmog.io import detect_format

# Detect the format of a file
format_type = detect_format("data.json")  # Returns "json"
```

## Supported Formats

Currently supported formats include:

- JSON (.json)
- JSONL (.jsonl)
- CSV (.csv)
- Parquet (.parquet, requires PyArrow)

## Format Options

Each format supports specific options:

### JSON Options

```python
# JSON formatting options
result.to_json_bytes(indent=2, sort_keys=True)
result.write_all_json("output", indent=2, sort_keys=True)
```

### CSV Options

```python
# CSV formatting options
result.to_csv_bytes(delimiter=",", include_header=True)
result.write_all_csv("output", delimiter=",", include_header=True)
```

### Parquet Options

```python
# Parquet formatting options (requires PyArrow)
result.to_parquet_bytes(compression="snappy", row_group_size=10000)
result.write_all_parquet(
    "output",
    compression="snappy",
    row_group_size=10000
)
```
