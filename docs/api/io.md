# IO API

> **User Guide**: For usage guidance and examples, see the [Input/Output Operations User Guide](../user/processing/io.md).

This document describes the IO functionality in Transmog.

## Input/Output in the New API

In Transmog 1.1.0, IO operations are simplified through the main API functions and the `FlattenResult` class.

### Reading Data

```python
import transmog as tm

# Read from a JSON file
result = tm.flatten_file(
    file_path="data.json",
    name="records"
)

# Read from a CSV file
result = tm.flatten_file(
    file_path="data.csv",
    name="records",
    has_header=True,
    delimiter=","
)
```

### Writing Data

```python
import transmog as tm

# Process data
result = tm.flatten(data, name="records")

# Save to JSON
result.save("output_directory", format="json")

# Save to CSV
result.save("output_directory", format="csv")

# Save to Parquet
result.save("output_directory", format="parquet")
```

### Streaming IO

```python
import transmog as tm

# Stream process a file directly to output
tm.flatten_stream(
    file_path="large_data.json",
    name="records",
    output_path="output_directory",
    output_format="parquet"
)
```

## Supported Formats

Transmog supports the following formats for input and output:

| Format | Input Support | Output Support | Streaming Support |
|--------|--------------|----------------|------------------|
| JSON | ✅ | ✅ | ✅ |
| CSV | ✅ | ✅ | ✅ |
| Parquet | ❌ | ✅ | ✅ |

## Format-Specific Options

### JSON Options

```python
# Save with JSON options
result.save(
    "output_directory",
    format="json",
    indent=2,              # Pretty-print with 2-space indentation
    encoding="utf-8"       # File encoding
)

# Stream with JSON options
tm.flatten_stream(
    file_path="data.json",
    name="records",
    output_path="output_directory",
    output_format="json",
    indent=2
)
```

### CSV Options

```python
# Save with CSV options
result.save(
    "output_directory",
    format="csv",
    include_header=True,   # Include column headers
    delimiter=",",         # Field delimiter
    quotechar='"',         # Character for quoting fields
    encoding="utf-8"       # File encoding
)

# Stream with CSV options
tm.flatten_stream(
    file_path="data.csv",
    name="records",
    output_path="output_directory",
    output_format="csv",
    include_header=True,
    delimiter=","
)
```

### Parquet Options

```python
# Save with Parquet options
result.save(
    "output_directory",
    format="parquet",
    compression="snappy",    # Compression codec
    row_group_size=10000     # Number of rows per row group
)

# Stream with Parquet options
tm.flatten_stream(
    file_path="data.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    compression="snappy",
    row_group_size=10000
)
```

## Format Auto-Detection

The `save()` method can automatically detect the format based on the file extension:

```python
# Format detected from extension
result.save("output.json")    # JSON format
result.save("output.csv")     # CSV format
result.save("output.parquet") # Parquet format
```

## Multiple Format Output

You can save the same result in multiple formats:

```python
# Save in multiple formats
result = tm.flatten(data, name="records")

# Save in different formats
result.save("output/json", format="json")
result.save("output/csv", format="csv")
result.save("output/parquet", format="parquet")
```



## Memory-Efficient IO

For large datasets, use memory-efficient streaming:

```python
# Stream process with memory optimization
tm.flatten_stream(
    file_path="very_large_file.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    low_memory=True,
    chunk_size=100
)
```

## Advanced: Internal IO Classes

> Note: These classes are used internally and most users won't need to interact with them directly.

### Format Registry

```python
from transmog.io import FormatRegistry
```

The `FormatRegistry` manages available input and output formats.

### Writer Factory

```python
from transmog.io import create_writer
```

Create a writer for a specific format:

```python
writer = create_writer("json", indent=2)
writer.write_records(records, "output.json")
```

### Data Writer Interface

```python
from transmog.io import DataWriter
```

Base class for all data writers.

### Streaming Writer Interface

```python
from transmog.io import StreamingWriter, create_streaming_writer
```

Interface for streaming writers:

```python
writer = create_streaming_writer("parquet", destination="output_dir")
```
