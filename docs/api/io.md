# IO API

> **User Guide**: For usage guidance and examples, see the [Input/Output Operations User Guide](../user/processing/io.md).

This document describes the IO functionality in Transmog.

## Format Registry

```python
from transmog.io import FormatRegistry
```

The `FormatRegistry` manages available input and output formats.

### Methods

| Method | Description |
|--------|-------------|
| `register_format(name, writer_class, ...)` | Register a new output format |
| `get_writer_class(format_name)` | Get the writer class for a format |
| `is_format_available(format_name)` | Check if a format is available |

## Format Detection

```python
from transmog.io import detect_format
```

Detect the format of input data:

```python
format_type = detect_format("data.json")  # Returns "json"
format_type = detect_format("data.csv")   # Returns "csv"
```

## Writer Factory

```python
from transmog.io import create_writer
```

Create a writer for a specific format:

```python
writer = create_writer("json", indent=2)
writer.write_records(records, "output.json")
```

## Data Writer Interface

```python
from transmog.io import DataWriter
```

Base class for all data writers.

### Methods

| Method | Description |
|--------|-------------|
| `write_records(records, file_path)` | Write records to a file |
| `write_bytes(records)` | Convert records to bytes |
| `write_objects(records)` | Convert records to Python objects |

## Streaming Writer Interface

```python
from transmog.io import StreamingWriter, create_streaming_writer
```

Interface for streaming writers:

```python
writer = create_streaming_writer("parquet", destination="output_dir")
```

### Methods

| Method | Description |
|--------|-------------|
| `initialize_main_table(schema=None)` | Initialize main table |
| `initialize_child_table(table_name, schema=None)` | Initialize child table |
| `write_main_records(records)` | Write records to main table |
| `write_child_records(table_name, records)` | Write records to child table |
| `finalize()` | Finalize all writing operations |

## Streaming Format Utilities

```python
from transmog.io import (
    get_supported_streaming_formats,
    is_streaming_format_available
)
```

Check available streaming formats:

```python
formats = get_supported_streaming_formats()  # Returns list of available formats
is_available = is_streaming_format_available("parquet")  # Returns True/False
```

## Specific Writers

### Parquet Streaming Writer

```python
from transmog.io import ParquetStreamingWriter
```

Writer for streaming to Parquet format.

```python
writer = ParquetStreamingWriter(
    destination_path="output_dir",
    compression="snappy",
    row_group_size=10000
)
```

## IO Initialization

```python
from transmog.io import initialize_io_features
```

Initialize IO features (called automatically during import).
