# Configuration API Reference

> **User Guide**: For usage guidance and examples, see the [Configuration Guide](../user/essentials/configuration.md).

This document provides a reference for the configuration options in Transmog.

## Configuration Parameters

In Transmog 1.1.0, configuration is done through simple parameters passed to the main API functions (`flatten()`, `flatten_file()`, and `flatten_stream()`). This section documents all available configuration parameters.

### Basic Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | Required | Name for the main entity (used for table naming) |
| `separator` | str | "_" | Separator to use between path segments in field names |
| `cast_to_string` | bool | False | Whether to cast all values to strings |
| `include_empty` | bool | False | Whether to include empty values |
| `skip_null` | bool | True | Whether to skip null values |

### Metadata Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `add_metadata` | bool | True | Whether to add metadata fields (_id, _parent_id) |
| `add_timestamps` | bool | False | Whether to add timestamp metadata |
| `id_field` | str, dict, "auto" | None | Field(s) to use for deterministic IDs |

### Processing Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_depth` | int | 100 | Maximum recursion depth |
| `deep_nesting_threshold` | int | 4 | Threshold for special handling of deeply nested structures |
| `low_memory` | bool | False | Whether to optimize for low memory usage |
| `chunk_size` | int | None | Size of chunks for processing (enables chunked processing) |

### Error Handling Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `error_handling` | str | "raise" | Error handling strategy ("raise", "skip", or "warn") |
| `error_log` | str | None | Path to write error logs (when using `flatten_stream()`) |

### Output Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stream` | bool | False | Whether to stream results directly to output files |
| `output_path` | str | None | Path for output files when streaming |
| `output_format` | str | None | Format for output files when streaming ("json", "csv", "parquet") |

### Format-Specific Options

These options can be passed to both the main API functions and the `save()` method on `FlattenResult`.

#### CSV Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_header` | bool | True | Whether to include header row in CSV |
| `delimiter` | str | "," | Delimiter character for CSV |
| `quotechar` | str | '"' | Character for quoting fields |
| `encoding` | str | "utf-8" | File encoding |

#### Parquet Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `compression` | str | None | Compression codec ("snappy", "gzip", "brotli", "zstd", etc.) |
| `row_group_size` | int | None | Number of rows per row group |

#### JSON Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `indent` | int | 2 | Indentation level for pretty printing |
| `encoding` | str | "utf-8" | File encoding |

## Configuration Examples

### Basic Configuration

```python
import transmog as tm

# Basic configuration
result = tm.flatten(
    data=data,
    name="records",
    separator="_",
    cast_to_string=False,
    include_empty=False,
    skip_null=True
)
```

### Memory Optimization

```python
# Memory-optimized configuration
result = tm.flatten(
    data=data,
    name="records",
    low_memory=True,
    chunk_size=100
)
```

### ID Field Configuration

```python
# Using a specific field for IDs
result = tm.flatten(
    data=data,
    name="records",
    id_field="id"  # Use "id" field for deterministic IDs
)

# Using different fields for different tables
result = tm.flatten(
    data=data,
    name="records",
    id_field={
        "": "id",                # Main table uses "id" field
        "records_items": "itemId"  # Items table uses "itemId" field
    }
)

# Automatic ID field discovery
result = tm.flatten(
    data=data,
    name="records",
    id_field="auto"  # Automatically discover ID fields
)
```

### Error Handling Configuration

```python
# Skip records with errors
result = tm.flatten(
    data=data,
    name="records",
    error_handling="skip"  # Skip records with errors
)

# Log warnings but continue processing
result = tm.flatten(
    data=data,
    name="records",
    error_handling="warn"  # Log warnings but continue
)
```

### Streaming Configuration

```python
# Stream results directly to files
tm.flatten(
    data=data,
    name="records",
    stream=True,
    output_path="output_directory",
    output_format="parquet",
    compression="snappy"
)

# Or use flatten_stream directly
tm.flatten_stream(
    file_path="large_file.json",
    name="records",
    output_path="output_directory",
    output_format="csv",
    include_header=True,
    delimiter=","
)
```

### Saving Results with Format Options

```python
# Process data
result = tm.flatten(data, name="records")

# Save with format-specific options
result.save(
    "output_directory",
    format="parquet",
    compression="snappy",
    row_group_size=10000
)

# Save as CSV with options
result.save(
    "output_directory",
    format="csv",
    include_header=True,
    delimiter=",",
    quotechar='"',
    encoding="utf-8"
)
```

## Transforms

Transforms allow you to modify field values during processing. They are specified as a dictionary mapping field names to transform functions.

```python
# Define transform functions
def uppercase_name(value):
    """Convert name to uppercase"""
    if isinstance(value, str):
        return value.upper()
    return value

def calculate_total(order):
    """Calculate order total"""
    if isinstance(order, dict):
        price = order.get("price", 0)
        quantity = order.get("quantity", 1)
        return price * quantity
    return 0

# Apply transforms during processing
result = tm.flatten(
    data=data,
    name="records",
    transforms={
        "name": uppercase_name,           # Transform the "name" field
        "orders.total": calculate_total   # Transform the "orders.total" field
    }
)
```

## Migration from v1.0.6

If you were previously using the `TransmogConfig` class and its methods, here's how to migrate to the new API:

| Old API (v1.0.6) | New API (v1.1.0) |
|------------------|------------------|
| `TransmogConfig().memory_optimized()` | `low_memory=True` |
| `TransmogConfig().performance_optimized()` | `low_memory=False, chunk_size=None` |
| `TransmogConfig.with_deterministic_ids("id")` | `id_field="id"` |
| `TransmogConfig.with_custom_id_generation(func)` | Use transforms with `id_field` |
| `config.with_naming(separator="-")` | `separator="-"` |
| `config.with_processing(visit_arrays=False)` | Not directly supported, use transforms |
| `config.with_metadata(add_timestamps=True)` | `add_timestamps=True` |
| `config.with_error_handling(strategy=ErrorStrategy.SKIP_AND_LOG)` | `error_handling="skip"` |
