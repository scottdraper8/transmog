# Configuration

> **API Reference**: For detailed API documentation, see the [Configuration API Reference](../../api/config.md).

Transmog v1.1.0 provides two levels of configuration:

1. **Simple Options** - Direct parameters to the `flatten()` function
2. **Advanced Configuration** - Full configuration system for complex scenarios

## Simple Configuration (New in v1.1.0)

For most use cases, you can configure Transmog directly through parameters to the `flatten()` function:

```python
import transmog as tm

# Basic configuration through parameters
result = tm.flatten(
    data,
    name="users",

    # ID generation options
    id_field="id",               # Use natural IDs when available
    parent_id_field="_parent_id", # Custom parent ID field name
    add_timestamp=True,           # Add processing timestamp

    # Naming options
    separator=".",                # Use dots for nested fields
    nested_threshold=5,           # Control when to create child tables

    # Array handling
    arrays="separate",            # How to handle arrays: "separate", "inline", or "skip"

    # Data options
    preserve_types=False,         # Convert values to strings
    skip_null=True,               # Skip null values
    skip_empty=True,              # Skip empty values

    # Error handling
    errors="skip",                # Skip problematic records

    # Performance
    batch_size=1000,              # Batch size for processing
    low_memory=False              # Enable memory optimization
)
```

### Common Simple Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | str | `"data"` | Base name for the flattened tables |
| `id_field` | str/dict | `None` | Field name for record IDs or dict mapping table names to ID fields |
| `parent_id_field` | str | `"_parent_id"` | Field name for parent record IDs |
| `add_timestamp` | bool | `False` | Add processing timestamp to records |
| `separator` | str | `"_"` | Separator for nested field names |
| `nested_threshold` | int | `4` | Depth threshold for creating child tables |
| `arrays` | str | `"separate"` | Array handling: "separate", "inline", or "skip" |
| `preserve_types` | bool | `False` | Keep original data types (don't convert to strings) |
| `skip_null` | bool | `True` | Skip null values in output |
| `skip_empty` | bool | `True` | Skip empty values in output |
| `errors` | str | `"raise"` | Error handling: "raise", "skip", or "warn" |
| `batch_size` | int | `1000` | Number of records per batch for processing |
| `low_memory` | bool | `False` | Enable memory optimization for large datasets |

## File Processing Options

When processing files directly with `flatten_file()`, you can specify additional options:

```python
# Process a file with options
result = tm.flatten_file(
    "data.json",
    name="records",              # Base name for tables
    format="json",               # Force specific format
    errors="warn",               # Log warnings but continue

    # All other options from flatten() are also available
    separator=".",
    arrays="separate",
    preserve_types=True
)
```

## Streaming Options

For streaming processing with `flatten_stream()`, you can configure output formats:

```python
# Stream with format-specific options
tm.flatten_stream(
    large_dataset,
    output_path="output/",       # Output directory or file
    name="big_data",             # Base name for tables
    format="parquet",            # Output format: "json", "csv", "parquet"

    # Format-specific options
    compression="snappy",        # Compression for Parquet

    # Processing options (same as flatten())
    batch_size=10000,            # Records per batch
    add_timestamp=True,          # Add timestamps
    arrays="separate",           # Array handling
    errors="skip"                # Error handling
)
```

## Save Method Options

The `save()` method accepts format-specific options:

```python
# Save with format-specific options
result = tm.flatten(data, name="products")

# JSON options
result.save(
    "output.json",
    format="json",               # Explicitly specify format
    indent=2,                    # Pretty-print with indentation
    ensure_ascii=False           # Allow non-ASCII characters
)

# CSV options
result.save(
    "output_csv/",
    format="csv",                # Explicitly specify format
    delimiter=",",               # Field delimiter
    quotechar='"',               # Quote character
    include_headers=True         # Include header row
)

# Parquet options
result.save(
    "output.parquet",
    format="parquet",            # Explicitly specify format
    compression="snappy",        # Compression algorithm
    row_group_size=10000         # Row group size
)
```

## Advanced Configuration

For complex scenarios, you can still use the full configuration system through the internal API:

```python
from transmog.process import Processor
from transmog.config import TransmogConfig

# Create a custom configuration with the fluent API
config = (
    TransmogConfig.default()
    .with_naming(
        separator=".",
        max_table_component_length=30,
        deep_nesting_threshold=4
    )
    .with_processing(
        batch_size=5000,
        cast_to_string=True,
        include_empty=False
    )
    .with_metadata(
        id_field="_custom_id",
        parent_field="_parent_id",
        time_field="_processed_at"
    )
    .with_error_handling(
        recovery_strategy="skip",
        allow_malformed_data=True,
        max_retries=3
    )
    .with_caching(
        enabled=True,
        maxsize=10000,
        clear_after_batch=False
    )
)

# Create processor with this configuration
processor = Processor(config=config)

# Process data with advanced configuration
result = processor.process(data, entity_name="users")
```

## Pre-configured Optimization Modes

The advanced API provides factory methods for common optimization scenarios:

```python
from transmog.process import Processor

# Memory-optimized for large datasets
processor = Processor.memory_optimized()
result = processor.process(data, entity_name="users")

# Performance-optimized for speed
processor = Processor.performance_optimized()
result = processor.process(data, entity_name="users")
```

## ID Generation Strategies

### Natural IDs

Use existing fields as record identifiers:

```python
# Simple API - use a specific field as ID
result = tm.flatten(data, name="users", id_field="user_id")

# Simple API - use different fields for different tables
result = tm.flatten(
    data,
    name="users",
    id_field={
        "": "id",                # Root level uses "id" field
        "users_orders": "order_id"  # Order records use "order_id" field
    }
)

# Advanced API
processor = Processor.with_deterministic_ids({
    "": "id",                     # Root level uses "id" field
    "user_orders": "order_id"     # Order records use "order_id" field
})
```

### Custom ID Generation

For advanced ID generation, use the internal API:

```python
# Advanced API
def custom_id_strategy(record):
    return f"CUSTOM-{record.get('id', 'unknown')}"

processor = Processor.with_custom_id_generation(custom_id_strategy)
```

## Error Handling Strategies

Three error handling modes are available:

1. **"raise"** (default) - Raises exceptions on errors
2. **"skip"** - Skips problematic records and continues
3. **"warn"** - Logs warnings but continues processing

```python
# Simple API
result = tm.flatten(data, name="users", errors="skip")

# Advanced API
config = TransmogConfig.default().with_error_handling(recovery_strategy="skip")
processor = Processor(config=config)
```

## Memory Optimization

For large datasets, use these approaches:

```python
# Simple streaming API - best for very large datasets
tm.flatten_stream(
    large_dataset,
    output_path="output/",
    name="big_data",
    format="parquet",
    batch_size=10000
)

# Simple API with memory optimization
result = tm.flatten(
    large_dataset,
    name="big_data",
    low_memory=True,
    batch_size=5000
)

# Advanced API with memory optimization
processor = Processor.memory_optimized()
result = processor.process(data, entity_name="users")
```

## Configuration Best Practices

- **Simple vs. Advanced**: Use the simple API for most cases, advanced for complex scenarios
- **Performance vs. Memory**: For large datasets, use streaming or memory optimization
- **ID Consistency**: Use natural IDs when possible for consistent record identification
- **Error Recovery**: Choose error handling based on data quality:
  - `"raise"`: For data where errors indicate problems that should stop processing
  - `"skip"`: For data with known issues that should be ignored
  - `"warn"`: For data with issues that should be logged but processing should continue
- **Array Handling**: Choose array handling based on your needs:
  - `"separate"`: For normalized data with child tables (default)
  - `"inline"`: For denormalized data with arrays kept in the parent
  - `"skip"`: To ignore arrays completely

## Migration from v1.0.x

If you're migrating from v1.0.x, here's how configuration options map to the new API:

| v1.0.x Configuration | v1.1.0 Simple API |
|----------------------|-------------------|
| `TransmogConfig().with_naming(separator=".")` | `tm.flatten(data, separator=".")` |
| `TransmogConfig().with_metadata(id_field="custom_id")` | `tm.flatten(data, id_field="custom_id")` |
| `TransmogConfig().with_metadata(add_timestamp=True)` | `tm.flatten(data, add_timestamp=True)` |
| `TransmogConfig().with_error_handling(recovery_strategy="skip")` | `tm.flatten(data, on_error="skip")` |
| `TransmogConfig().with_processing(cast_to_string=False)` | `tm.flatten(data, cast_to_string=False)` |

For more complex configurations, you can still use the advanced API with the `Processor` class.
