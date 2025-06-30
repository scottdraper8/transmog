# Processor API Reference

> **User Guide**: For usage guidance and examples, see:
>
> - [Processing Overview](../user/processing/processing-overview.md) - General processing concepts and methods
> - [Streaming Guide](../user/advanced/streaming.md) - Streaming processing techniques
> - [File Processing Guide](../user/processing/file-processing.md) - Processing data from various file formats
>
> For details on the underlying processing components, see the [Process API](process.md).

## New API (v1.1.0)

In Transmog v1.1.0, the `Processor` class has been replaced with a simpler, more intuitive API. The new API consists of three main functions:

- `flatten()` - Process in-memory data
- `flatten_file()` - Process data from a file
- `flatten_stream()` - Stream process data directly to output files

These functions provide a more straightforward interface for common use cases while maintaining all the functionality of the previous API.

```python
import transmog as tm

# Process in-memory data
result = tm.flatten(data, name="customers")

# Process a file
result = tm.flatten_file("data.json", name="customers")

# Stream process a file directly to output
tm.flatten_stream(
    file_path="large_data.json",
    name="customers",
    output_path="output_directory",
    output_format="parquet"
)
```

For detailed documentation on these functions, see the [Core API Reference](core.md).

## Internal Processor Class

> Note: The `Processor` class is now considered an internal implementation detail. Most users should use the new API functions instead.

For advanced users who need direct access to the underlying processor, it can still be imported from the internal module:

```python
from transmog.process import Processor
```

The internal `Processor` class provides the implementation for the new API functions and maintains backward compatibility with existing code.

### When to Use the Internal Processor

You might need to use the internal `Processor` class directly in these rare cases:

1. You have existing code using the old API that you haven't migrated yet
2. You need extremely fine-grained control over the processing pipeline
3. You're extending Transmog with custom processing logic

### Migration from Processor to New API

If you're currently using the `Processor` class, here's how to migrate to the new API:

| Old API (v1.0.6) | New API (v1.1.0) |
|------------------|------------------|
| `processor = Processor()` <br> `result = processor.process(data, entity_name="customers")` | `result = tm.flatten(data, name="customers")` |
| `result = processor.process_file("data.json", entity_name="customers")` | `result = tm.flatten_file("data.json", name="customers")` |
| `result = processor.process_csv("data.csv", entity_name="customers")` | `result = tm.flatten_file("data.csv", name="customers")` |
| `result = processor.process_stream(stream_data, streaming_output=True)` <br> `result.write_streaming_json("output")` | `tm.flatten_stream(data=stream_data, name="customers", output_path="output", output_format="json")` |
| `processor = Processor.memory_optimized()` | `result = tm.flatten(data, name="customers", low_memory=True)` |
| `processor = Processor.with_deterministic_ids("id")` | `result = tm.flatten(data, name="customers", id_field="id")` |

## Example: Using the New API

```python
import transmog as tm

# Process in-memory data
data = {
    "id": 1,
    "name": "John Doe",
    "orders": [
        {"id": 101, "product": "Laptop", "price": 999.99},
        {"id": 102, "product": "Mouse", "price": 24.99}
    ]
}

result = tm.flatten(data, name="customer")

# Access the main table
print(result.main)

# Access child tables
print(result.tables["customer_orders"])

# Save to files
result.save("output_directory", format="json")
```

## Example: Advanced Usage with Internal Processor

```python
from transmog.process import Processor
from transmog.config import TransmogConfig
from transmog.types import ProcessingMode

# Create a custom configuration
config = TransmogConfig()
config.processing.processing_mode = ProcessingMode.LOW_MEMORY
config.processing.batch_size = 100
config.metadata.id_field = "custom_id"

# Create a processor with the custom configuration
processor = Processor(config)

# Process data
result = processor.process(data, entity_name="customers")

# Access results
main_table = result.get_main_table()
child_tables = result.get_child_tables()
```

For most use cases, we strongly recommend using the new API functions (`flatten()`, `flatten_file()`, `flatten_stream()`) as they provide a simpler and more intuitive interface.
