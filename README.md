# Transmog

[![PyPI version](https://img.shields.io/pypi/v/transmog.svg?logo=pypi)](https://pypi.org/project/transmog/)
[![Python versions](https://img.shields.io/badge/python-3.9%2B-blue?logo=python)](https://pypi.org/project/transmog/)
[![License](https://img.shields.io/github/license/scottdraper8/transmog.svg?logo=github)](https://github.com/scottdraper8/transmog/blob/main/LICENSE)

A Python library for transforming complex nested data structures into flat,
tabular formats while preserving hierarchical relationships.

## Features

- **Multiple Input Formats**: JSON, JSONL, CSV
- **Nested Structure Handling**: Flattens deeply nested objects with customizable separators
- **Array Processing**: Extracts arrays as child tables with parent-child relationships maintained
- **Output Options**: Python dictionaries, PyArrow tables, JSON, CSV, Parquet
- **Performance Features**: Chunked processing, streaming output, memory optimization
- **Data Integrity**: Deterministic ID generation, consistent parent-child linking
- **Error Recovery**: Configurable strategies for handling malformed data

## Installation

```bash
pip install transmog
```

Optional dependencies:

```bash
pip install transmog[dev]  # Development tools
```

## Quick Example

```python
import transmog as tm

# Sample nested data
data = {
    "user": {
        "id": 1,
        "name": "John Doe",
        "contact": {
            "email": "john@example.com"
        },
        "orders": [
            {"id": 101, "amount": 99.99},
            {"id": 102, "amount": 45.50}
        ]
    }
}

# Process the data
processor = tm.Processor()
result = processor.process(data)

# Access the data
tables = result.to_dict()
main_table = tables["main"]
orders = tables["user_orders"]

# Export to different formats
result.write_all_json("output/json")
result.write_all_csv("output/csv")
result.write_all_parquet("output/parquet")
```

## Configuration

```python
# Use pre-configured modes
config = tm.TransmogConfig.memory_optimized()
# or
config = tm.TransmogConfig.performance_optimized()

# Custom configuration
config = (
    tm.TransmogConfig.default()
    .with_naming(separator=".")
    .with_processing(cast_to_string=True)
    .with_metadata(id_field="custom_id")
    .with_error_handling(max_retries=3)
)

processor = tm.Processor(config=config)
```

## Large Dataset Processing

```python
# Memory-optimized processing
processor = tm.Processor.memory_optimized()

# Chunked processing
result = processor.process_chunked(
    "large_data.jsonl",
    entity_name="records",
    chunk_size=1000
)

# Streaming output
processor.stream_process_file(
    "large_data.jsonl",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir"
)
```

## Error Handling

```python
# Skip and log errors
processor = tm.Processor().with_error_handling(recovery_strategy="skip")

# Partial recovery (preserves valid portions)
processor = tm.Processor.with_partial_recovery()
```

## Documentation

- [Installation Guide](https://scottdraper8.github.io/transmog/installation.html)
- [Getting Started](https://scottdraper8.github.io/transmog/getting-started.html)
- [Configuration Guide](https://scottdraper8.github.io/transmog/configuration.html)
- [API Reference](https://scottdraper8.github.io/transmog/api/index.html)
- [Examples](examples/README.md)

## License

MIT License
