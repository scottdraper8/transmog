# Transmog

[![PyPI version](https://img.shields.io/pypi/v/transmog.svg)](https://pypi.org/project/transmog/)
[![Python versions](https://img.shields.io/pypi/pyversions/transmog.svg)](https://pypi.org/project/transmog/)
[![License](https://img.shields.io/github/license/scottdraper8/transmog.svg)](https://github.com/scottdraper8/transmog/blob/main/LICENSE)

A Python library for transforming complex nested JSON data into flat, structured formats while preserving
parent-child relationships.

## Features

- **Multiple Input Formats**: Process JSON, JSONL (line-delimited JSON), and CSV files
- **Flattening**: Flatten deeply nested structures with customizable delimiter options
- **Array Handling**:
  - Extract arrays of objects as child tables with parent-child relationships
  - Process arrays of primitive values (strings, numbers, booleans) as child tables
  - Customizable null value handling
- **Output Flexibility**:
  - Native formats: Python dictionaries, JSON objects, PyArrow Tables
  - Bytes output: Serialize directly to Parquet, CSV, or JSON bytes
  - File export: Write to various file formats (JSON, CSV, Parquet)
- **Performance Optimization**:
  - Process large datasets with configurable memory management
  - Single-pass or chunked processing depending on data size
  - Stream data from files efficiently
- **Metadata Generation**: Track data lineage with automatic ID generation and parent-child relationships
- **Error Recovery**: Recover from malformed data with customizable strategies
- **Consistent IDs**: Deterministic ID generation for data consistency across processing runs

## Installation

```bash
pip install transmog
```

For minimal installation without optional dependencies:

```bash
pip install transmog[minimal]
```

For development installation:

```bash
pip install transmog[dev]
```

See the [installation guide](https://scottdraper8.github.io/transmog/installation.html) for more details.

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

# Process the data with default configuration
processor = tm.Processor()
result = processor.process(data)

# Native data structure output
tables = result.to_dict()                # Get all tables as Python dictionaries
pa_tables = result.to_pyarrow_tables()   # Get as PyArrow Tables

# Access the data in memory
main_table = tables["main"]              # Main table as Python dict
orders = tables["user_orders"]           # Child table as Python dict

# Bytes output for direct writing
json_bytes = result.to_json_bytes(indent=2)  # Get all tables as JSON bytes
csv_bytes = result.to_csv_bytes()        # Get all tables as CSV bytes
parquet_bytes = result.to_parquet_bytes()    # Get all tables as Parquet bytes

# Direct write to files
with open("main_table.json", "wb") as f:
    f.write(json_bytes["main"])

# Or use PyArrow tables directly
pa_table = pa_tables["main"]       # Work with PyArrow Table directly
print(f"Table has {pa_table.num_rows} rows and {pa_table.num_columns} columns")

# File output (still supported)
result.write_all_json("output_dir/json")
result.write_all_csv("output_dir/csv")
result.write_all_parquet("output_dir/parquet")
```

## Configuration

Transmog provides a flexible configuration system through the `TransmogConfig` class:

```python
import transmog as tm

# Use pre-configured modes
config = tm.TransmogConfig.memory_optimized()  # For large datasets
# or
config = tm.TransmogConfig.performance_optimized()  # For speed-critical processing

# Create custom configuration
config = (
    tm.TransmogConfig.default()
    .with_naming(
        separator=".",
        deeply_nested_threshold=4  # Configure when to simplify deeply nested paths
    )
    .with_processing(
        batch_size=5000,
        cast_to_string=True
    )
    .with_metadata(
        id_field="custom_id"
    )
    .with_error_handling(
        max_retries=3
    )
)

# Use the configuration
processor = tm.Processor(config=config)
```

See the [configuration guide](https://scottdraper8.github.io/transmog/configuration.html) for more details.

## Naming Convention

Transmog uses a simplified naming approach that combines field names with separators:

```python
# For table names (arrays)
# First level array: entity_arrayname
customer_orders

# Nested array: entity_parent_arrayname
customer_orders_items

# Deeply nested array: entity_first_nested_last
customer_orders_nested_details
```

Special handling for deeply nested paths prevents excessively long names while maintaining clarity.
The threshold for when paths are considered "deeply nested" is configurable (default is 4).

## Error Recovery Strategies

Transmog provides several recovery strategies for handling problematic data:

```python
# Default strict recovery (fails on any error)
processor = tm.Processor.default()

# Skip and log recovery (skips problematic records)
processor = tm.Processor().with_error_handling(recovery_strategy="skip")

# Partial recovery (preserves valid portions of problematic records)
processor = tm.Processor.with_partial_recovery()

# Process data that may contain errors
result = processor.process(problematic_data, entity_name="records")
```

The partial recovery strategy is particularly valuable when working with:

- Data migration from legacy systems
- Processing API responses with inconsistent structures
- Recovering data from malformed files

See the [error handling guide](https://scottdraper8.github.io/transmog/error-handling.html) for more information.

## Cache Configuration

Transmog provides configurable value processing cache for performance optimization:

```python
import transmog as tm

# Default configuration
processor = tm.Processor()

# Disable caching completely
processor = tm.Processor(
    tm.TransmogConfig.default().with_caching(enabled=False)
)

# Configure cache size
processor = tm.Processor(
    tm.TransmogConfig.default().with_caching(maxsize=50000)
)

# Clear cache after batch processing
processor = tm.Processor(
    tm.TransmogConfig.default().with_caching(clear_after_batch=True)
)

# Manually clear cache
processor.clear_cache()
```

The cache system improves performance when processing datasets with repeated values while providing memory
usage control. See the [cache configuration guide](https://scottdraper8.github.io/transmog/caching.html)
for more details.

## Processing Large Datasets

For large datasets, use memory-optimized configuration:

```python
# Memory-optimized processor
config = tm.TransmogConfig.memory_optimized()
processor = tm.Processor(config=config)

# Process a large file in chunks
result = processor.process_chunked(
    "large_data.jsonl",
    entity_name="records",
    chunk_size=1000  # Process 1000 records at a time
)
```

## Performance Benchmarking

Transmog provides tools to benchmark performance and identify optimization opportunities:

```bash
# Command-line benchmarking script
python scripts/run_benchmarks.py --records 5000 --complexity complex
python scripts/run_benchmarks.py --mode streaming
python scripts/run_benchmarks.py --strategy memory

# Pytest benchmarks
pytest tests/benchmarks/
```

The benchmarking tools help evaluate:

- Overall performance with different configurations
- Performance impact of different processing modes and strategies
- Memory usage characteristics
- Component-level performance metrics

For more details, see the [Benchmarking Guide](https://scottdraper8.github.io/transmog/benchmarking.html).

## Deterministic ID Generation

Configure deterministic IDs using the new configuration system:

```python
# Configure deterministic IDs based on specific fields
config = tm.TransmogConfig.with_deterministic_ids({
    "": "id",                     # Root level uses "id" field
    "user_orders": "id"           # Order records use "id" field
})

# Or use a custom ID generation strategy
def custom_id_strategy(record):
    return f"CUSTOM-{record['id']}"

config = tm.TransmogConfig.with_custom_id_generation(custom_id_strategy)

# Use the configuration
processor = tm.Processor(config=config)
result = processor.process(data)
```

See the [deterministic IDs guide](https://scottdraper8.github.io/transmog/deterministic-ids.html) for more information.

## Output Format Options

Transmog provides three main categories of output formats:

1. **Native Data Structures** - Python objects like dictionaries and PyArrow Tables

   ```python
   result.to_dict()              # Python dictionaries
   result.to_json_objects()      # JSON-serializable Python objects
   result.to_pyarrow_tables()    # PyArrow Tables
   ```

2. **Bytes Serialization** - Raw bytes in JSON, CSV, or Parquet format

   ```python
   result.to_json_bytes()        # JSON bytes
   result.to_csv_bytes()         # CSV bytes
   result.to_parquet_bytes()     # Parquet bytes
   ```

3. **File Output** - Direct writing to files in different formats

   ```python
   result.write_all_json()       # Write to JSON files
   result.write_all_csv()        # Write to CSV files
   result.write_all_parquet()    # Write to Parquet files
   result.stream_to_parquet()    # Stream to Parquet files with optimal memory usage
   ```

4. **Streaming Output** - Direct streaming for memory-efficient processing

   ```python
   # Process and stream data directly to Parquet files
   from transmog.io import create_streaming_writer

   writer = create_streaming_writer(
       "parquet",
       destination="output_dir",
       compression="snappy",
       row_group_size=10000
   )

   with writer:
       # Process and write data in batches
       for batch in data_batches:
           batch_result = processor.process_batch(batch)
           writer.write_main_records(batch_result.get_main_table())

           for table_name in batch_result.get_table_names():
               writer.initialize_child_table(table_name)
               writer.write_child_records(table_name, batch_result.get_child_table(table_name))
   ```

## Documentation

- [Installation Guide](https://scottdraper8.github.io/transmog/installation.html)
- [Getting Started](https://scottdraper8.github.io/transmog/getting-started.html)
- [Configuration Guide](https://scottdraper8.github.io/transmog/configuration.html)
- [Output Formats](https://scottdraper8.github.io/transmog/output-formats.html)
- [In-Memory Processing](https://scottdraper8.github.io/transmog/in-memory-processing.html)
- [Deterministic IDs](https://scottdraper8.github.io/transmog/deterministic-ids.html)
- [API Reference](https://scottdraper8.github.io/transmog/api/index.html)
- [Examples](examples/README.md)

## Use Cases

- Data ETL pipelines
- API response processing
- JSON/CSV conversion
- Preparing nested data for tabular analysis
- Data normalization and standardization
- Integration with data processing frameworks
- In-memory data transformation
- Cloud-based serverless processing
- Incremental data processing with consistent IDs

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please make sure to update tests as appropriate.

## License

Distributed under the MIT License. See `LICENSE` for more information.
