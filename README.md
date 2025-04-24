# Transmog

[![PyPI version](https://img.shields.io/pypi/v/transmog.svg)](https://pypi.org/project/transmog/)
[![Python versions](https://img.shields.io/pypi/pyversions/transmog.svg)](https://pypi.org/project/transmog/)
[![License](https://img.shields.io/github/license/scottdraper8/transmog.svg)](https://github.com/scottdraper8/transmog/blob/main/LICENSE)

A Python library for transforming complex nested JSON data into flat, structured formats while preserving parent-child relationships.

## Features

- **Multiple Input Formats**: Process JSON, JSONL (line-delimited JSON), and CSV files
- **Flattening**: Flatten deeply nested structures with customizable delimiter options
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

See the [installation guide](docs/installation.md) for more details.

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

## Input Formats

Transmog supports multiple input formats:

```python
# Process standard JSON file
result = processor.process_file("data.json", entity_name="entity")

# Process JSONL (line-delimited JSON)
result = processor.process_file("data.jsonl", entity_name="entity")

# Process CSV file with options
result = processor.process_csv(
    "data.csv",
    entity_name="records",
    delimiter=",",
    has_header=True,
    infer_types=True
)
```

## Processing Large Datasets

For large datasets, use memory-optimized processing:

```python
# Memory-optimized processor
processor = tm.Processor(optimize_for_memory=True)

# Process a large file in chunks
result = processor.process_chunked(
    "large_data.jsonl",
    entity_name="records",
    chunk_size=1000  # Process 1000 records at a time
)
```

## Metadata Generation

Transmog automatically adds metadata to processed records:

- `__extract_id` - Unique identifier for each record
- `__parent_extract_id` - Reference to parent record (for child tables)
- `__extract_datetime` - Processing timestamp

## Deterministic ID Generation

Ensure consistent IDs across processing runs:

```python
# Configure deterministic IDs based on specific fields
processor = tm.Processor(
    deterministic_id_fields={
        "": "id",                     # Root level uses "id" field
        "user_orders": "id"           # Order records use "id" field
    }
)

# Process the data - IDs will be consistent across runs
result = processor.process(data)

# For complex ID generation logic, use a custom function
def custom_id_generator(record):
    # Generate custom ID based on record contents
    if "id" in record:
        return f"CUSTOM-{record['id']}"
    return str(uuid.uuid4())  # Fallback

processor = tm.Processor(id_generation_strategy=custom_id_generator)
```

See the [deterministic IDs guide](docs/user/deterministic-ids.md) for more information.

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
   ```

## Configurable Options

Transmog offers many configuration options:

```python
processor = tm.Processor(
    # Data formatting
    separator="_",                # Separator for flattened field names
    cast_to_string=True,         # Cast all values to strings
    include_empty=False,         # Include empty values
    skip_null=True,              # Skip null values
    
    # Performance
    optimize_for_memory=False,   # Prioritize memory efficiency over speed
    batch_size=1000,             # Default batch size for large datasets
    path_parts_optimization=True, # Optimize path handling for deep structures
    
    # Naming options
    abbreviate_table_names=True, # Abbreviate table names
    abbreviate_field_names=True, # Abbreviate field names
    
    # Error handling
    allow_malformed_data=False   # Attempt to recover from malformed data
)
```

## Documentation

- [Installation Guide](docs/installation.md)
- [Getting Started](docs/getting_started.md)
- [Output Formats](docs/user/output-formats.md)
- [In-Memory Processing](docs/user/in-memory-processing.md)
- [Deterministic IDs](docs/user/deterministic-ids.md)
- [API Reference](docs/api/index.md)
- [Examples](docs/examples/)

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