# Transmogrify

[![PyPI version](https://img.shields.io/pypi/v/transmogrify.svg)](https://pypi.org/project/transmogrify/)
[![Python versions](https://img.shields.io/pypi/pyversions/transmogrify.svg)](https://pypi.org/project/transmogrify/)
[![License](https://img.shields.io/github/license/scottdraper8/transmogrify.svg)](https://github.com/scottdraper8/transmogrify/blob/main/LICENSE)

A Python library for transforming complex nested JSON data into flat, structured formats.

## Features

- Flatten deeply nested JSON/dict structures with customizable delimiter options
- Transform values during processing with custom functions
- Native Formats: output to PyArrow Tables, Python dictionaries, or JSON objects
- Bytes Output: serialize directly to Parquet, CSV, or JSON bytes
- File Export: write to various file formats (JSON, CSV, Parquet)
- Recover from errors in malformed data with customizable strategies
- Optimize for performance with optional dependencies
- Stream large datasets efficiently
- Deterministic ID generation for data consistency across processing runs

## Installation

```bash
pip install transmogrify
```

For minimal installation without optional dependencies:

```bash
pip install transmogrify[minimal]
```

For development installation:

```bash
pip install transmogrify[dev]
```

See the [installation guide](docs/installation.md) for more details.

## Quick Example

```python
import transmogrify as tm

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

## Deterministic ID Generation

Transmogrify can now ensure consistent IDs for records across multiple processing runs:

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

Transmogrify provides three main categories of output formats:

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