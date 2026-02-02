# Getting Started

## Overview

Transmog transforms nested data structures into flat, tabular formats while
preserving relationships between parent and child records.

## Installation

```bash
pip install transmog              # Full install (CSV, Parquet, ORC, Avro output)
pip install transmog[minimal]     # CSV only (no pyarrow, fastavro, or cramjam)
```

## Quick Start

### Basic Data Transformation

Transform nested data with a single function call:

```python
import transmog as tm

# Sample nested data
data = {
    "company": "TechCorp",
    "location": {
        "city": "San Francisco",
        "country": "USA"
    },
    "employees": [
        {"name": "Alice", "role": "Engineer", "salary": 95000},
        {"name": "Bob", "role": "Designer", "salary": 75000}
    ]
}

# Transform the data
result = tm.flatten(data, name="companies")

# Explore the results
print("Main table:")
print(result.main)

print("\nEmployee table:")
print(result.tables["companies_employees"])
```

**Output:**

Main table:

```python
[{
    'company': 'TechCorp',
    'location_city': 'San Francisco',
    'location_country': 'USA',
    '_id': 'auto_generated_id',
    '_timestamp': '2025-01-15 10:30:00.123456'
}]
```

The `_timestamp` field uses a UTC timestamp in `YYYY-MM-DD HH:MM:SS.ssssss` format.

:::{note}
Timestamp tracking can be disabled by setting `time_field=None` in
`TransmogConfig`. See [Configuration](configuration.md) for details.
:::

Employee table:

```python
[
    {
        'name': 'Alice',
        'role': 'Engineer',
        'salary': 95000,
        '_parent_id': 'auto_generated_id',
        '_id': 'auto_generated_id',
        '_timestamp': '2025-01-15 10:30:00.123456'
    },
    {
        'name': 'Bob',
        'role': 'Designer',
        'salary': 75000,
        '_parent_id': 'auto_generated_id',
        '_id': 'auto_generated_id',
        '_timestamp': '2025-01-15 10:30:00.123456'
    }
]
```

## Configuration Examples

```python
# Default: types preserved, optimized for analytics
result = tm.flatten(data)

# CSV: includes empty/null values
config = tm.TransmogConfig(include_nulls=True)
result = tm.flatten(data, config=config)

# Memory: small batches (100)
config = tm.TransmogConfig(batch_size=100)
result = tm.flatten(data, config=config)

```

### Behavior

Default configuration:

- Flattens nested objects: `location.city` becomes `location_city`
- Keeps simple arrays (primitives) as native arrays
- Extracts complex arrays (objects) into separate tables
- Links parent and child records with generated IDs

### Working with Files

Process files directly:

```python
# Process a JSON file
result = tm.flatten("data.json", name="products")

# Process JSON Lines / NDJSON
result = tm.flatten("data.jsonl", name="logs")
result = tm.flatten("data.ndjson", name="logs")
```

:::{important}
JSON5 and HJSON formats require additional packages:
:::

```python
# Process JSON5 (with comments, trailing commas, etc.)
# Requires: pip install json5
result = tm.flatten("config.json5", name="settings")

# Process HJSON (human-friendly JSON)
# Requires: pip install hjson
result = tm.flatten("data.hjson", name="records")

# Save results as CSV
result.save("output", output_format="csv")

# Save results as Parquet
result.save("output", output_format="parquet")

# Save results as ORC
result.save("output", output_format="orc")
```

### Streaming Large Data

For large datasets that don't fit in memory:

```python
# Stream process directly to files
tm.flatten_stream(
    large_data,
    output_path="output/",
    name="large_dataset",
    output_format="parquet"
)
```

:::{tip}
Use `flatten_stream()` for datasets larger than available RAM. It processes
data in batches and writes directly to disk, using significantly less memory
than `flatten()`.
:::

## Functions

- `tm.flatten(data)` - Returns `FlattenResult` object with data in memory
- `tm.flatten_stream(data, output_path)` - Writes directly to files

## Configuration

```python
# Array handling
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)

# ID generation
config = tm.TransmogConfig(id_generation="natural", id_field="product_id")
config = tm.TransmogConfig(id_generation="hash")
config = tm.TransmogConfig(id_generation=["user_id", "date"])
```

See [Array Handling](arrays.md) and [ID Management](ids.md) for details.

## Results

```python
result = tm.flatten(data, name="products")

# Access main table
main_data = result.main

# Access specific child table
reviews = result.tables["products_reviews"]

# Get all tables including main
all_tables = result.all_tables

# Table information
print(f"Tables: {list(result.all_tables.keys())}")
print(f"Main table records: {len(result.main)}")

# Access main table records
for record in result.main:
    print(record)
```

## Error Handling

Errors are raised as exceptions. See [Error Handling](errors.md) for details.

## Reference

```python
result = tm.flatten(data, name="table_name")
result = tm.flatten("input.json", name="table_name")
tm.flatten_stream(data, "output/", name="table_name", output_format="parquet")

result.save("output", output_format="csv")
result.save("output.csv")

main_table = result.main
child_tables = result.tables
all_tables = result.all_tables
```
