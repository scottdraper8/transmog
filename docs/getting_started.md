# Getting Started

## Overview

Transmog transforms nested data structures into flat, tabular formats while
preserving relationships between parent and child records.

## Installation

```bash
pip install transmog              # Full install (CSV, Parquet, ORC, Avro output)
```

:::{tip}
All output format dependencies (pyarrow, fastavro, cramjam) are included in the
default install. No extras are required for full functionality.
:::

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
    '_id': '8b596e4b-8c20-413b-a503-3fe15fe766e1',
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
        '_parent_id': '8b596e4b-8c20-413b-a503-3fe15fe766e1',
        '_id': 'c1a2b3d4-e5f6-7890-abcd-ef1234567890',
        '_timestamp': '2025-01-15 10:30:00.123456'
    },
    {
        'name': 'Bob',
        'role': 'Designer',
        'salary': 75000,
        '_parent_id': '8b596e4b-8c20-413b-a503-3fe15fe766e1',
        '_id': 'd2b3c4e5-f6a7-8901-bcde-f12345678901',
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

# Smaller streaming batches (values below 500 emit a UserWarning)
config = tm.TransmogConfig(batch_size=1000)
result = tm.flatten(data, config=config)

```

### Behavior

Default configuration:

- Flattens nested objects: `location.city` becomes `location_city`
- Keeps simple arrays (primitives) as native arrays
- Extracts complex arrays (objects) into separate tables
- Links parent and child records with generated IDs

(working-with-files)=

### Working with Files

Process files directly. A string is treated as a **file path only if that
path exists**. Otherwise it is parsed as a JSON string. A `pathlib.Path` that
does not exist raises `ValidationError: File not found`.

```python
from pathlib import Path

# Process a JSON file (path must exist)
result = tm.flatten("data.json", name="products")
result = tm.flatten(Path("data.json"), name="products")

# Process JSON Lines / NDJSON
result = tm.flatten("data.jsonl", name="logs")
result = tm.flatten("data.ndjson", name="logs")

# Iterator / generator of records
result = tm.flatten((row for row in records), name="events")
```

:::{note}
JSON5 and HJSON support is included in the default install.
:::

```python
# Process JSON5 (with comments, trailing commas, etc.)
result = tm.flatten("config.json5", name="settings")

# Process HJSON (human-friendly JSON)
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
than `flatten()`. Part files are merged into one file per table by default.
Pass `consolidate=False` to keep numbered part files.
:::

### Performance

JSON parsing uses [orjson](https://github.com/ijl/orjson) for faster
deserialization. When streaming large `.json` array files via
`flatten_stream()`, [ijson](https://github.com/ICRAR/ijson) provides
constant-memory parsing so the entire file is not loaded at once.

Both libraries are included in the default install and used automatically.

## Next steps

- [Array Handling](arrays.md) — SMART, SEPARATE, INLINE, SKIP
- [ID Management](ids.md) — random, natural, hash, composite keys
- [Streaming](streaming.md) — `flatten_stream()`, `consolidate`, `coerce_schema`
- [Error Handling](errors.md) — exception types and file-path vs JSON-string input
- [API Reference](api.md) — full signatures
