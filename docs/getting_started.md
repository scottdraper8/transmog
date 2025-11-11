# Getting Started

This guide provides everything needed to get up and running quickly with data transformation.

## What is Transmog?

Transmog transforms complex nested data structures into flat, tabular formats while preserving
relationships between parent and child records. Use cases include:

- Converting JSON data for database storage
- Preparing API responses for analytics
- Normalizing document data for SQL queries
- ETL pipeline data transformation

## Installation

**Standard installation** (includes Parquet support):

```bash
pip install transmog
```

**Minimal installation** (CSV only):

```bash
pip install transmog[minimal]
```

The minimal installation excludes PyArrow (~50MB), useful for environments where only CSV output is needed.

Verify the installation:

```python
import transmog as tm
print(tm.__version__)
```

## 10 Minutes to Transmog

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

## Configuration Presets

```python
# Default: types preserved, optimized for analytics
result = tm.flatten(data)

# CSV: strings, includes empty/null values
config = tm.TransmogConfig.for_csv()
result = tm.flatten(data, config=config)

# Memory: small batches (100)
config = tm.TransmogConfig.for_memory()
result = tm.flatten(data, config=config)

# Error-tolerant: skip malformed records
config = tm.TransmogConfig.error_tolerant()
result = tm.flatten(data, config=config)
```

### How It Works

The transformation process uses **smart mode** by default:

1. **Flattens nested objects** - `location.city` becomes `location_city`
2. **Intelligently handles arrays**:
   - Simple arrays (primitives) are kept as native arrays
   - Complex arrays (objects) are extracted into separate tables
3. **Preserves relationships** - Links parent and child records with IDs

### Working with Files

Process files directly:

```python
# Process a JSON file
result = tm.flatten_file("data.json", name="products")

# Save results as CSV
result.save("output", output_format="csv")

# Save results as Parquet
result.save("output", output_format="parquet")
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

## Core Functions

Transmog provides three main functions:

| Function | Purpose | Use When |
|----------|---------|----------|
| `tm.flatten(data)` | Transform data in memory | Data fits in memory |
| `tm.flatten_file(path)` | Process files directly | Working with files |
| `tm.flatten_stream(data, output_path)` | Stream to files | Large datasets |

## Configuration Basics

### Array Handling

```python
# Default: smart mode - simple arrays inline, complex arrays separate
result = tm.flatten(data)  # Uses tm.ArrayMode.SMART by default

# All arrays become separate tables
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, config=config)
```

See [Array Handling](user_guide.md#array-handling) section in the User Guide for complete details.

### Field Naming

```python
# Use dots instead of underscores
config = tm.TransmogConfig(separator=".")
result = tm.flatten(data, config=config)
```

### ID Management

```python
# Use existing field as ID
config = tm.TransmogConfig(id_field="product_id")
result = tm.flatten(data, config=config)
```

See [ID Management](user_guide.md#id-management) section in the User Guide for complete details.

## Understanding the Results

The `FlattenResult` object provides easy access to transformed data:

```python
result = tm.flatten(data, name="products")

# Access main table
main_data = result.main

# Access specific child table
reviews = result.tables["products_reviews"]

# Get all tables including main
all_tables = result.all_tables

# Table information
info = result.table_info()
print(f"Tables: {list(result.keys())}")
print(f"Main table records: {len(result)}")

# Iterate over main table
for record in result:
    print(record)

# Check if table exists
if "products_tags" in result:
    print(result["products_tags"])
```

## Error Handling

```python
# Strict mode (default) - stops on first error
config = tm.TransmogConfig(recovery_mode=tm.RecoveryMode.STRICT)
result = tm.flatten(data, config=config)

# Skip mode - skips problematic records
config = tm.TransmogConfig.error_tolerant()
result = tm.flatten(data, config=config)
```

See [Error Handling](user_guide.md#error-handling) section in the User Guide for complete details.

## Next Steps

Understanding the basics:

1. **[User Guide](user_guide.md)** - Comprehensive guide with practical examples
2. **[API Reference](api_reference/api.md)** - Complete function documentation
3. **[Developer Guide](developer_guide/streaming.md)** - Advanced streaming and performance optimization

## Quick Reference

```python
import transmog as tm

# Basic usage
result = tm.flatten(data, name="table_name")

# File processing
result = tm.flatten_file("input.json", name="table_name")

# Streaming
tm.flatten_stream(data, "output/", name="table_name", output_format="parquet")

# Save results
result.save("output", output_format="csv")
result.save("output.csv")  # Single file for simple data

# Access data
main_table = result.main
child_tables = result.tables
all_tables = result.all_tables
```
