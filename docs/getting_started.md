# Getting Started

This guide provides everything needed to get up and running quickly with data transformation.

## What is Transmog?

Transmog transforms complex nested data structures into flat, tabular formats while preserving relationships between parent and child records. Perfect for:

- Converting JSON data for database storage
- Preparing API responses for analytics
- Normalizing document data for SQL queries
- ETL pipeline data transformation

## Installation

Install Transmog using pip:

```bash
pip install transmog
```

Verify the installation:

```python
import transmog as tm
print(tm.__version__)  # Should print "1.1.0"
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
    '_id': 'auto_generated_id'
}]
```

Employee table:
```python
[
    {
        'name': 'Alice',
        'role': 'Engineer',
        'salary': '95000',
        '_parent_id': 'auto_generated_id'
    },
    {
        'name': 'Bob',
        'role': 'Designer',
        'salary': '75000',
        '_parent_id': 'auto_generated_id'
    }
]
```

### How It Works

The transformation process:

1. **Flattens nested objects** - `location.city` becomes `location_city`
2. **Extracts arrays** - `employees` array becomes a separate table
3. **Preserves relationships** - Links parent and child records with IDs

### Working with Files

Process files directly:

```python
# Process a JSON file
result = tm.flatten_file("data.json", name="products")

# Save results as CSV
result.save("output", format="csv")

# Save results as JSON
result.save("output", format="json")
```

### Streaming Large Data

For large datasets that don't fit in memory:

```python
# Stream process directly to files
tm.flatten_stream(
    large_data,
    output_path="output/",
    name="large_dataset",
    format="parquet"
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

Control how arrays are processed:

```python
# Default: arrays become separate tables
result = tm.flatten(data, arrays="separate")

# Keep arrays as JSON strings in main table
result = tm.flatten(data, arrays="inline")

# Skip arrays entirely
result = tm.flatten(data, arrays="skip")
```

### Field Naming

Customize how nested fields are named:

```python
# Use dots instead of underscores
result = tm.flatten(data, separator=".")

# Simplify deeply nested paths
result = tm.flatten(data, nested_threshold=2)
```

### ID Management

Control identifier fields:

```python
# Use existing field as ID
result = tm.flatten(data, id_field="product_id")

# Custom parent ID field name
result = tm.flatten(data, parent_id_field="parent_ref")

# Add timestamp metadata
result = tm.flatten(data, add_timestamp=True)
```

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

Configure how errors are handled using the unified error handling system:

```python
# Raise errors (default) - stops on first error
result = tm.flatten(data, errors="raise")

# Skip problematic records - continues processing
result = tm.flatten(data, errors="skip")

# Warn about issues but continue - logs warnings
result = tm.flatten(data, errors="warn")
```

The error handling system provides consistent error messages with standardized templates and context information across all processing modules.

## Common Patterns

### JSON API Response Processing

```python
# API response with nested user data
api_response = {
    "users": [
        {
            "id": 1,
            "profile": {"name": "Alice", "email": "alice@example.com"},
            "preferences": {"theme": "dark", "notifications": True},
            "posts": [
                {"title": "Hello World", "likes": 10},
                {"title": "Python Tips", "likes": 25}
            ]
        }
    ]
}

result = tm.flatten(api_response["users"], name="users")
```

### Log File Processing

```python
# Process log entries
log_data = [
    {
        "timestamp": "2024-01-01T10:00:00Z",
        "level": "INFO",
        "source": {"service": "api", "version": "1.2.0"},
        "metadata": {"request_id": "abc123", "user_id": "user456"}
    }
]

result = tm.flatten(log_data, name="logs")
```

### Configuration Data Normalization

```python
# Application configuration
config = {
    "database": {
        "host": "localhost",
        "port": 5432,
        "credentials": {"username": "admin", "password": "secret"}
    },
    "features": {
        "feature_flags": ["new_ui", "beta_api"],
        "limits": {"max_users": 1000, "max_requests": 10000}
    }
}

result = tm.flatten(config, name="config")
```

## Next Steps

Understanding the basics:

1. **[User Guide](user_guide/file-processing.md)** - Comprehensive task-oriented guides
2. **[API Reference](api_reference/api.md)** - Complete function documentation
3. **[Developer Guide](developer_guide/extending.md)** - Advanced usage and customization

## Quick Reference

```python
import transmog as tm

# Basic usage
result = tm.flatten(data, name="table_name")

# File processing
result = tm.flatten_file("input.json", name="table_name")

# Streaming
tm.flatten_stream(data, "output/", name="table_name", format="parquet")

# Save results
result.save("output", format="csv")
result.save("output.json")  # Single file for simple data

# Access data
main_table = result.main
child_tables = result.tables
all_tables = result.all_tables
```
