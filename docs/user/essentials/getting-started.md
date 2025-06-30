# Getting Started with Transmog

This guide covers basic usage of Transmog for transforming nested JSON into flattened formats using the simplified API.

## Quick Start

The simplest way to flatten nested JSON structures:

```python
import transmog as tm

# Sample nested data
data = {
    "user": {
        "id": 1,
        "name": "John Doe",
        "contact": {
            "email": "john@example.com",
            "phone": "555-1234"
        },
        "orders": [
            {"id": 101, "amount": 99.99},
            {"id": 102, "amount": 45.50}
        ]
    }
}

# Flatten the data with one simple call
result = tm.flatten(data, name="users")

# Access the flattened data
print("Main table:", result.main)
print("Child tables:", list(result.tables.keys()))
```

This outputs:

```python
Main table: [
    {
        "_id": "12345678-90ab-cdef-1234-567890abcdef",
        "user_id": "1",
        "user_name": "John Doe",
        "user_contact_email": "john@example.com",
        "user_contact_phone": "555-1234"
    }
]

Child tables: ['users_user_orders']
```

## Core Concepts

Transmog consists of:

- **`flatten()` function**: Simple entry point for transforming data
- **`FlattenResult`**: Contains the output tables with intuitive access
- **Options**: Simple parameters for customizing behavior
- **Advanced API**: Full `Processor` class for complex scenarios
- **Multiple formats**: Easy export to JSON, CSV, Parquet

## Common Usage Patterns

### Save Results to Files

```python
# Flatten and save in one step
result = tm.flatten(data, name="users")

# Save to different formats
result.save("output.json")      # JSON format
result.save("output.csv")       # CSV format (creates multiple files for child tables)
result.save("output.parquet")   # Parquet format
```

### Process Files Directly

```python
# Process a JSON file directly
result = tm.flatten_file("input.json", name="users")
result.save("output.csv")
```

### Streaming for Large Datasets

```python
# For very large datasets, stream directly to files
tm.flatten_stream(
    large_data,
    output_dir="output/",
    name="users",
    format="parquet"
)
```

### Custom Options

```python
# Customize the flattening behavior
result = tm.flatten(
    data,
    name="users",
    natural_ids=True,        # Use natural IDs when possible
    add_timestamp=True,      # Add timestamp to records
    on_error="skip"         # Skip problematic records
)
```



## Understanding the Output

### Main Table Structure

The main table contains the flattened parent object:

```python
result = tm.flatten(data, name="users")
print(result.main[0])
```

```python
{
    "_id": "12345678-90ab-cdef-1234-567890abcdef",
    "user_id": "1",
    "user_name": "John Doe",
    "user_contact_email": "john@example.com",
    "user_contact_phone": "555-1234"
}
```

### Child Tables

Arrays are automatically extracted into separate child tables:

```python
# Access child tables
orders = result.tables["users_user_orders"]
print(orders)
```

```python
[
    {
        "_id": "23456789-0abc-def1-2345-6789abcdef01",
        "_parent_id": "12345678-90ab-cdef-1234-567890abcdef",
        "id": "101",
        "amount": "99.99",
        "_array_field": "orders",
        "_array_index": 0
    },
    {
        "_id": "3456789a-bcde-f123-4567-89abcdef0123",
        "_parent_id": "12345678-90ab-cdef-1234-567890abcdef",
        "id": "102",
        "amount": "45.50",
        "_array_field": "orders",
        "_array_index": 1
    }
]
```

### All Tables Access

```python
# Access all tables at once
for table_name, records in result.all_tables.items():
    print(f"{table_name}: {len(records)} records")
```

## Advanced Usage

For complex scenarios, you can still access the full `Processor` API:

```python
from transmog.process import Processor
from transmog.config import TransmogConfig

# Create custom configuration
config = (
    TransmogConfig.default()
    .with_naming(separator="/")
    .with_processing(cast_to_string=False)
    .with_metadata(add_timestamp=True)
)

# Use the advanced processor
processor = Processor(config)
result = processor.process(data, entity_name="users")

# Access results using the internal API
main_data = result.get_main_table()
child_tables = result.get_child_table("users_user_orders")
```



## Next Steps

- [Configuration Guide](configuration.md) - Learn about all available options
- [Processing Guide](../processing/processing-overview.md) - Understand data transformation
- [Array Handling](../processing/array-handling.md) - Master array processing
