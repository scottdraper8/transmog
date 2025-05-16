# Getting Started with Transmog

This guide covers basic usage of Transmog for transforming nested JSON into flattened formats.

## Installation

```bash
# Basic installation
pip install transmog

# With optional dependencies
pip install transmog[all]
```

## Basic Usage

Flattening nested JSON structures:

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

# Create a processor with default configuration
processor = tm.Processor()

# Process the data - entity_name is a required parameter
result = processor.process(data, entity_name="users")

# View the flattened data
print(result.get_main_table())
```

This outputs a list containing the flattened record:

```python
[
    {
        "__extract_id": "12345678-90ab-cdef-1234-567890abcdef",
        "__extract_datetime": "2023-01-01T12:00:00",
        "user_id": "1",
        "user_name": "John Doe",
        "user_contact_email": "john@example.com",
        "user_contact_phone": "555-1234"
    }
]
```

## Core Concepts

Transmog consists of:

- **Processor**: Entry point for transforming data
- **ProcessingResult**: Contains the output tables
- **Configuration**: System for customizing processing behavior
- **ID Generation**: Methods for record identification
- **Output formats**: Options to export transformed data

## Common Customizations

### Using Pre-configured Modes

Pre-configured modes for common use cases:

```python
# Memory-optimized configuration
processor = tm.Processor.memory_optimized()

# Performance-optimized configuration
processor = tm.Processor.performance_optimized()
```

### Custom Separators

Change the separator for nested keys:

```python
# Create a processor with custom separator
processor = tm.Processor(
    tm.TransmogConfig.default().with_naming(separator="/")
)
result = processor.process(data, entity_name="users")
print(result.get_main_table())
```

Output with forward slash separators:

```python
[
    {
        "__extract_id": "12345678-90ab-cdef-1234-567890abcdef",
        "__extract_datetime": "2023-01-01T12:00:00",
        "user/id": "1",
        "user/name": "John Doe",
        "user/contact/email": "john@example.com",
        "user/contact/phone": "555-1234"
    }
]
```

### Handling Arrays

By default, Transmog extracts arrays into separate child tables:

```python
processor = tm.Processor()
result = processor.process(data, entity_name="users")

# Get the main flattened data
main_data = result.get_main_table()
print("Main data:", main_data)

# Get the extracted child tables
table_names = result.get_table_names()
print("Tables:", table_names)

# Access the orders table
orders_table = result.get_child_table("users_user_orders")
print("Orders:", orders_table)
```

Output:

```python
Main data: [
    {
        "__extract_id": "12345678-90ab-cdef-1234-567890abcdef",
        "__extract_datetime": "2023-01-01T12:00:00",
        "user_id": "1",
        "user_name": "John Doe",
        "user_contact_email": "john@example.com",
        "user_contact_phone": "555-1234"
    }
]

Tables: ['users_user_orders']

Orders: [
    {
        "__extract_id": "23456789-0abc-def1-2345-6789abcdef01",
        "__parent_extract_id": "12345678-90ab-cdef-1234-567890abcdef",
        "__extract_datetime": "2023-01-01T12:00:00",
        "id": "101",
        "amount": "99.99",
        "__array_field": "orders",
        "__array_index": 0
    },
    {
        "__extract_id": "3456789a-bcde-f123-4567-89abcdef0123",
        "__parent_extract_id": "12345678-90ab-cdef-1234-567890abcdef",
        "__extract_datetime": "2023-01-01T12:00:00",
        "id": "102",
        "amount": "45.50",
        "__array_field": "orders",
        "__array_index": 1
    }
]
```

### Processing Options

Configuration system:

```python
# Create a custom configuration
config = (
    tm.TransmogConfig.default()
    .with_processing(
        cast_to_string=True,  # Convert all values to strings
        include_empty=False,  # Skip empty strings
        skip_null=True,       # Skip null values
        batch_size=500        # Process in batches of 500
    )
    .with_naming(
        separator="_"         # Use underscore as path separator
    )
)

# Use the configuration
processor = tm.Processor(config=config)
```

## Exporting Data

Output format options:

```python
# Get structured output
tables = result.to_dict()                # Get all tables as Python dictionaries
pa_tables = result.to_pyarrow_tables()   # Get as PyArrow Tables

# Bytes output for direct writing
json_bytes = result.to_json_bytes(indent=2)  # Get all tables as JSON bytes
csv_bytes = result.to_csv_bytes()        # Get all tables as CSV bytes
parquet_bytes = result.to_parquet_bytes()    # Get all tables as Parquet bytes

# Direct write to files
result.write_all_json("output_dir/json")
result.write_all_csv("output_dir/csv")
result.write_all_parquet("output_dir/parquet")
```

## Error Handling

Error recovery strategies:

```python
# Create a configuration with error handling
config = (
    tm.TransmogConfig.default()
    .with_error_handling(
        allow_malformed_data=True,
        recovery_strategy="skip",
        max_retries=3
    )
)

# Use the configuration
processor = tm.Processor(config=config)

# Process potentially problematic data
result = processor.process(data, entity_name="users")
```

## Deterministic IDs

Configure deterministic IDs for consistent processing:

```python
# Configure deterministic IDs based on specific fields
processor = tm.Processor.with_deterministic_ids({
    "": "id",                     # Root level uses "id" field
    "users_user_orders": "id"     # Order records use "id" field
})

# Process data with deterministic IDs
result = processor.process(data, entity_name="users")
```

## Processing Different Input Types

Transmog automatically selects the appropriate processing strategy based on the input type:

```python
# Process in-memory data (dictionary or list)
result = processor.process(data, entity_name="users")

# Process a file directly
result = processor.process("data.json", entity_name="users")

# Process data in chunks for large datasets
result = processor.process_chunked("large_data.jsonl", entity_name="users", chunk_size=1000)

# Process a CSV file
result = processor.process_csv("data.csv", entity_name="users", has_header=True)

# Process a batch of records
batch_data = [{"id": 1}, {"id": 2}, {"id": 3}]
result = processor.process_batch(batch_data, entity_name="users")
```

## Next Steps

Once you're comfortable with the basics, you can explore more advanced topics:

- [Data Transformation Guide](../processing/data-transformation.md)
- [JSON Handling and Transformation](../processing/json-handling.md)
- [Processing Overview](../processing/processing-overview.md)
- [File Processing Guide](../processing/file-processing.md)
- [Output Format Options](../output/output-formats.md)
