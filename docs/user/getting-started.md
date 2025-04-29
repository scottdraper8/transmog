# Getting Started with Transmog

This guide will help you get started with Transmog to transform nested JSON into flattened formats.

## Installation

```bash
# Basic installation
pip install transmog

# With optional dependencies
pip install transmog[all]
```

## Basic Usage

A common use case for Transmog is flattening nested JSON structures:

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

# Process the data
result = processor.process(data, entity_name="test")

# View the flattened data
print(result.get_main_table())
```

This will output a list containing the flattened record:

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

Transmog is built around a few key concepts:

- **Processor**: The main entry point for transforming data
- **ProcessingResult**: Contains the output tables after processing
- **Configuration**: Flexible system for customizing processing behavior
- **ID Generation**: Methods for consistent record identification
- **Output formats**: Different ways to export your transformed data

## Common Customizations

### Using Pre-configured Modes

Transmog provides pre-configured modes for common use cases:

```python
# Memory-optimized configuration
config = tm.TransmogConfig.memory_optimized()
processor = tm.Processor(config=config)

# Performance-optimized configuration
config = tm.TransmogConfig.performance_optimized()
processor = tm.Processor(config=config)
```

### Custom Separators

Change the character used to separate nested keys:

```python
# Create a custom configuration with a forward slash separator
config = tm.TransmogConfig.default().with_naming(separator="/")
processor = tm.Processor(config=config)
result = processor.process(data, entity_name="test")
print(result.get_main_table())
```

Output would include paths separated by forward slashes:

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
result = processor.process(data, entity_name="test")

# Get the main flattened data
main_data = result.get_main_table()
print("Main data:", main_data)

# Get the extracted child tables
table_names = result.get_table_names()
print("Tables:", table_names)

# Access the orders table
orders_table = result.get_child_table("test_user_orders")
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

Tables: ['main', 'test_user_orders']

Orders: [
    {
        "__extract_id": "23456789-0abc-def1-2345-6789abcdef01",
        "__parent_extract_id": "12345678-90ab-cdef-1234-567890abcdef",
        "__extract_datetime": "2023-01-01T12:00:00",
        "id": "101",
        "amount": "99.99"
    },
    {
        "__extract_id": "3456789a-bcde-f123-4567-89abcdef0123",
        "__parent_extract_id": "12345678-90ab-cdef-1234-567890abcdef",
        "__extract_datetime": "2023-01-01T12:00:00",
        "id": "102",
        "amount": "45.50"
    }
]
```

### Processing Options

Transmog provides a flexible configuration system:

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

Transmog supports exporting to multiple formats:

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

Transmog provides error recovery strategies through configuration:

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
result = processor.process(data, entity_name="test")
```

## Deterministic IDs

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
result = processor.process(data, entity_name="test")
```

## Next Steps

- [Configuration Guide](configuration.md) - Learn about the configuration system
- [Flattening Options](flattening.md) - More details on flattening options
- [Working with Arrays](arrays.md) - Deep dive into array handling
- [Deterministic IDs](deterministic-ids.md) - Learn about ID generation options
- [Output Formats](output-formats.md) - Explore different output formats 