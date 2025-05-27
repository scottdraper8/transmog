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

By default, arrays are removed from the main table after being extracted. If you want to keep
the original arrays in the main table while still creating child tables, use the `keep_arrays` parameter:

```python
# Configure to keep arrays in main table after processing
processor = tm.Processor(
    tm.TransmogConfig.default().keep_arrays()
)
result = processor.process(data, entity_name="users")

# The main table will contain both flattened fields and the original arrays
main_data = result.get_main_table()
# The child tables are still created as before
```

### Processing Options

Configuration system for customizing processing behavior:

```python
# Create a processor with custom processing options
processor = tm.Processor(
    tm.TransmogConfig.default()
    .with_processing(
        cast_to_string=True,      # Convert values to strings
        include_empty=False,      # Exclude empty values
        skip_null=True,           # Skip null values
        visit_arrays=True         # Process arrays into child tables
    )
)
```

For detailed configuration options, see:

- [Configuration Guide](configuration.md) - Complete configuration system documentation
- [Array Handling Options](../examples/array_handling.md) - Detailed array processing options
- [Configuration API Reference](../../api/config.md) - Technical API details
