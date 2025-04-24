# Flattening Nested Data

Transmogrify's core functionality is flattening nested data structures into tabular formats.

## Basic Flattening

By default, Transmogrify flattens nested structures by concatenating the keys at each level:

```python
import transmogrify as tm

data = {
    "user": {
        "id": 1,
        "name": "John Doe",
        "contact": {
            "email": "john@example.com",
            "phone": "555-1234"
        }
    }
}

processor = tm.Processor()
result = processor.process(data, entity_name="example")
print(result.get_main_table()[0])
```

Output:

```python
{
    "__extract_id": "12345678-90ab-cdef-1234-567890abcdef",
    "__extract_datetime": "2023-01-01T12:00:00",
    "user_id": "1",
    "user_name": "John Doe",
    "user_contact_email": "john@example.com",
    "user_contact_phone": "555-1234"
}
```

## Custom Separators

By default, Transmogrify uses an underscore (`_`) as the separator between nested keys. You can customize this with the `separator` parameter:

```python
# Use a forward slash as the separator
processor = tm.Processor(separator="/")
result = processor.process(data, entity_name="example")
print(result.get_main_table()[0])
```

This will output:

```python
{
    "__extract_id": "12345678-90ab-cdef-1234-567890abcdef",
    "__extract_datetime": "2023-01-01T12:00:00",
    "user/id": "1",
    "user/name": "John Doe",
    "user/contact/email": "john@example.com",
    "user/contact/phone": "555-1234"
}
```

## Handling Arrays

JSON arrays are handled by extracting them to separate tables:

### Array Elements in Separate Tables

Arrays of objects are extracted as child tables with parent references:

```python
data = {
    "user": {
        "id": 1,
        "name": "John Doe",
        "tags": ["customer", "premium", "active"],
        "orders": [
            {"id": 101, "amount": 99.99},
            {"id": 102, "amount": 45.50}
        ]
    }
}

processor = tm.Processor()
result = processor.process(data, entity_name="example")

# Get main table
main_table = result.get_main_table()
print("Main table:", main_table)

# Get child tables
table_names = result.get_table_names()
print("Table names:", table_names)

# Get orders table
orders = result.get_child_table("example_user_orders")
print("Orders table:", orders)
```

Output:

```python
Main table: [
    {
        "__extract_id": "12345678-90ab-cdef-1234-567890abcdef",
        "__extract_datetime": "2023-01-01T12:00:00",
        "user_id": "1",
        "user_name": "John Doe",
        "user_tags_0": "customer",
        "user_tags_1": "premium",
        "user_tags_2": "active"
    }
]

Table names: ["main", "example_user_orders"]

Orders table: [
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

## Flattening Options

Transmogrify provides several options to control the flattening process:

### Value Handling

Control how values are processed:

```python
processor = tm.Processor(
    cast_to_string=True,    # Convert all values to strings
    include_empty=False,    # Skip empty string values
    skip_null=True          # Skip null values
)
```

### ID Field Customization

Customize the field names for IDs:

```python
processor = tm.Processor(
    id_field="record_id",              # Default: "__extract_id"
    parent_field="parent_record_id",   # Default: "__parent_extract_id"
    time_field="processed_at"          # Default: "__extract_datetime"
)
```

### Memory Optimization

For large datasets, you can optimize for memory usage:

```python
processor = tm.Processor(
    optimize_for_memory=True,  # Prioritize memory efficiency
    batch_size=500             # Process in smaller batches
)
```

## Processing Modes

Transmogrify offers different processing modes for balancing memory usage and performance:

```python
from transmogrify.processor import ProcessingMode

# Standard mode (default)
processor = tm.Processor()

# Low memory mode
processor = tm.Processor(
    optimize_for_memory=True
)

# Using the ProcessingMode enum
from transmogrify.processor import ProcessingMode

result = processor._process_data(
    data=[record1, record2, record3],
    entity_name="example",
    memory_mode=ProcessingMode.LOW_MEMORY
)
```

## Converting to Other Formats

The flattened data can be easily converted to other formats:

```python
# Get data as Python dictionaries
tables = result.to_dict()

# Get data as JSON-compatible objects
json_objects = result.to_json_objects()

# Get data as PyArrow tables
pa_tables = result.to_pyarrow_tables()

# Output to CSV bytes
csv_bytes = result.to_csv_bytes()

# Output to JSON bytes
json_bytes = result.to_json_bytes(indent=2)

# Output to Parquet bytes
parquet_bytes = result.to_parquet_bytes()

# Write directly to files
result.write_all_json("output/json")
result.write_all_csv("output/csv")
result.write_all_parquet("output/parquet")
```

## Best Practices

- **Use meaningful entity names**: The entity name is used for table naming and helps with organization
- **Keep separator consistency**: Use the same separator across your application
- **Consider memory requirements**: For large datasets, use memory optimization and chunked processing
- **Use deterministic IDs**: For data that will be processed incrementally, configure deterministic ID generation
- **Customize output paths**: For file output, specify base directories that make sense for your workflow

## Next Steps

For more advanced information on transforming data:

- Learn about [Working with Arrays](arrays.md)
- Explore [Error Handling](error-handling.md)
- See [Concurrency](concurrency.md) for parallel processing 