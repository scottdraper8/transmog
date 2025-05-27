---
title: Data Transformation
---

For API details, see [Core API](../../api/core.md).

# Data Transformation

This comprehensive guide explains how Transmog transforms nested JSON data into flat, tabular formats.
It covers flattening, array handling, and parent-child relationships.

## Part 1: Basic Flattening

By default, Transmog flattens nested structures by concatenating the keys at each level:

```python
import transmog as tm

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

### Custom Separators

By default, Transmog uses an underscore (`_`) as the separator between nested keys. You can customize
this with the `separator` parameter:

```python
# Use a forward slash as the separator
processor = tm.Processor(
    tm.TransmogConfig.default().with_naming(separator="/")
)
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

### Flattening Options

Transmog provides several options to control the flattening process:

#### Value Handling

Control how values are processed:

```python
processor = tm.Processor(
    tm.TransmogConfig.default().with_processing(
        cast_to_string=True,    # Convert all values to strings
        include_empty=False,    # Skip empty string values
        skip_null=True          # Skip null values
    )
)
```

#### ID Field Customization

Customize the field names for IDs:

```python
processor = tm.Processor(
    tm.TransmogConfig.default().with_metadata(
        id_field="record_id",              # Default: "__extract_id"
        parent_field="parent_record_id",   # Default: "__parent_extract_id"
        time_field="processed_at"          # Default: "__extract_datetime"
    )
)
```

## Part 2: Working with Arrays

Transmog offers several strategies for handling arrays in your nested data:

### Simple Array Flattening

By default, Transmog flattens simple arrays by including the array index in the path:

```python
import transmog as tm

# Sample data with arrays
data = {
    "user": {
        "id": 1,
        "name": "Jane Doe",
        "tags": ["customer", "premium", "active"]
    }
}

# Default behavior flattens simple arrays
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
    "user_name": "Jane Doe",
    "user_tags_0": "customer",
    "user_tags_1": "premium",
    "user_tags_2": "active"
}
```

### Arrays of Objects as Child Tables

Arrays of objects are extracted as child tables with parent references:

```python
data = {
    "user": {
        "id": 1,
        "name": "John Doe",
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
        "user_name": "John Doe"
    }
]

Table names: ["example_user_orders"]

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

### Complex Nested Arrays

Transmog handles multiple levels of nesting by creating a separate table for each array:

```python
data = {
    "id": "customer456",
    "orders": [
        {
            "order_id": "B001",
            "items": [
                {"product": "Widget", "quantity": 2},
                {"product": "Gadget", "quantity": 1}
            ]
        }
    ]
}

result = processor.process(data, entity_name="customer")

# Access the tables
main_table = result.get_main_table()
orders_table = result.get_child_table("customer_orders")
items_table = result.get_child_table("customer_orders_items")

# Each level links to its parent
print(f"Customer ID: {main_table[0]['id']}")
print(f"Customer extract ID: {main_table[0]['__extract_id']}")

for order in orders_table:
    print(f"Order {order['order_id']} has parent ID {order['__parent_id']}")

for item in items_table:
    print(f"Item {item['product']} has parent ID {item['__parent_id']}")
```

### Array Processing Options

Transmog provides several options for controlling how arrays are processed:

1. **Default Behavior**: Arrays are extracted into child tables and removed from the main table

   ```python
   processor = tm.Processor()
   ```

2. **Keep Arrays in Main Table**: Arrays are extracted into child tables but also kept in the main table

   ```python
   processor = tm.Processor(
       tm.TransmogConfig.default().keep_arrays()
   )
   ```

3. **Skip Array Processing**: Arrays are kept in the main table and not processed into child tables

   ```python
   processor = tm.Processor(
       tm.TransmogConfig.default().disable_arrays()
   )
   ```

For a complete guide to array handling options, see [Array Handling](array-handling.md).

## Part 3: Understanding Parent-Child Relationships

When processing nested data, Transmog:

1. Flattens the main entity (root level)
2. Extracts arrays into separate child tables
3. Preserves relationships between parent and child entities using consistent ID fields

### Relationship Preservation

Transmog automatically maintains relationships between tables through ID fields:

- Each record in the main table gets a unique `__extract_id` field
- Child records contain a `__parent_extract_id` field that references their parent's `__extract_id`
- Each child table is named based on the parent entity name and the array field's path

```python
import transmog as tm

data = {
    "id": "user123",
    "name": "John Doe",
    "orders": [
        {"order_id": "A001", "total": 99.99},
        {"order_id": "A002", "total": 149.99}
    ]
}

processor = tm.Processor()
result = processor.process(data, entity_name="customer")

# Get the main and child tables
main_table = result.get_main_table()
child_table = result.get_child_table("customer_orders")

print(f"Main table has {len(main_table)} records")
print(f"Orders table has {len(child_table)} records")

# Parent record
print(main_table[0])  # Contains __extract_id, id, name

# Child records - each links back to the parent
for order in child_table:
    print(f"Order {order['order_id']} belongs to parent {order['__parent_extract_id']}")
```

### ID Generation Strategies

Transmog offers several strategies for generating IDs:

#### Random UUID Strategy (default)

```python
processor = tm.Processor()  # Uses UUID strategy by default
result = processor.process(data, entity_name="customer")
```

#### Deterministic ID Strategy

This ensures the same data always produces the same IDs:

```python
# Configure deterministic IDs
processor = tm.Processor.with_deterministic_ids({
    "": "id",                     # Root level uses "id" field
    "customer_orders": "order_id"  # Order records use "order_id" field
})

result = processor.process(data, entity_name="customer")
```

#### Custom ID Strategy

You can implement your own ID generation strategy:

```python
# Define a custom ID generation function
def custom_id_strategy(record):
    if "id" in record:
        return f"CUSTOM-{record['id']}"
    elif "order_id" in record:
        return f"ORDER-{record['order_id']}"
    else:
        return "UNKNOWN"

# Configure the processor with custom ID generation
processor = tm.Processor.with_custom_id_generation(custom_id_strategy)
result = processor.process(data, entity_name="customer")
```

## Part 4: Advanced Techniques

### Working with Multiple Processing Results

When processing data in batches, you can combine the results:

```python
# Process data in batches
result1 = processor.process_batch(batch1, entity_name="customer")
result2 = processor.process_batch(batch2, entity_name="customer")

# Combine the results
combined_result = tm.ProcessingResult.combine([result1, result2])

# Access the combined data
all_customers = combined_result.get_main_table()
all_orders = combined_result.get_child_table("customer_orders")
```

### Memory Optimization

For large datasets, you can optimize for memory usage:

```python
# Create a memory-optimized processor
processor = tm.Processor.memory_optimized()

# Process large data
result = processor.process_chunked(large_data,
                                   entity_name="records",
                                   chunk_size=1000)
```

## Best Practices

1. **Naming Conventions**:
   - Always specify a meaningful `entity_name` when processing data
   - Choose descriptive field names to make the flattened output more readable

2. **ID Generation**:
   - For deterministic processing, use deterministic IDs with stable identifier fields
   - Customize ID field names to match your existing systems if needed

3. **Array Handling**:
   - Let Transmog extract arrays of objects into separate tables by default
   - For simple scalar arrays, they'll be flattened inline with indexed field names

4. **Processing Large Data**:
   - Use batch or chunked processing for large datasets
   - Consider streaming output formats for very large results
   - Combine results from multiple processing runs when needed

5. **Child Table Access**:
   - Use `get_child_table()` with the table name to access extracted arrays
   - Get all table names with `get_table_names()`

6. **Output Formats**:
   - Choose appropriate output formats based on your downstream needs
   - Use native formats (dict, JSON) for direct manipulation
   - Use file-based outputs (CSV, Parquet) for storage and sharing
