# Data Processing Flow

Transmog organizes data processing into a structured flow that ensures predictable and controllable transformations.
This guide explains how data flows through the system and the various stages available.

## Overview

When processing data with Transmog, your data goes through several well-defined stages:

1. **Input** - Data enters the system from various sources
2. **Preprocessing** - Initial standardization and validation
3. **Transforms** - Custom data modifications and enrichment
4. **Validation** - Schema-based validation checks
5. **Output** - Data is sent to its destination

```text
┌─────────┐     ┌──────────────┐     ┌────────────┐     ┌────────────┐     ┌────────┐
│  Input  │ ──▶ │ Preprocessing│ ──▶ │ Transforms │ ──▶ │ Validation │ ──▶ │ Output │
└─────────┘     └──────────────┘     └────────────┘     └────────────┘     └────────┘
```

## Creating a Processor

The main entry point for working with Transmog is the `Processor` class:

```python
import transmog as tm

# Create a basic processor
processor = tm.Processor()

# Process some data
result = processor.process({"name": "John Doe"}, entity_name="user")
```

## Processing Stages in Detail

### 1. Input Stage

Data can come from various sources:

```python
# Direct dictionary/JSON input
result = processor.process({"name": "John"}, entity_name="user")

# File input
result = processor.process_file("users.json", entity_name="user")

# Stream input
with open("users.json", "rb") as f:
    result = processor.process_stream(f, entity_name="user")
```

### 2. Preprocessing Stage

Preprocessing handles initial standardization and preparation:

```python
# Create processor with preprocessing options
processor = tm.Processor(
    preprocess_options={
        "lowercase_keys": True,       # Convert all keys to lowercase
        "strip_strings": True,        # Remove whitespace from string values
        "remove_empty": True,         # Remove empty/null values
        "date_format": "%Y-%m-%d"     # Format to use for date strings
    }
)

# Process data with preprocessing applied
result = processor.process({
    "USER_ID": "12345",
    "Name": " John Doe  ",
    "email": "  john@example.com",
    "notes": "",
    "joined_date": "2023-01-15"
})

# Result will have standardized data:
# {
#   "user_id": "12345",
#   "name": "John Doe",
#   "email": "john@example.com",
#   "joined_date": "2023-01-15"
# }
```

#### Available Preprocessing Options

- `lowercase_keys`: Convert all dictionary keys to lowercase
- `strip_strings`: Remove leading/trailing whitespace from strings
- `remove_empty`: Remove keys with empty values (None, "", [], {})
- `date_format`: Format for parsing date strings
- `replace_none`: Value to replace None values with (if not removing them)
- `flatten_lists`: Whether to flatten nested lists
- `max_depth`: Maximum depth for processing nested structures

### 3. Transform Stage

Transforms allow you to modify or enrich your data:

```python
def add_full_name(data):
    """Add a full_name field by combining first_name and last_name."""
    if "first_name" in data and "last_name" in data:
        data["full_name"] = f"{data['first_name']} {data['last_name']}"
    return data

# Create processor with the transform
processor = tm.Processor(
    transforms=[
        add_full_name
    ]
)

# Process data with the transform applied
result = processor.process({
    "user_id": "12345",
    "first_name": "John",
    "last_name": "Doe"
})

# Result will include the new field:
# {
#   "user_id": "12345",
#   "first_name": "John",
#   "last_name": "Doe",
#   "full_name": "John Doe"
# }
```

You can also apply targeted transforms to specific paths:

```python
def uppercase_name(value):
    """Convert name to uppercase."""
    return value.upper() if isinstance(value, str) else value

# Create processor with targeted transforms
processor = tm.Processor(
    path_transforms={
        "$.name": uppercase_name,
        "$.users[*].name": uppercase_name
    }
)

# Process data with the targeted transform
result = processor.process({
    "name": "John Doe",
    "users": [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
})

# Result will have uppercase names:
# {
#   "name": "JOHN DOE",
#   "users": [
#     {"id": 1, "name": "ALICE"},
#     {"id": 2, "name": "BOB"}
#   ]
# }
```

### 4. Validation Stage

Validation ensures your data meets expected requirements:

```python
# Define a schema
user_schema = {
    "id": {"type": "string", "required": True},
    "email": {"type": "string", "pattern": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"},
    "age": {"type": "integer", "min": 0}
}

# Create processor with schema validation
processor = tm.Processor(
    schema=user_schema,
    schema_options={
        "strict": True,          # Reject fields not in schema
        "fail_on_error": True    # Raise exception on validation failure
    }
)

# Process data with validation applied
try:
    result = processor.process({
        "id": "user123",
        "email": "invalid-email",
        "age": 30
    }, entity_name="user")
except tm.ValidationError as e:
    print(f"Validation failed: {e}")
```

### 5. Output Stage

The final processed data is returned as a result object:

```python
result = processor.process(data, entity_name="user")

# Access the processed data
processed_data = result.data

# Check for errors
if result.has_errors():
    for error in result.get_errors():
        print(f"Error: {error}")

# Get processing stats
stats = result.stats
print(f"Processing time: {stats.processing_time_ms}ms")
print(f"Input size: {stats.input_size} bytes")
print(f"Output size: {stats.output_size} bytes")
```

## Combining Multiple Stages

A typical Transmog processor will combine multiple stages:

```python
import transmog as tm

def calculate_total(data):
    """Calculate order total from items."""
    if "items" in data and isinstance(data["items"], list):
        data["total"] = sum(item.get("price", 0) * item.get("quantity", 0)
                           for item in data["items"])
    return data

# Define schema
order_schema = {
    "id": {"type": "string", "required": True},
    "customer_id": {"type": "string", "required": True},
    "items": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "product_id": {"type": "string", "required": True},
                "quantity": {"type": "integer", "min": 1, "required": True},
                "price": {"type": "number", "min": 0, "required": True}
            }
        }
    },
    "total": {"type": "number"}
}

# Create processor with all stages
processor = tm.Processor(
    preprocess_options={
        "lowercase_keys": True,
        "strip_strings": True,
        "remove_empty": True
    },
    transforms=[
        calculate_total
    ],
    schema=order_schema,
    schema_options={
        "coerce_types": True,
        "fail_on_error": True
    }
)

# Process an order
order_data = {
    "ID": "ORD-12345",
    "CUSTOMER_ID": "  CUST-789  ",
    "ITEMS": [
        {"PRODUCT_ID": "PROD-1", "QUANTITY": "2", "PRICE": 10.99},
        {"PRODUCT_ID": "PROD-2", "QUANTITY": "1", "PRICE": 24.99},
        {"notes": ""}  # This will be removed by remove_empty
    ]
}

result = processor.process(order_data, entity_name="order")

# Result will contain standardized, transformed, and validated data:
# {
#   "id": "ord-12345",
#   "customer_id": "cust-789",
#   "items": [
#     {"product_id": "prod-1", "quantity": 2, "price": 10.99},
#     {"product_id": "prod-2", "quantity": 1, "price": 24.99}
#   ],
#   "total": 46.97
# }
```

## Processing Collections

Transmog can process collections of data:

```python
# Process a list of users
users = [
    {"id": "user1", "name": "Alice"},
    {"id": "user2", "name": "Bob"},
    {"id": "user3", "name": "Charlie"}
]

processor = tm.Processor()
result = processor.process_collection(users, entity_name="users")

# Access results
processed_users = result.data
error_count = result.error_count
```

## Configuration and Reuse

Processors can be configured once and reused:

```python
# Create a reusable processor for user data
user_processor = tm.Processor(
    preprocess_options={"lowercase_keys": True},
    schema=user_schema
)

# Process multiple data sources with the same processor
result1 = user_processor.process(data1, entity_name="user")
result2 = user_processor.process_file("users.json", entity_name="users")
```

## Error Handling

Transmog provides flexible error handling:

```python
# Non-strict mode to collect errors instead of failing
processor = tm.Processor(
    schema=schema,
    schema_options={"fail_on_error": False}
)

result = processor.process(data, entity_name="user")

# Check for errors
if result.has_errors():
    print(f"Processed with {result.error_count} errors:")
    for error in result.get_errors():
        print(f"Field '{error.field}': {error.message}")
else:
    print("Processing successful!")
```

## Performance Considerations

- For large datasets, consider processing in batches
- Disable unnecessary stages (preprocessing or validation) when performance is critical
- Use targeted transforms (`path_transforms`) instead of full-data transforms when possible
- Profile your transform functions as they can become bottlenecks

## Best Practices

- Start with simple processors and add complexity as needed
- Define schemas to document your data structures and prevent bugs
- Use preprocessing to standardize input before applying custom logic
- Create reusable transform functions for common operations
- Test your processors with both valid and invalid data
- Use entity_name to make logs and errors more descriptive
