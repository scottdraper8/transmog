# Array Handling

This guide covers Transmog's array processing capabilities, including the four handling modes and
advanced array processing scenarios.

## Array Handling Overview

Transmog provides four modes for handling arrays in nested data:

| Mode | Description | Use Case |
|------|-------------|----------|
| `ArrayMode.SMART` | **Default**. Explode complex arrays, keep simple as native | Optimal for Parquet |
| `ArrayMode.SEPARATE` | Extract all arrays into child tables | Full relational analysis |
| `ArrayMode.INLINE` | Keep all arrays as JSON strings | Document storage |
| `ArrayMode.SKIP` | Ignore arrays completely | Focus on scalar data |

## Smart Mode (Default)

Smart mode intelligently determines the best way to handle each array based on its content:

- **Simple arrays** (containing only primitives like strings, numbers, booleans) are preserved as native arrays
- **Complex arrays** (containing objects, nested arrays, or mixed types) are exploded into child tables

This provides optimal performance and storage, especially for Parquet output.

### Basic Smart Mode Usage

```python
import transmog as tm

data = {
    "product": {
        "name": "Laptop",
        "tags": ["electronics", "computers", "portable"],  # Simple array
        "reviews": [  # Complex array
            {"rating": 5, "comment": "Excellent"},
            {"rating": 4, "comment": "Good value"}
        ]
    }
}

# Default behavior uses smart mode
result = tm.flatten(data, name="products")

print("Main table:", result.main)
# [
#   {
#     'product_name': 'Laptop',
#     'product_tags': ['electronics', 'computers', 'portable'],  # Native array!
#     '_id': 'generated_id',
#     '_timestamp': '2024-01-15T10:30:00'
#   }
# ]

print("Reviews table:", result.tables["products_reviews"])
# [
#   {'rating': 5, 'comment': 'Excellent', '_parent_id': 'generated_id', '_id': 'generated_id', '_timestamp': '2024-01-15T10:30:00'},
#   {'rating': 4, 'comment': 'Good value', '_parent_id': 'generated_id', '_id': 'generated_id', '_timestamp': '2024-01-15T10:30:00'}
# ]
```

### When to Use Smart Mode

Smart mode is ideal for:

- **Parquet output** - native arrays are efficiently stored and queried
- **Mixed data** - automatically handles both simple and complex arrays appropriately
- **Performance** - avoids unnecessary table creation for simple arrays
- **General use** - provides sensible defaults for most use cases

### Smart Mode with Parquet

```python
import transmog as tm

data = {
    "user_id": 123,
    "tags": ["premium", "verified"],  # Kept as native array
    "preferences": ["email", "sms"],  # Kept as native array
    "purchases": [  # Exploded to child table
        {"item": "Widget", "price": 19.99},
        {"item": "Gadget", "price": 29.99}
    ]
}

result = tm.flatten(data, name="users")

# Save to Parquet - native arrays are efficiently stored
result.save("users.parquet")

# Query in DuckDB or Polars can use native array operations
# SELECT user_id FROM users WHERE 'premium' IN tags
```

## Separate Tables Mode

### Basic Array Extraction

```python
import transmog as tm

data = {
    "product": {
        "name": "Laptop",
        "tags": ["electronics", "computers", "portable"],
        "reviews": [
            {"rating": 5, "comment": "Excellent"},
            {"rating": 4, "comment": "Good value"}
        ]
    }
}

# Configure to extract all arrays as separate tables
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="products", config=config)

print("Main table:", result.main)
# [{'product_name': 'Laptop', '_id': 'generated_id'}]

print("Tags table:", result.tables["products_tags"])
# [
#   {'value': 'electronics', '_parent_id': 'generated_id'},
#   {'value': 'computers', '_parent_id': 'generated_id'},
#   {'value': 'portable', '_parent_id': 'generated_id'}
# ]

print("Reviews table:", result.tables["products_reviews"])
# [
#   {'rating': 5, 'comment': 'Excellent', '_parent_id': 'generated_id', '_id': 'generated_id', '_timestamp': '2024-01-15T10:30:00'},
#   {'rating': 4, 'comment': 'Good value', '_parent_id': 'generated_id', '_id': 'generated_id', '_timestamp': '2024-01-15T10:30:00'}
# ]
```

### Nested Array Processing

Arrays can contain objects with their own nested arrays:

```python
data = {
    "company": "TechCorp",
    "departments": [
        {
            "name": "Engineering",
            "teams": [
                {"name": "Frontend", "size": 5},
                {"name": "Backend", "size": 8}
            ]
        },
        {
            "name": "Marketing",
            "teams": [
                {"name": "Digital", "size": 3}
            ]
        }
    ]
}

config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="company", config=config)

# Multiple levels of child tables
print("Tables created:", list(result.all_tables.keys()))
# ['company', 'company_departments', 'company_departments_teams']

# Department table
print("Departments:", result.tables["company_departments"])
# [
#   {'name': 'Engineering', '_parent_id': 'main_id', '_id': 'dept_1'},
#   {'name': 'Marketing', '_parent_id': 'main_id', '_id': 'dept_2'}
# ]

# Teams table (nested array)
print("Teams:", result.tables["company_departments_teams"])
# [
#   {'name': 'Frontend', 'size': 5, '_parent_id': 'dept_1'},
#   {'name': 'Backend', 'size': 8, '_parent_id': 'dept_1'},
#   {'name': 'Digital', 'size': 3, '_parent_id': 'dept_2'}
# ]
```

### Relationship Tracking

Parent-child relationships are preserved through ID fields:

```python
# Build relationship map
def build_relationship_map(result):
    relationships = {}

    # Map main records
    for record in result.main:
        relationships[record["_id"]] = {
            "record": record,
            "children": {}
        }

    # Map child tables
    for table_name, records in result.tables.items():
        for record in records:
            parent_id = record["_parent_id"]
            if parent_id in relationships:
                if table_name not in relationships[parent_id]["children"]:
                    relationships[parent_id]["children"][table_name] = []
                relationships[parent_id]["children"][table_name].append(record)

    return relationships

relationships = build_relationship_map(result)
```

## Inline Mode

### JSON String Preservation

```python
# Keep arrays as JSON strings in the main table
config = tm.TransmogConfig(array_mode=tm.ArrayMode.INLINE)
result = tm.flatten(data, name="products", config=config)

print("Main table with inline arrays:", result.main)
# [
#   {
#     'product_name': 'Laptop',
#     'product_tags': '["electronics", "computers", "portable"]',
#     'product_reviews': '[{"rating": 5, "comment": "Excellent"}, {"rating": 4, "comment": "Good value"}]',
#     '_id': 'generated_id'
#   }
# ]

print("Child tables:", result.tables)
# {} (empty - no child tables created)
```

### When to Use Inline Mode

Inline mode is useful when:

- Document-oriented storage is preferred
- Array relationships are not needed for analysis
- Minimizing table count is important
- Arrays will be processed by other tools

### Working with Inline Arrays

```python
import json

config = tm.TransmogConfig(array_mode=tm.ArrayMode.INLINE)
result = tm.flatten(data, name="products", config=config)

# Parse inline arrays when needed
for record in result.main:
    if "product_tags" in record:
        tags = json.loads(record["product_tags"])
        print(f"Product tags: {tags}")

    if "product_reviews" in record:
        reviews = json.loads(record["product_reviews"])
        avg_rating = sum(r["rating"] for r in reviews) / len(reviews)
        print(f"Average rating: {avg_rating}")
```

## Skip Mode

### Ignoring Arrays

```python
# Skip arrays entirely during processing
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SKIP)
result = tm.flatten(data, name="products", config=config)

print("Main table (arrays skipped):", result.main)
# [{'product_name': 'Laptop', '_id': 'generated_id'}]

print("Child tables:", result.tables)
# {} (empty - arrays were ignored)
```

### When to Use Skip Mode

Skip mode is useful when:

- Only scalar data is relevant
- Arrays contain unstructured or irrelevant data
- Simplifying data structure is the goal
- Array processing will be handled separately

## Advanced Array Scenarios

### Mixed Array Types

```python
data = {
    "record": {
        "scalar_values": [1, 2, 3, 4, 5],              # Simple values
        "object_array": [                               # Complex objects
            {"id": 1, "name": "Item A"},
            {"id": 2, "name": "Item B"}
        ],
        "mixed_array": [                                # Mixed types
            {"type": "object", "data": {"value": 10}},
            {"type": "string", "data": "text_value"},
            {"type": "number", "data": 42}
        ]
    }
}

config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="records", config=config)

# All arrays are extracted consistently
print("Scalar values table:", result.tables["records_scalar_values"])
# [
#   {'value': 1, '_parent_id': 'main_id'},
#   {'value': 2, '_parent_id': 'main_id'},
#   ...
# ]

print("Object array table:", result.tables["records_object_array"])
# [
#   {'id': 1, 'name': 'Item A', '_parent_id': 'main_id'},
#   {'id': 2, 'name': 'Item B', '_parent_id': 'main_id'}
# ]
```

### Empty and Null Arrays

```python
data = {
    "item": {
        "name": "Product",
        "empty_array": [],
        "null_array": None,
        "valid_array": ["item1", "item2"]
    }
}

config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="items", config=config)

# Empty and null arrays are handled gracefully
print("Tables created:", list(result.tables.keys()))
# ['items_valid_array'] (only non-empty arrays create tables)

# Control empty array handling with include_empty
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE, include_empty=True)
result = tm.flatten(data, name="items", config=config)
```

### Array Field Naming

Table names for arrays follow a predictable pattern:

```python
data = {
    "company": {
        "departments": [
            {
                "teams": [
                    {"members": ["Alice", "Bob"]}
                ]
            }
        ]
    }
}

config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="org", config=config)

# Table naming: {entity_name}_{array_path}
print("Table names:", list(result.tables.keys()))
# [
#   'org_departments',
#   'org_departments_teams',
#   'org_departments_teams_members'
# ]
```

## Performance Considerations

### Memory Usage by Mode

```python
# Separate mode: More tables, distributed memory usage
config_separate = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result_separate = tm.flatten(data, config=config_separate)
print(f"Tables: {len(result_separate.all_tables)}")

# Inline mode: Fewer tables, concentrated memory usage
config_inline = tm.TransmogConfig(array_mode=tm.ArrayMode.INLINE)
result_inline = tm.flatten(data, config=config_inline)
print(f"Tables: {len(result_inline.all_tables)}")  # Usually 1

# Skip mode: Minimal memory usage
config_skip = tm.TransmogConfig(array_mode=tm.ArrayMode.SKIP)
result_skip = tm.flatten(data, config=config_skip)
print(f"Tables: {len(result_skip.all_tables)}")    # Usually 1
```

### Large Array Processing

```python
# For large arrays, use streaming with separate mode
config = tm.TransmogConfig(
    array_mode=tm.ArrayMode.SEPARATE,
    batch_size=100,
    cache_size=1000
)
tm.flatten_stream(
    large_data_with_arrays,
    output_path="output/",
    name="large_dataset",
    output_format="parquet",
    config=config
)

# For very large arrays that don't need analysis, use inline
config = tm.TransmogConfig(
    array_mode=tm.ArrayMode.INLINE,
    batch_size=100,
    cache_size=1000
)
result = tm.flatten(data_with_huge_arrays, name="documents", config=config)
```

## Working with Array Results

### Analyzing Array Data

```python
def analyze_arrays(result):
    """Analyze array structure in results."""
    analysis = {}

    for table_name, records in result.tables.items():
        analysis[table_name] = {
            "record_count": len(records),
            "fields": list(records[0].keys()) if records else [],
            "parent_count": len(set(r["_parent_id"] for r in records))
        }

    return analysis

# Analyze the arrays
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(complex_data, name="analysis", config=config)
array_info = analyze_arrays(result)

for table, info in array_info.items():
    print(f"{table}: {info['record_count']} records, {info['parent_count']} parents")
```

### Reconstructing Arrays

```python
def reconstruct_arrays(result):
    """Reconstruct original array structure from separated tables."""
    # Start with main records
    reconstructed = {r["_id"]: dict(r) for r in result.main}

    # Add arrays back
    for table_name, records in result.tables.items():
        # Extract array field name from table name
        array_field = table_name.split("_", 1)[1]

        # Group records by parent
        for record in records:
            parent_id = record["_parent_id"]
            if parent_id in reconstructed:
                if array_field not in reconstructed[parent_id]:
                    reconstructed[parent_id][array_field] = []

                # Remove metadata fields for cleaner reconstruction
                clean_record = {k: v for k, v in record.items()
                              if not k.startswith("_")}
                reconstructed[parent_id][array_field].append(clean_record)

    return list(reconstructed.values())

# Reconstruct original structure
original_structure = reconstruct_arrays(result)
```

## Configuration Patterns

### Database-Optimized Arrays

```python
# Configuration for database loading
config = tm.TransmogConfig(
    array_mode=tm.ArrayMode.SEPARATE,
    id_field="id",
    skip_null=True
)
result = tm.flatten(data, name="entities", config=config)
```

### Document-Optimized Arrays

```python
# Configuration for document storage
config = tm.TransmogConfig(
    array_mode=tm.ArrayMode.INLINE,
    cast_to_string=False,
    include_empty=True
)
result = tm.flatten(data, name="documents", config=config)
```

### Analytics-Optimized Arrays

```python
# Configuration for data analysis
config = tm.TransmogConfig(
    array_mode=tm.ArrayMode.SEPARATE,
    cast_to_string=False,
    skip_null=False,
    time_field="_timestamp"
)
result = tm.flatten(data, name="analytics", config=config)
```

## Next Steps

- **[ID Management](id-management.md)** - Control record identification
- **[Output Formats](output-formats.md)** - Choose optimal output formats for array data
