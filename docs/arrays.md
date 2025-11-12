# Array Handling

Arrays are processed according to the `array_mode` configuration parameter.

## Array Modes

### SMART Mode (Default)

Processes arrays based on content type:

```python
import transmog as tm

data = {
    "product": {
        "name": "Laptop",
        "tags": ["electronics", "computers"],  # Simple array - kept as native
        "reviews": [  # Complex array - extracted to child table
            {"rating": 5, "comment": "Excellent"},
            {"rating": 4, "comment": "Good value"}
        ]
    }
}

result = tm.flatten(data, name="products")

print(result.main)
# [
#   {
#     'product_name': 'Laptop',
#     'product_tags': ['electronics', 'computers'],  # Native array
#     '_id': '...',
#     '_timestamp': '...'
#   }
# ]

print(result.tables["products_reviews"])
# [
#   {'rating': 5, 'comment': 'Excellent', '_parent_id': '...', '_id': '...'},
#   {'rating': 4, 'comment': 'Good value', '_parent_id': '...', '_id': '...'}
# ]
```

Simple arrays contain only primitive values (strings, numbers, booleans,
null). Complex arrays contain objects or nested structures.

### SEPARATE Mode

Extract all arrays into child tables:

```python
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="products", config=config)

# All arrays become separate tables
print(result.tables.keys())
# ['products_tags', 'products_reviews']
```

### INLINE Mode

Keep arrays as JSON strings:

```python
config = tm.TransmogConfig(array_mode=tm.ArrayMode.INLINE)
result = tm.flatten(data, name="products", config=config)

print(result.main)
# [
#   {
#     'product_name': 'Laptop',
#     'product_tags': '["electronics", "computers"]',
#     'product_reviews': '[{"rating": 5, ...}]',
#     '_id': '...'
#   }
# ]
```

### SKIP Mode

Ignore arrays entirely:

```python
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SKIP)
result = tm.flatten(data, name="products", config=config)

# Only scalar fields are included
print(result.main)
# [{'product_name': 'Laptop', '_id': '...'}]
```

## Nested Arrays

Arrays can contain objects with nested arrays, creating multi-level hierarchies:

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
        }
    ]
}

config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="company", config=config)

# Creates multi-level hierarchy
print(list(result.all_tables.keys()))
# ['company', 'company_departments', 'company_departments_teams']
```

Each level maintains parent-child relationships through `_parent_id` fields.
