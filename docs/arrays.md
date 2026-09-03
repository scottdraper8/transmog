# Array Handling

Arrays are processed according to the `array_mode` configuration parameter.

## Table Names

Child tables are named `{entity}_{array_field}`. The entity is the `name`
passed to `flatten()` or `flatten_stream()`. The array field is the key of
the array being extracted, not the full nested path.

```python
result = tm.flatten(
    {"name": "Laptop", "reviews": [{"rating": 5}]},
    name="products",
)
# Child table: products_reviews
```

Nested arrays under extracted objects use the nested field name only:

```python
result = tm.flatten(
    {"company": "TechCorp", "departments": [{"name": "Eng", "teams": [{"name": "FE"}]}]},
    name="company",
    config=tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE),
)
# Tables: company, company_departments, company_teams
# not company_departments_teams
```

## Array Modes

### SMART Mode (Default)

Processes arrays based on content type. Simple arrays (primitives only) stay
on the parent row as native lists. Complex arrays (objects or nested
structures) are extracted to a child table.

```python
import transmog as tm

data = {
    "name": "Laptop",
    "tags": ["electronics", "computers"],  # Simple array — kept as native
    "reviews": [  # Complex array — extracted to child table
        {"rating": 5, "comment": "Excellent"},
        {"rating": 4, "comment": "Good value"}
    ]
}

result = tm.flatten(data, name="products")

print(result.main)
# [
#   {
#     'name': 'Laptop',
#     'tags': ['electronics', 'computers'],
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

If an array mixes objects and primitives, the whole array is treated as
complex and extracted to a child table.

:::{tip}
**When to use SMART mode**

Default choice for most use cases. Balances data normalization
with simplicity by keeping simple lists inline while properly normalizing
complex nested data.
:::

### SEPARATE Mode

Extract all arrays into child tables. Primitive array items become rows with
a `value` column:

```python
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="products", config=config)

print(list(result.tables.keys()))
# ['products_tags', 'products_reviews']

print(result.tables["products_tags"])
# [
#   {'value': 'electronics', '_parent_id': '...', '_id': '...'},
#   {'value': 'computers', '_parent_id': '...', '_id': '...'}
# ]
```

:::{tip}
**When to use SEPARATE mode**

Choose SEPARATE when:

- Child records need to be queried independently
- Building a fully normalized relational schema
- Array items have their own identity or lifecycle
- Performing analytics that aggregate across array items
:::

### INLINE Mode

Keep arrays as JSON strings:

```python
config = tm.TransmogConfig(array_mode=tm.ArrayMode.INLINE)
result = tm.flatten(data, name="products", config=config)

print(result.main)
# [
#   {
#     'name': 'Laptop',
#     'tags': '["electronics", "computers"]',
#     'reviews': '[{"rating": 5, ...}]',
#     '_id': '...'
#   }
# ]
```

:::{tip}
**When to use INLINE mode**

Choose INLINE when:

- Arrays are treated as opaque blobs
- Downstream systems parse JSON natively
- Preserving exact array structure is important
- Minimizing table count is a priority
:::

### SKIP Mode

Ignore arrays entirely:

```python
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SKIP)
result = tm.flatten(data, name="products", config=config)

# Only scalar fields are included
print(result.main)
# [{'name': 'Laptop', '_id': '...'}]
```

:::{tip}
**When to use SKIP mode**

Choose SKIP when:

- Arrays are not relevant to the analysis
- Extracting only top-level scalar fields
- Reducing output size by excluding nested data
:::

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

print(list(result.all_tables.keys()))
# ['company', 'company_departments', 'company_teams']
```

Each level maintains parent-child relationships through `_parent_id` fields.
