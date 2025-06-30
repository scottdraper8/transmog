---
title: Array Handling Options
---

# Array Handling Options

This guide demonstrates the different options for handling arrays in Transmog.

## Default Behavior: Extract Arrays into Child Tables

By default, Transmog extracts arrays into separate child tables and removes them from the main table:

```python
import transmog as tm

# Sample data with arrays
data = {
    "id": 1,
    "name": "Test Record",
    "items": [
        {"id": 101, "name": "Item 1"},
        {"id": 102, "name": "Item 2"}
    ]
}

# Process with default settings (arrays="tables")
result = tm.flatten(data, name="record")

# Main table - arrays are removed
main_table = result.main[0]
print("Main table fields:", list(main_table.keys()))
# Output: Main table fields: ['_id', '_datetime', 'id', 'name']

# Child table - contains the array items
child_table = result.tables["record_items"]
print("Child table has", len(child_table), "records")
# Output: Child table has 2 records
```

## Keeping Arrays in Main Table

If you want to keep the original arrays in the main table while still extracting them to child tables:

```python
# Process with arrays as both tables and inline
result = tm.flatten(data, name="record", arrays="both")

# Main table - arrays are kept
main_table = result.main[0]
print("Main table fields:", list(main_table.keys()))
# Output: Main table fields: ['_id', '_datetime', 'id', 'name', 'items']

# Child table - still contains the array items
child_table = result.tables["record_items"]
print("Child table has", len(child_table), "records")
# Output: Child table has 2 records
```

When `arrays="both"` is specified:

1. Arrays are processed into child tables as usual
2. The original arrays are also kept in the main table as JSON strings
3. This allows accessing the data in both forms

## Keeping Arrays Inline (No Child Tables)

If you want to disable array extraction completely:

```python
# Process with arrays kept inline
result = tm.flatten(data, name="record", arrays="inline")

# Main table - arrays are kept in original form
main_table = result.main[0]
print("Main table fields:", list(main_table.keys()))
# Output: Main table fields: ['_id', '_datetime', 'id', 'name', 'items']

# No child tables are created
table_names = list(result.tables.keys())
print("Table names:", table_names)
# Output: Table names: []
```

## Expanding Arrays into Columns

For simple arrays of primitive values, you can expand them into columns:

```python
# Sample data with simple arrays
data = {
    "id": 1,
    "name": "Test Record",
    "tags": ["important", "urgent", "review"]
}

# Process with arrays expanded to columns
result = tm.flatten(data, name="record", arrays="columns")

# Main table - arrays are expanded to columns
main_table = result.main[0]
print("Main table fields:", list(main_table.keys()))
# Output: Main table fields: ['_id', '_datetime', 'id', 'name', 'tags_0', 'tags_1', 'tags_2']
```

## Use Cases

- **arrays="tables"** (default): Best for relational database storage, normalizing the data
- **arrays="both"**: Useful when you need both the normalized child tables and the original arrays
- **arrays="inline"**: Best when you want to keep the arrays in their original form
- **arrays="columns"**: Useful for simple arrays of primitive values that should be expanded into columns

## Recommended Approach

Choose the approach based on your needs:

1. For maximum normalization and relational database compatibility, use `arrays="tables"` (default)
2. For compatibility with systems that expect arrays, while still creating child tables, use `arrays="both"`
3. For document databases that support arrays natively, consider `arrays="inline"`
4. For simple arrays of primitive values that should be expanded into columns, use `arrays="columns"`

## Advanced Array Handling

### Custom Array Processing

You can control which arrays are processed and which are kept inline:

```python
import transmog as tm

# Sample data with multiple arrays
data = {
    "id": 1,
    "name": "Product",
    "tags": ["electronics", "sale"],
    "variants": [
        {"id": "v1", "color": "red"},
        {"id": "v2", "color": "blue"}
    ]
}

# Process with specific arrays processed differently
result = tm.flatten(
    data, 
    name="product",
    arrays={
        "tags": "inline",     # Keep tags as inline array
        "variants": "tables"  # Process variants as child table
    }
)

# Check main table
main_table = result.main[0]
print("Main table has 'tags':", "tags" in main_table)  # True
print("Main table has 'variants':", "variants" in main_table)  # False

# Check child tables
print("Child tables:", list(result.tables.keys()))  # ['product_variants']
```

### Handling Deeply Nested Arrays

For deeply nested arrays, Transmog creates a hierarchy of tables:

```python
import transmog as tm

# Deeply nested data
data = {
    "company": "ACME Corp",
    "departments": [
        {
            "name": "Engineering",
            "teams": [
                {
                    "name": "Frontend",
                    "members": [{"name": "Alice"}, {"name": "Bob"}]
                },
                {
                    "name": "Backend",
                    "members": [{"name": "Charlie"}]
                }
            ]
        }
    ]
}

# Process with default settings
result = tm.flatten(data, name="org")

# Access the hierarchy of tables
print("Main table:", result.main[0])
print("Departments:", result.tables["org_departments"][0])
print("Teams:", result.tables["org_departments_teams"][0])
print("Members:", result.tables["org_departments_teams_members"][0])

# Save all tables
result.save("output/nested_arrays")
```
