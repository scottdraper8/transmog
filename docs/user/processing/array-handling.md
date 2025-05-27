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

# Process with default configuration
processor = tm.Processor()
result = processor.process(data, entity_name="record")

# Main table - arrays are removed
main_table = result.get_main_table()[0]
print("Main table fields:", list(main_table.keys()))
# Output: Main table fields: ['__extract_id', '__extract_datetime', 'id', 'name']

# Child table - contains the array items
child_table = result.get_child_table("record_items")
print("Child table has", len(child_table), "records")
# Output: Child table has 2 records
```

## Keeping Arrays in Main Table

If you want to keep the original arrays in the main table while still extracting them to child tables:

```python
# Create a configuration that keeps arrays
config = tm.TransmogConfig.default().keep_arrays()
processor = tm.Processor(config=config)
result = processor.process(data, entity_name="record")

# Main table - arrays are kept
main_table = result.get_main_table()[0]
print("Main table fields:", list(main_table.keys()))
# Output: Main table fields: ['__extract_id', '__extract_datetime', 'id', 'name', 'items']

# Child table - still contains the array items
child_table = result.get_child_table("record_items")
print("Child table has", len(child_table), "records")
# Output: Child table has 2 records
```

When `keep_arrays` is enabled:

1. Arrays are processed into child tables as usual
2. The original arrays are also kept in the main table as JSON strings
3. This allows accessing the data in both forms

## Disabling Array Processing

If you want to disable array processing completely:

```python
# Create a configuration that disables array processing
config = tm.TransmogConfig.default().disable_arrays()
processor = tm.Processor(config=config)
result = processor.process(data, entity_name="record")

# Main table - arrays are kept in original form
main_table = result.get_main_table()[0]
print("Main table fields:", list(main_table.keys()))
# Output: Main table fields: ['__extract_id', '__extract_datetime', 'id', 'name', 'items']

# No child tables are created
table_names = result.get_table_names()
print("Table names:", table_names)
# Output: Table names: ['main']
```

## Use Cases

- **Default behavior**: Best for relational database storage, normalizing the data
- **Keep arrays**: Useful when you need both the normalized child tables and the original arrays
- **Disable arrays**: Best when you want to keep the arrays in their original form

## Recommended Approach

Choose the approach based on your needs:

1. For maximum normalization and relational database compatibility, use the default behavior
2. For compatibility with systems that expect arrays, while still creating child tables, use `keep_arrays`
3. For document databases that support arrays natively, consider `disable_arrays`
