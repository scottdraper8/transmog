# Metadata Management

A key feature of Transmog is its comprehensive metadata management system. This system ensures proper
record identification, relationship tracking, and data lineage as complex nested structures are transformed
into flat, tabular formats.

## Metadata Fields

When processing data, Transmog automatically adds several metadata fields to each record:

| Field | Default Name | Description |
|-------|--------------|-------------|
| ID | `__extract_id` | Unique identifier for each record |
| Parent ID | `__parent_extract_id` | Reference to the parent record's ID |
| Timestamp | `__extract_datetime` | When the record was processed |
| Array Field | `__array_field` | Name of the original array field (for child tables) |
| Array Index | `__array_index` | Original index in the array (for child tables) |

These metadata fields serve several important purposes:

1. **Record Identification**: Each record gets a unique ID
2. **Relationship Tracking**: Child records reference their parent record's ID
3. **Data Lineage**: Timestamps and source information help track data origin
4. **Reconstruction**: Enables reconstructing the original nested structure if needed

## Basic Example

Consider this simple nested structure:

```python
data = {
    "id": 1,
    "name": "Example",
    "details": {
        "category": "test",
        "active": True
    },
    "items": [
        {"item_id": "A", "value": 10},
        {"item_id": "B", "value": 20}
    ]
}
```

After processing with Transmog:

```python
import transmog as tm

processor = tm.Processor()
result = processor.process(data, entity_name="example")

# Main table
print(result.get_main_table())

# Items table
print(result.get_child_table("example_items"))
```

The output would include:

Main table:

```python
[{
    "__extract_id": "abcd1234-5678-90ef-1234-567890abcdef",
    "__extract_datetime": "2023-06-15T10:30:00",
    "id": "1",
    "name": "Example",
    "details_category": "test",
    "details_active": "true"
}]
```

Items table:

```python
[{
    "__extract_id": "bcde2345-6789-01fg-2345-67890abcdef1",
    "__parent_extract_id": "abcd1234-5678-90ef-1234-567890abcdef",
    "__extract_datetime": "2023-06-15T10:30:00",
    "__array_field": "items",
    "__array_index": 0,
    "item_id": "A",
    "value": "10"
}, {
    "__extract_id": "cdef3456-789a-01gh-3456-7890abcdef12",
    "__parent_extract_id": "abcd1234-5678-90ef-1234-567890abcdef",
    "__extract_datetime": "2023-06-15T10:30:00",
    "__array_field": "items",
    "__array_index": 1,
    "item_id": "B",
    "value": "20"
}]
```

## Customizing Metadata Fields

You can customize the names of metadata fields to match your existing schema or data warehouse:

```python
import transmog as tm

# Create a configuration with custom metadata field names
config = (
    tm.TransmogConfig.default()
    .with_metadata(
        id_field="record_id",
        parent_field="parent_record_id",
        time_field="processed_at"
    )
)

processor = tm.Processor(config=config)
result = processor.process(data, entity_name="example")
```

With this configuration, the metadata fields would be:

- `record_id` instead of `__extract_id`
- `parent_record_id` instead of `__parent_extract_id`
- `processed_at` instead of `__extract_datetime`

## ID Generation

Transmog supports several strategies for generating record IDs:

### 1. Random UUIDs (Default)

By default, Transmog generates random UUIDs for each record:

```python
processor = tm.Processor()  # Uses random UUIDs by default
```

### 2. Deterministic IDs

For consistent IDs across processing runs, use deterministic ID generation based on field values:

```python
# Use "id" field for all tables
processor = tm.Processor.with_deterministic_ids("id")

# Or specify different fields for different tables
processor = tm.Processor.with_deterministic_ids({
    "": "id",                     # Root level uses "id" field
    "example_items": "item_id"    # Items table uses "item_id" field
})
```

### 3. Custom ID Generation

For advanced scenarios, implement a custom ID generation function:

```python
def custom_id_generator(record):
    # Generate an ID based on record contents
    if "id" in record:
        return f"REC-{record['id']}"
    elif "item_id" in record:
        return f"ITEM-{record['item_id']}"
    else:
        import uuid
        return str(uuid.uuid4())

processor = tm.Processor.with_custom_id_generation(custom_id_generator)
```

## Parent-Child Relationships

Transmog preserves parent-child relationships through the parent ID reference:

```text
Main Record
  │
  │ (__extract_id referenced by __parent_extract_id)
  │
  ├─► Child Record 1
  │
  └─► Child Record 2
```

These references create a directed graph that mirrors the original nested structure:

```python
# Access the main record
main_record = result.get_main_table()[0]
main_id = main_record["__extract_id"]

# Find child records that reference this parent
child_records = [
    r for r in result.get_child_table("example_items")
    if r["__parent_extract_id"] == main_id
]
```

## Custom Extraction Time

You can specify a custom extraction timestamp when processing data:

```python
from datetime import datetime

# Use a specific timestamp
extract_time = datetime(2023, 6, 15, 10, 30, 0)
result = processor.process(data, entity_name="example", extract_time=extract_time)
```

This is useful for:

- Preserving the original data extraction time
- Batch processing with consistent timestamps
- Data lineage tracking

## Multiple Nesting Levels

Transmog handles multiple levels of nesting by maintaining the parent-child chain:

```python
data = {
    "id": "root",
    "level1": [
        {
            "id": "L1A",
            "level2": [
                {"id": "L2A", "value": 100},
                {"id": "L2B", "value": 200}
            ]
        }
    ]
}

processor = tm.Processor()
result = processor.process(data, entity_name="nested")

# Access the tables
main_table = result.get_main_table()
level1_table = result.get_child_table("nested_level1")
level2_table = result.get_child_table("nested_level1_level2")

# Follow the chain of relationships
root_id = main_table[0]["__extract_id"]
level1_records = [r for r in level1_table if r["__parent_extract_id"] == root_id]
level1_id = level1_records[0]["__extract_id"]
level2_records = [r for r in level2_table if r["__parent_extract_id"] == level1_id]
```

## Using Metadata in Database Loading

The metadata fields are particularly useful when loading processed data into a database:

```sql
-- Example database schema
CREATE TABLE main (
    extract_id UUID PRIMARY KEY,
    extract_datetime TIMESTAMP,
    id INTEGER,
    name TEXT,
    details_category TEXT,
    details_active BOOLEAN
);

CREATE TABLE items (
    extract_id UUID PRIMARY KEY,
    parent_extract_id UUID REFERENCES main(extract_id),
    extract_datetime TIMESTAMP,
    array_field TEXT,
    array_index INTEGER,
    item_id TEXT,
    value INTEGER
);
```

The `__extract_id` and `__parent_extract_id` fields enable proper foreign key relationships,
maintaining the integrity of your data.

## Best Practices

1. **Keep Default Metadata Fields**: Unless you have a specific reason to change them, use the default field names.

2. **Use Deterministic IDs for Incremental Loading**: When processing data incrementally, use deterministic IDs.

3. **Preserve Metadata When Exporting**: When writing to files or databases, preserve all metadata fields.

4. **Include Timestamps for Data Lineage**: Use custom extract times to track when the original data was sourced.

5. **Store Array Information**: The `__array_field` and `__array_index` fields help reconstruct the original structure.

By leveraging Transmog's metadata system effectively, you can maintain data integrity while enjoying the
benefits of a flattened format.
