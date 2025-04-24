# Basic Examples

This section contains examples of common usage patterns for Transmog.

## Simple Flattening

The most basic use case is flattening a nested structure:

```python
import transmog as tm

# Sample nested data
data = {
    "id": 123,
    "name": "Example",
    "details": {
        "category": "Test",
        "active": True,
        "metadata": {
            "created": "2023-01-01",
            "updated": "2023-01-02"
        }
    },
    "tags": ["tag1", "tag2", "tag3"]
}

# Create a processor with default settings
processor = tm.Processor()

# Process the data
result = processor.process(data, entity_name="example")

# Get the flattened data as a dictionary
flattened = result.to_dict()["main"][0]
print(flattened)
```

Output:
```
{
    "__extract_id": "abc123...",
    "id": "123",
    "name": "Example",
    "details_category": "Test",
    "details_active": "True",
    "details_metadata_created": "2023-01-01",
    "details_metadata_updated": "2023-01-02",
    "tags": "[\"tag1\", \"tag2\", \"tag3\"]"
}
```

## Customizing Separator

You can customize the separator used for flattened field names:

```python
# Use a dot as separator instead of underscore
processor = tm.Processor(separator=".")
result = processor.process(data, entity_name="example")
flattened = result.to_dict()["main"][0]
print(flattened)
```

Output:
```
{
    "__extract_id": "abc123...",
    "id": "123",
    "name": "Example",
    "details.category": "Test",
    "details.active": "True",
    "details.metadata.created": "2023-01-01",
    "details.metadata.updated": "2023-01-02",
    "tags": "[\"tag1\", \"tag2\", \"tag3\"]"
}
```

## Handling Arrays

When the input contains arrays, they can be extracted into child tables:

```python
# Sample data with arrays
data = {
    "order_id": "ORD-001",
    "customer": "John Doe",
    "items": [
        {"id": "ITEM-1", "name": "Product A", "price": 19.99},
        {"id": "ITEM-2", "name": "Product B", "price": 29.99}
    ]
}

processor = tm.Processor(separator=".")
result = processor.process(data, entity_name="orders")

# Main table
main_table = result.to_dict()["main"][0]
print("Main table:")
print(main_table)

# Child tables
child_tables = result.to_dict()
for table_name, records in child_tables.items():
    if table_name != "main":
        print(f"\nChild table '{table_name}':")
        for record in records:
            print(record)
```

Output:
```
Main table:
{
    "__extract_id": "def456...",
    "order_id": "ORD-001",
    "customer": "John Doe"
}

Child table 'items':
{
    "__extract_id": "ghi789...",
    "__parent_extract_id": "def456...",
    "id": "ITEM-1",
    "name": "Product A",
    "price": "19.99"
}
{
    "__extract_id": "jkl012...",
    "__parent_extract_id": "def456...",
    "id": "ITEM-2",
    "name": "Product B",
    "price": "29.99"
}
```

## Output Formats

The same data can be output in various formats:

```python
# Process data
processor = tm.Processor()
result = processor.process(data, entity_name="orders")

# Get as Python dict
dict_output = result.to_dict()

# Get as PyArrow table (requires PyArrow)
pa_tables = result.to_pyarrow_tables()

# Get as CSV bytes
csv_bytes = result.to_csv_bytes()

# Get as JSON bytes
json_bytes = result.to_json_bytes()

# Get as Parquet bytes (requires PyArrow)
parquet_bytes = result.to_parquet_bytes()

# Write to files
result.write_all_json("output/json")
result.write_all_csv("output/csv")
result.write_all_parquet("output/parquet")  # Requires PyArrow
```

## Processing Multiple Records

You can process multiple records in a batch:

```python
# Multiple records
records = [
    {"id": 1, "name": "Record 1"},
    {"id": 2, "name": "Record 2"},
    {"id": 3, "name": "Record 3"}
]

processor = tm.Processor()
result = processor.process_batch(records, entity_name="batch_example")

# Access the records
main_records = result.to_dict()["main"]
print(f"Processed {len(main_records)} records")
```

## Processing Files

You can process JSON or JSONL files:

```python
processor = tm.Processor()

# Process a JSON file
result = processor.process_file("data.json", entity_name="file_example")

# Process a JSON Lines file
result = processor.process_file("data.jsonl", entity_name="jsonl_example")

# Process a CSV file
result = processor.process_csv("data.csv", entity_name="csv_example")
```

## Customizing Value Handling

You can control how values are processed:

```python
# Keep original types instead of converting to strings
processor = tm.Processor(cast_to_string=False)

# Include empty strings in the output
processor = tm.Processor(include_empty=True)

# Include null values in the output
processor = tm.Processor(skip_null=False)
```

## Processing Large Datasets

For large datasets, use chunked processing:

```python
# Process a large file in chunks to reduce memory usage
processor = tm.Processor()
result = processor.process_chunked(
    "large_data.json",
    entity_name="large_example",
    chunk_size=1000  # Process 1000 records at a time
)
```

## Error Handling

You can configure how errors are handled:

```python
import transmog as tm
from transmog.recovery import SkipAndLogRecovery

# Create a processor with a recovery strategy
processor = tm.Processor(
    recovery_strategy=SkipAndLogRecovery()
)

# Process potentially problematic data
try:
    result = processor.process(problematic_data, entity_name="error_example")
    print("Processing completed with recovery")
except Exception as e:
    print(f"Processing failed: {e}")
```

## Customizing Field Names

You can customize the names of special fields:

```python
processor = tm.Processor(
    id_field="record_id",
    parent_field="parent_record_id",
    time_field="processed_at"
)
``` 