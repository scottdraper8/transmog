---
title: JSON Handling and Transformation
---

# JSON Handling and Transformation

> **API Reference**: For detailed API documentation, see the [Core API Reference](../../api/core.md).

This document provides a comprehensive guide to working with JSON data in Transmog, including processing
sources and customizing transformations.

## Part 1: Processing JSON Data

Transmog can process JSON data from various sources:

### From Python Objects

```python
import transmog as tm

# Process a dictionary
data = {
    "id": 123,
    "name": "Example",
    "items": [{"id": 1}, {"id": 2}]
}

result = tm.flatten(data, name="record")

# Process a list of dictionaries
data_list = [
    {"id": 1, "name": "First"},
    {"id": 2, "name": "Second"}
]

result = tm.flatten(data_list, name="records")
```

### From Files

Transmog uses a unified approach to process files with automatic format detection:

```python
import transmog as tm

# Process a JSON file
result = tm.flatten_file("data.json", name="records")

# Process a JSONL (line-delimited JSON) file
# Format is automatically detected based on file extension
result = tm.flatten_file("data.jsonl", name="records")
```

### From Strings or Bytes

```python
import transmog as tm
import json

# Process JSON string
json_string = '{"id": 123, "name": "Example"}'
data = json.loads(json_string)
result = tm.flatten(data, name="record")

# Process JSON bytes
json_bytes = b'{"id": 123, "name": "Example"}'
data = json.loads(json_bytes)
result = tm.flatten(data, name="record")
```

## File Processing Options

When processing files, Transmog handles format detection automatically:

```python
# Process a file with default settings
result = tm.flatten_file("data.json", name="records")

# Process and save to a specific format
result = tm.flatten_file("data.json", name="records")
result.save("output_dir", format="csv")

# Process in memory-efficient chunks
result = tm.flatten_file(
    "large_data.jsonl",
    name="records",
    chunk_size=1000
)
```

## Stream Processing

For large files, you can use streaming for memory efficiency:

```python
# Stream process a file directly to output format
tm.flatten_stream(
    file_path="large_data.json",
    name="records",
    output_path="output_dir",
    output_format="parquet"
)
```

## Working with Results

The result of processing JSON data is a `FlattenResult` object:

```python
# Get the main table (flattened records)
main_table = result.main

# Get child tables (extracted arrays)
child_tables = result.tables

# Convert to different formats
result.save("output_dir/json", format="json")
result.save("output_dir/csv", format="csv")
result.save("output_dir/parquet", format="parquet")

# Or let the format be detected from the file extension
result.save("output_dir/data.json")  # JSON format
result.save("output_dir/data.csv")   # CSV format
result.save("output_dir/data.parquet")  # Parquet format
```

## Part 2: Customizing JSON Transformation

Transmog provides several ways to customize how JSON data is processed:

### Configuration-Based Transformation

The primary way to customize JSON transformation is through parameters:

```python
import transmog as tm

# Process with custom options
result = tm.flatten(
    data,
    name="records",
    cast_to_string=True,      # Convert values to strings
    include_empty=False,      # Exclude empty values
    skip_null=True,           # Skip null values
    arrays="tables",          # Process arrays as separate tables
    separator=".",            # Use dots as separators
    deep_nesting_threshold=4, # Handle deep nesting
    max_field_length=5        # Limit component length
)
```

### Type Handling

Control type conversion during processing:

```python
# Process data with original data types preserved
result = tm.flatten(
    data,
    name="records",
    cast_to_string=False  # Preserve original types
)

# Process data with all values converted to strings
result = tm.flatten(
    data,
    name="records",
    cast_to_string=True  # Convert all values to strings
)
```

### Naming and Path Transformation

Customize field naming during transformation:

```python
# Configure naming options
result = tm.flatten(
    data,
    name="records",
    separator="/",                # Use slash as separator
    max_field_length=10,         # Limit component length
    deep_nesting_threshold=4     # Threshold for deep nesting
)
```

### Custom ID Generation

Generate custom IDs during processing:

```python
# Option 1: ID based on existing field
result = tm.flatten(
    data,
    name="users",
    id_field="user_id"  # Use user_id field as the ID
)

# Option 2: Different ID fields for different tables
result = tm.flatten(
    data,
    name="users",
    id_field={
        "": "id",                  # Main table uses "id" field
        "user_orders": "order_id"  # Orders table uses "order_id" field
    }
)

# Option 3: Custom ID generation function
def generate_custom_id(record):
    # Create a custom ID based on record values
    if "id" in record:
        return f"CUSTOM-{record['id']}"
    elif "name" in record:
        return f"NAME-{record['name']}"
    else:
        return "UNKNOWN"

result = tm.flatten(
    data,
    name="records",
    id_generator=generate_custom_id
)
```

### Pre-Processing and Post-Processing

For more advanced transformations, you can pre-process data before passing it to Transmog or post-process results:

#### Pre-Processing Example

```python
import transmog as tm

# Pre-process data before transformation
def preprocess_data(data_list):
    processed_data = []
    for item in data_list:
        # Add derived fields
        if "price" in item and "quantity" in item:
            item["total"] = item["price"] * item["quantity"]
        processed_data.append(item)
    return processed_data

# Apply pre-processing
original_data = [
    {"id": 1, "price": 10, "quantity": 2},
    {"id": 2, "price": 15, "quantity": 3}
]
processed_data = preprocess_data(original_data)

# Process the pre-processed data
result = tm.flatten(processed_data, name="orders")
```

#### Post-Processing Example

```python
import transmog as tm

# Process data
result = tm.flatten(data, name="products")

# Post-process the results
def postprocess_results(table):
    for record in table:
        # Add calculated fields
        if "price" in record:
            record["price_with_tax"] = float(record["price"]) * 1.2
    return table

# Apply post-processing to main table
processed_main = postprocess_results(result.main)

# Apply post-processing to child tables
processed_children = {}
for table_name, table_data in result.tables.items():
    processed_children[table_name] = postprocess_results(table_data)
```

## Advanced JSON Processing

### Handling Complex Nested Structures

Transmog automatically handles complex nested structures by flattening them into related tables:

```python
import transmog as tm

# Complex nested structure
data = {
    "company": "ACME Corp",
    "departments": [
        {
            "name": "Engineering",
            "employees": [
                {"id": 101, "name": "Alice"},
                {"id": 102, "name": "Bob"}
            ]
        },
        {
            "name": "Marketing",
            "employees": [
                {"id": 201, "name": "Charlie"}
            ]
        }
    ]
}

# Process with default settings
result = tm.flatten(data, name="organization")

# Access the hierarchical tables
print(result.main)  # Main company record
print(result.tables["organization_departments"])  # Departments
print(result.tables["organization_departments_employees"])  # Employees

# Save all tables
result.save("output/organization")
```

### Controlling Array Handling

Control how arrays are processed:

```python
# Option 1: Process arrays as separate tables (default)
result = tm.flatten(data, name="data", arrays="tables")

# Option 2: Keep arrays inline (as JSON strings)
result = tm.flatten(data, name="data", arrays="inline")

# Option 3: Expand simple arrays into columns
result = tm.flatten(data, name="data", arrays="columns")
```

### Handling Large JSON Files

For large JSON files, use chunked processing and streaming:

```python
# Process a large file in chunks
result = tm.flatten_file(
    "large_data.json",
    name="records",
    chunk_size=1000,  # Process 1000 records at a time
    low_memory=True   # Optimize for memory usage
)

# Stream directly to output files
tm.flatten_stream(
    file_path="very_large.json",
    name="records",
    output_path="output/data",
    output_format="parquet",
    chunk_size=5000
)
```

### Converting Between Formats

Convert JSON to other formats:

```python
# JSON to CSV
result = tm.flatten_file("data.json", name="data")
result.save("output/data.csv")  # Format detected from extension

# JSON to Parquet
result = tm.flatten_file("data.json", name="data")
result.save("output/data.parquet")
```
