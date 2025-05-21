---
title: JSON Handling and Transformation
---

# JSON Handling and Transformation

> **API Reference**: For detailed API documentation, see the [Processor API Reference](../../api/processor.md).

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

processor = tm.Processor()
result = processor.process(data, entity_name="record")

# Process a list of dictionaries
data_list = [
    {"id": 1, "name": "First"},
    {"id": 2, "name": "Second"}
]

result = processor.process(data_list, entity_name="records")
```

### From Files

Transmog uses a unified approach to process files with automatic format detection:

```python
import transmog as tm

processor = tm.Processor()

# Process a JSON file
result = processor.process_file("data.json", entity_name="records")

# Process a JSONL (line-delimited JSON) file
# Format is automatically detected based on file extension
result = processor.process_file("data.jsonl", entity_name="records")
```

### From Strings or Bytes

```python
import transmog as tm
import json

# Process JSON string
json_string = '{"id": 123, "name": "Example"}'
processor = tm.Processor()
result = processor.process(json_string, entity_name="record")

# Process JSON bytes
json_bytes = b'{"id": 123, "name": "Example"}'
result = processor.process(json_bytes, entity_name="record")
```

## File Processing Options

When processing files, Transmog handles format detection automatically:

```python
# Process a file with default settings
result = processor.process_file("data.json", entity_name="records")

# Process and convert to a specific format
result = processor.process_file_to_format(
    "data.json",
    entity_name="records",
    output_format="csv",
    output_path="output_dir"
)

# Process in memory-efficient chunks
result = processor.process_chunked(
    "large_data.jsonl",
    entity_name="records",
    chunk_size=1000
)
```

## Stream Processing

For large files, you can use streaming for memory efficiency:

```python
# Stream process a file directly to output format
processor.stream_process_file(
    "large_data.json",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir"
)
```

## Working with Results

The result of processing JSON data is a `ProcessingResult` object:

```python
# Get the main table (flattened records)
main_table = result.get_main_table()

# Get child tables (extracted arrays)
child_tables = result.get_child_tables()

# Convert to different formats
result.write_all_json("output_dir/json")
result.write_all_csv("output_dir/csv")
result.write_all_parquet("output_dir/parquet")
```

## Part 2: Customizing JSON Transformation

Transmog provides several ways to customize how JSON data is processed:

### Configuration-Based Transformation

The primary way to customize JSON transformation is through configuration:

```python
import transmog as tm

# Create a configuration with custom options
config = (
    tm.TransmogConfig.default()
    .with_processing(
        cast_to_string=True,      # Convert values to strings
        include_empty=False,      # Exclude empty values
        skip_null=True,           # Skip null values
        visit_arrays=True         # Process arrays as separate tables
    )
    .with_naming(
        separator=".",            # Use dots as separators
        deep_nesting_threshold=4,     # Handle deep nesting
        max_field_component_length=5  # Limit component length
    )
)

# Create processor with this configuration
processor = tm.Processor(config=config)

# Process data with the configuration
result = processor.process(data, entity_name="records")
```

### Type Handling

Control type conversion during processing:

```python
# Create a processor that preserves original data types
processor = tm.Processor(
    config=tm.TransmogConfig.default()
    .with_processing(cast_to_string=False)
)

# Process data (numeric values will remain as numbers)
result = processor.process(data, entity_name="records")

# Create a processor that converts everything to strings
processor = tm.Processor(
    config=tm.TransmogConfig.default()
    .with_processing(cast_to_string=True)
)

# Process data (all values will be converted to strings)
result = processor.process(data, entity_name="records")
```

### Naming and Path Transformation

Customize field naming during transformation:

```python
# Configure naming options
processor = tm.Processor(
    config=tm.TransmogConfig.default()
    .with_naming(
        separator="/",                   # Use slash as separator
        max_field_component_length=10,   # Limit component length
        deep_nesting_threshold=4         # Threshold for deep nesting
    )
)

# Process data with custom naming
result = processor.process(data, entity_name="records")
```

### Custom ID Generation

Generate custom IDs during processing:

```python
# Option 1: ID based on existing field
processor = tm.Processor.with_deterministic_ids("user_id")

# Option 2: Different ID fields for different tables
processor = tm.Processor.with_deterministic_ids({
    "": "id",                  # Main table uses "id" field
    "user_orders": "order_id"  # Orders table uses "order_id" field
})

# Option 3: Custom ID generation function
def generate_custom_id(record):
    # Create a custom ID based on record values
    if "id" in record:
        return f"CUSTOM-{record['id']}"
    elif "name" in record:
        return f"NAME-{record['name']}"
    else:
        return "UNKNOWN"

processor = tm.Processor.with_custom_id_generation(generate_custom_id)

# Process data with custom ID generation
result = processor.process(data, entity_name="records")
```

### Pre-Processing and Post-Processing

For more advanced transformations, you can pre-process data before passing it to Transmog or post-process results:

#### Pre-Processing Example

```python
import transmog as tm

# Pre-process data before transformation
def preprocess_data(data):
    # Add derived fields
    if "price" in data and "quantity" in data:
        data["total"] = data["price"] * data["quantity"]

    # Convert date strings to a standard format
    if "date" in data:
        from datetime import datetime
        try:
            date_obj = datetime.strptime(data["date"], "%m/%d/%Y")
            data["date"] = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return data

# Apply pre-processing to input data
preprocessed_data = preprocess_data(original_data)

# Process the pre-processed data
processor = tm.Processor()
result = processor.process(preprocessed_data, entity_name="records")
```

#### Post-Processing Example

```python
import transmog as tm

# Process data
processor = tm.Processor()
result = processor.process(data, entity_name="records")

# Post-process the results
main_table = result.get_main_table()
post_processed = []

for record in main_table:
    # Add derived fields
    if "user_age" in record:
        age = int(record["user_age"])
        record["age_group"] = "senior" if age >= 65 else "adult" if age >= 18 else "minor"

    # Format specific fields
    if "user_phone" in record:
        # Format phone number
        phone = record["user_phone"].replace("-", "").replace(" ", "")
        if len(phone) == 10:
            record["user_phone"] = f"({phone[0:3]}) {phone[3:6]}-{phone[6:]}"

    post_processed.append(record)

# Create a new result with post-processed records
from transmog.process.result import ProcessingResult
post_processed_result = ProcessingResult(
    main_table=post_processed,
    child_tables=result.get_child_tables(),
    entity_name=result.entity_name
)
```

## Best Practices

1. Use configuration options first for standard transformations
2. For small data, use `process()` with Python objects
3. For files, use `process_file()` with automatic format detection
4. For large files, use `process_chunked()` or `stream_process_file()`
5. For a complete processing and conversion pipeline, use `process_file_to_format()`
6. Pre-process data when you need to modify the structure before flattening
7. Post-process results when you need to derive fields or format data after flattening
8. For consistent IDs across processing runs, use deterministic ID generation
