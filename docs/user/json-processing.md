# JSON Processing

Transmog provides comprehensive support for processing JSON data, with powerful features for handling
complex nested structures, arrays, and large datasets.

## Basic JSON Processing

The simplest way to process JSON data is using the `process` method:

```python
from transmog import Processor

processor = Processor()

# Process a single JSON object
json_data = {
    "id": "1234",
    "name": "Example Product",
    "price": 29.99,
    "attributes": {
        "color": "blue",
        "weight": "2kg"
    }
}

result = processor.process(json_data, entity_name="products")

# Access the processed data
main_table = result.get_main_table()
print(f"Processed {len(main_table)} records")
```

## Processing JSON Files

Transmog can directly process JSON files:

```python
# Process a JSON file
result = processor.process_json_file(
    "data.json",
    entity_name="customers"
)

# Access the processed data
main_table = result.get_main_table()
print(f"Processed {len(main_table)} records")
```

## Processing JSONL (JSON Lines) Files

For JSON Lines files (one JSON object per line):

```python
# Process a JSONL file
result = processor.process_jsonl_file(
    "data.jsonl",
    entity_name="logs",
    chunk_size=1000  # Process 1000 lines at a time
)
```

## Processing Nested JSON Structures

Transmog excels at handling deeply nested JSON data:

```python
# Complex nested JSON example
nested_json = {
    "order_id": "ORD-12345",
    "customer": {
        "id": "CUST-001",
        "name": "John Doe",
        "contact": {
            "email": "john@example.com",
            "phone": "555-123-4567"
        }
    },
    "items": [
        {
            "product_id": "PROD-001",
            "name": "Widget A",
            "price": 19.99,
            "quantity": 2
        },
        {
            "product_id": "PROD-002",
            "name": "Widget B",
            "price": 29.99,
            "quantity": 1
        }
    ],
    "shipping": {
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345"
        },
        "method": "express",
        "cost": 15.00
    }
}

result = processor.process(nested_json, entity_name="orders")

# Transmog automatically flattens nested structures
# and creates relationships between parent and child objects
```

## Memory-Optimized JSON Processing

For large JSON files, Transmog offers memory-optimized processing:

```python
# Create a processor optimized for memory usage
processor = Processor(optimize_for_memory=True)

# Process a large JSON file
result = processor.process_json_file(
    "large_dataset.json",
    entity_name="transactions",
    optimize_for_memory=True  # Override the processor's default setting
)
```

## Processing JSON Arrays

Transmog can handle JSON arrays of objects:

```python
# JSON array of objects
json_array = [
    {"id": "1", "name": "Product A", "price": 19.99},
    {"id": "2", "name": "Product B", "price": 29.99},
    {"id": "3", "name": "Product C", "price": 39.99}
]

result = processor.process(json_array, entity_name="products")
```

## Transforming JSON Data

You can transform JSON data during processing:

```python
def transform_json(data):
    # Add a discount field to each product
    if "price" in data:
        data["discounted_price"] = data["price"] * 0.9
    return data

result = processor.process(
    json_data,
    entity_name="products",
    transform_function=transform_json
)
```

## Filtering JSON Data

You can filter JSON data during processing:

```python
def filter_json(data):
    # Only include products with price > 20
    return data.get("price", 0) > 20

result = processor.process(
    json_array,
    entity_name="products",
    filter_function=filter_json
)
```

## Custom JSON Schemas

You can define a schema for validation and type conversion:

```python
schema = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "price": {"type": "number"},
        "in_stock": {"type": "boolean"},
        "tags": {"type": "array", "items": {"type": "string"}}
    },
    "required": ["id", "name", "price"]
}

result = processor.process(
    json_data,
    entity_name="products",
    schema=schema
)
```

## Working with JSON Results

The processed JSON data can be exported in various formats:

```python
# Process JSON data
result = processor.process(json_data, entity_name="products")

# Get the data as a list of dictionaries
data_dicts = result.get_main_table()

# Export as JSON
json_output = result.to_json()

# Export as CSV
csv_output = result.to_csv()
```

## Advanced JSON Processing

### Handling JSON Paths

You can extract specific data using JSON paths:

```python
# Extract specific fields using JSON path
result = processor.process_json_file(
    "data.json",
    entity_name="orders",
    path_mapping={
        "order_id": "$.id",
        "customer_name": "$.customer.name",
        "total_items": "$.items.length",
        "first_item_name": "$.items[0].name"
    }
)
```

### Processing Streaming JSON

For very large JSON datasets, you can process in streaming mode:

```python
# Process a large JSON file in streaming mode
with open("very_large.json", "r") as f:
    # Process the JSON data in chunks
    results = []
    for chunk in processor.process_json_stream(f, chunk_size=100):
        results.append(chunk)

    # Combine the results if needed
    from transmog.processing_result import ProcessingResult
    combined_result = ProcessingResult.combine(results)
```

## Performance Considerations

- For very large JSON files, use `optimize_for_memory=True`
- When processing arrays of objects, consider chunking to process in batches
- Use `process_jsonl_file` for large collections of JSON objects (one per line)
- For maximum performance with clean data, use `validate=False` to skip validation
