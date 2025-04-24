# Working with Arrays

This guide explains how to handle arrays effectively when processing nested JSON with Transmog.

## Array Handling Options

Transmog offers several strategies for handling arrays in your nested data:

1. **Default (flatten)**: Flattens arrays into indexed paths
2. **Extract**: Extracts arrays into separate tables
3. **Preserve**: Keeps arrays intact in the output

## Flattening Arrays (Default)

By default, Transmog flattens arrays by including the array index in the path:

```python
import transmog as tm

# Sample data with arrays
data = {
    "user": {
        "id": 1,
        "name": "Jane Doe",
        "tags": ["customer", "premium", "active"]
    }
}

# Default behavior flattens arrays
processor = tm.Processor()
result = processor.process(data)
print(result.to_dict())
```

Output:

```python
{
    "user.id": 1,
    "user.name": "Jane Doe",
    "user.tags.0": "customer",
    "user.tags.1": "premium",
    "user.tags.2": "active"
}
```

This works well for simple arrays and maintains the original structure in a flat format.

## Preserving Arrays

When you want to keep arrays intact (not flattened), use the `preserve_arrays` option:

```python
# Keep arrays intact
processor = tm.Processor(preserve_arrays=True)
result = processor.process(data)
print(result.to_dict())
```

Output:

```python
{
    "user.id": 1,
    "user.name": "Jane Doe",
    "user.tags": ["customer", "premium", "active"]
}
```

This is useful when you want to maintain arrays as proper collections in your output.

## Extracting Arrays

For more complex data with arrays of objects, the extract mode is particularly powerful:

```python
# Data with nested object arrays
data = {
    "order": {
        "id": "ORD-123",
        "customer_id": 1001,
        "items": [
            {"product_id": "P100", "name": "Widget", "quantity": 2, "price": 19.99},
            {"product_id": "P200", "name": "Gadget", "quantity": 1, "price": 29.99}
        ],
        "shipments": [
            {"tracking_id": "TRK-001", "carrier": "FedEx", "status": "delivered"}
        ]
    }
}

# Extract arrays into separate tables
processor = tm.Processor(array_handling="extract")
result = processor.process(data)

# Get the main data (without arrays)
main_data = result.to_dict()
print("Main data:")
print(main_data)

# Get the extracted arrays
arrays = result.get_arrays()
print("\nExtracted arrays:")
for array_name, array_data in arrays.items():
    print(f"\n{array_name}:")
    for item in array_data:
        print(f"  {item}")
```

Output:

```
Main data:
{
    "order.id": "ORD-123",
    "order.customer_id": 1001
}

Extracted arrays:

order.items:
  {'product_id': 'P100', 'name': 'Widget', 'quantity': 2, 'price': 19.99, '_parent_key': 'order', '_array_index': 0}
  {'product_id': 'P200', 'name': 'Gadget', 'quantity': 1, 'price': 29.99, '_parent_key': 'order', '_array_index': 1}

order.shipments:
  {'tracking_id': 'TRK-001', 'carrier': 'FedEx', 'status': 'delivered', '_parent_key': 'order', '_array_index': 0}
```

### Benefits of Array Extraction

Array extraction is especially useful when:

1. You want to create relational tables from nested JSON
2. Arrays contain complex objects that would be unwieldy when flattened
3. You plan to load the data into a relational database
4. The arrays might contain many items, making a fully flattened structure too wide

### Metadata in Extracted Arrays

When arrays are extracted, Transmog adds metadata fields to help maintain relationships:

- `_parent_key`: The path to the parent object containing the array
- `_array_index`: The original position in the array

These fields help maintain the relationship between the main data and extracted arrays.

## Working with Extracted Arrays

### Accessing Arrays

```python
# Get all arrays
all_arrays = result.get_arrays()

# Get a specific array by path
items = result.get_array("order.items")

# Check if an array exists
has_items = result.has_array("order.items")
```

### Exporting Arrays

All arrays can be exported separately:

```python
# Export main data and all arrays to JSON files
result.to_json_files("output/data")  # Creates data.json, data_order_items.json, etc.

# Export to CSV files
result.to_csv_files("output/data")  # Creates data.csv, data_order_items.csv, etc.

# Export to Parquet files
result.to_parquet_files("output/data")  # Creates data.parquet, etc.
```

### Exporting as PyArrow Tables

You can directly use the PyArrow tables for further processing:

```python
# Get the data as PyArrow tables
tables = result.to_pyarrow_tables()

# Access the main table and child tables
main_table = tables["main"]
order_items_table = tables["order_items"]

# Work with PyArrow table features
print(f"Main table: {main_table.num_rows} rows")
print(f"Order items: {order_items_table.num_rows} rows")

# Filter data using PyArrow compute functions
import pyarrow.compute as pc
mask = pc.greater(order_items_table["price"], 50.0)
expensive_items = order_items_table.filter(mask)
print(f"Expensive items: {expensive_items.num_rows}")

# Convert specific columns to Python lists for processing
order_ids = order_items_table.column("order_id").to_pylist()
prices = order_items_table.column("price").to_pylist()
```

## Array Processing Strategies

### Using Wildcards in Paths

You can process specific elements in arrays using wildcards:

```python
processor = tm.Processor(
    include_paths=[
        "order.id",
        "order.items.*.product_id",
        "order.items.*.price"
    ]
)
result = processor.process(data)
```

### Array-Specific Transformations

You can target array elements for transformation:

```python
def format_price(value, path=None, context=None):
    if path and path.endswith("price") and isinstance(value, (int, float)):
        return f"${value:.2f}"
    return value

processor = tm.Processor(
    array_handling="extract",
    value_processors=[format_price]
)
```

### Filtering Arrays

You can filter arrays during extraction:

```python
def filter_items(array_path, items, context=None):
    if array_path == "order.items":
        return [item for item in items if item["price"] > 20]
    return items

processor = tm.Processor(
    array_handling="extract",
    array_filters=[filter_items]
)
```

## Best Practices

1. **Choose the right array strategy** based on your data structure:
   - Use default flattening for simple arrays
   - Use preservation for arrays you want to keep intact
   - Use extraction for complex object arrays

2. **Consider the output format** when choosing an array strategy. Some formats handle arrays better than others.

3. **Use extraction for relational data** to maintain proper relationships between entities.

4. **Add IDs to your data** before processing to maintain relationships in the extracted arrays.

5. **Use array filters** to remove unnecessary items before processing.

## Next Steps

- Learn about [Error Handling](error-handling.md) when processing complex arrays
- Explore [Concurrency](concurrency.md) for processing large datasets with arrays
- See the [API Reference](../api/processor.md) for all array processing options 