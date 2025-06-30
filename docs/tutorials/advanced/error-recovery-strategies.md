# Error Handling Strategies

This tutorial explores Transmog's error handling strategies for dealing with problematic data during transformation.

## Understanding Error Handling Strategies

Transmog offers three main approaches to error handling:

1. **Raise (default)** - Raises exceptions immediately, halting processing
2. **Skip** - Skips problematic records and continues processing
3. **Warn** - Logs warnings but continues processing with best-effort results

## When to Use Each Strategy

- **Raise**: Use during development or when data integrity is critical
- **Skip**: Use in production when processing should continue despite errors
- **Warn**: Use when you want to see all issues but still get results

## Configuring Error Strategies

Import the necessary components:

```python
import transmog as tm
```

### Raise Strategy Example (Default)

```python
# Sample data with errors
problematic_data = {
    "id": "customer123",
    "name": "John Smith",
    "orders": [
        {"id": "order1", "items": None},  # Problematic - items is None instead of array
        {"id": "order2", "items": [{"product": "Laptop"}]}
    ]
}

try:
    # This will raise an exception (default behavior)
    result = tm.flatten(
        data=problematic_data,
        name="customer"
    )
except Exception as e:
    print(f"Error encountered: {e}")
```

### Skip Strategy

```python
# Process data with skip strategy
result = tm.flatten(
    data=problematic_data,
    name="customer",
    error_handling="skip"  # Skip records with errors
)

# Check the results
print(f"Main table record count: {len(result.main)}")
if "customer_orders" in result.tables:
    print(f"Orders table record count: {len(result.tables['customer_orders'])}")
else:
    print("No orders table was created (all orders had errors)")
```

### Warn Strategy

```python
import warnings
import logging

# Configure logging to see warnings
logging.basicConfig(level=logging.WARNING)

# Process data with warn strategy
with warnings.catch_warnings(record=True) as w:
    result = tm.flatten(
        data=problematic_data,
        name="customer",
        error_handling="warn"  # Log warnings but continue
    )

    # Print captured warnings
    for warning in w:
        print(f"Warning: {warning.message}")

# Check the results (may contain partial data)
print(f"Main table record count: {len(result.main)}")
if "customer_orders" in result.tables:
    print(f"Orders table record count: {len(result.tables['customer_orders'])}")
```

## Error Handling with File Processing

Apply error handling strategies when processing files:

```python
import json

# Create a file with problematic data
with open("problematic_data.json", "w") as f:
    json.dump([
        {
            "id": "customer1",
            "name": "Alice Brown",
            "orders": [{"id": "order1", "items": [{"product": "Phone"}]}]
        },
        {
            "id": "customer2",
            "name": "Bob White",
            "orders": "invalid-orders-value"  # This should be an array
        },
        {
            "id": "customer3",
            "name": "Carol Green",
            "orders": [{"id": "order3", "items": None}]
        }
    ], f)

# Process the file with skip strategy
result = tm.flatten_file(
    file_path="problematic_data.json",
    name="customer",
    error_handling="skip"
)

# Save the successfully processed data
result.save("output_directory")

# Print summary
print(f"Processed {len(result.main)} customers successfully")
for table_name, records in result.tables.items():
    if table_name != "customer":  # Skip main table in this loop
        print(f"Table {table_name}: {len(records)} records")
```

## Error Handling with Streaming

For large datasets, combine error handling with streaming:

```python
# Stream process with error handling and logging
tm.flatten_stream(
    file_path="large_problematic_data.json",
    name="customer",
    output_path="output_directory",
    output_format="json",
    error_handling="skip",
    error_log="errors.log"  # Log errors to a file
)

# Check the error log
with open("errors.log", "r") as f:
    error_count = sum(1 for _ in f)
    print(f"Found {error_count} errors during processing")
```

## Custom Error Handling with Transforms

You can implement custom error handling logic using transforms:

```python
def safe_process_items(items_value):
    """Safely process items field, handling potential errors"""
    if items_value is None:
        return []  # Return empty list instead of None
    elif isinstance(items_value, list):
        return items_value
    else:
        # Try to convert to list if possible
        try:
            if isinstance(items_value, str):
                import json
                return json.loads(items_value)
            else:
                return list(items_value)
        except:
            return []  # Return empty list on failure

# Process with custom transform for error handling
result = tm.flatten(
    data=problematic_data,
    name="customer",
    transforms={
        "items": safe_process_items  # Apply transform to items field
    }
)
```

## Error Handling in Batch Processing

When processing batches of records, you can handle errors for each batch:

```python
# Sample batch data with some problematic records
batches = [
    [{"id": 1, "name": "Good Record 1"}, {"id": 2, "name": "Good Record 2"}],
    [{"id": 3, "name": "Good Record 3"}, {"id": None, "name": None}],  # Problem
    [{"id": 4, "name": "Good Record 4"}]
]

all_results = []

for i, batch in enumerate(batches):
    try:
        # Try to process each batch
        result = tm.flatten(
            data=batch,
            name="record",
            error_handling="skip"  # Skip problematic records
        )
        all_results.append(result)
        print(f"Batch {i+1}: Processed {len(result.main)} records")
    except Exception as e:
        print(f"Batch {i+1}: Failed to process - {str(e)}")

# Combine results (if needed)
combined_records = []
for result in all_results:
    combined_records.extend(result.main)

print(f"Total records processed: {len(combined_records)}")
```

## Best Practices for Error Handling

1. **Match Strategy to Environment**:
   - Use "raise" during development
   - Use "skip" or "warn" in production

2. **Log Errors Appropriately**:
   - In production, always log errors to a file
   - Include enough context to diagnose issues

3. **Implement Data Validation**:
   - Validate data before processing when possible
   - Use transforms to clean data proactively

4. **Monitor Error Rates**:
   - Track error rates over time
   - Investigate sudden increases in errors

5. **Balance Strictness and Resilience**:
   - Be strict with critical data
   - Be more lenient with non-critical fields

## Next Steps

- Learn about [performance optimization](../../user/advanced/performance-optimization.md)
- Explore [streaming large datasets](../intermediate/streaming-large-datasets.md)
- Try [customizing ID generation](../intermediate/customizing-id-generation.md)
