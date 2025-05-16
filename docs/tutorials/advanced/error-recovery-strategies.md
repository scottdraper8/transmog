# Error Recovery Strategies

This tutorial explores Transmog's error recovery strategies for handling problematic data during transformation.

## Understanding Error Recovery Strategies

Transmog offers three main approaches to error handling:

1. **Strict (default)** - Raises exceptions immediately, halting processing
2. **Skip and Log** - Skips problematic records and continues processing
3. **Partial Recovery** - Extracts valid portions of records with errors

## When to Use Each Strategy

- **Strict**: Use during development or when data integrity is critical
- **Skip and Log**: Use in production when processing should continue despite errors
- **Partial Recovery**: Use when extracting partial data from problematic records is better than no data

## Configuring Error Strategies

Import the necessary components:

```python
from transmog import TransmogProcessor, TransmogConfig
from transmog.error import ErrorStrategy, RecoveryAction
```

### Strict Strategy Example

```python
# Configure with strict error handling (default)
config = TransmogConfig().with_error_handling(
    strategy=ErrorStrategy.STRICT
)
processor = TransmogProcessor(config)

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
    # This will raise an exception
    result = processor.process_data(problematic_data)
except Exception as e:
    print(f"Error encountered: {e}")
```

### Skip and Log Strategy

```python
# Configure with skip and log strategy
config = TransmogConfig().with_error_handling(
    strategy=ErrorStrategy.SKIP_AND_LOG
)
processor = TransmogProcessor(config)

# Process data with skip and log strategy
result = processor.process_data(problematic_data)

# Check for errors
error_count = result.error_count
print(f"Processed with {error_count} errors")

# Access error information
for error in result.errors:
    print(f"Error: {error.message}")
    print(f"Record path: {error.path}")
    print(f"Problematic value: {error.value}")
```

### Partial Recovery Strategy

```python
# Configure with partial recovery
config = TransmogConfig().with_error_handling(
    strategy=ErrorStrategy.PARTIAL_RECOVERY
)
processor = TransmogProcessor(config)

# Process data with partial recovery
result = processor.process_data(problematic_data)

# Check what was recovered
tables = result.to_dict()
for table_name, records in tables.items():
    print(f"\n=== {table_name} ===")
    print(f"Record count: {len(records)}")
    if records:
        print(f"First record: {records[0]}")
```

## Advanced Recovery Configuration

For more control, you can configure specific recovery actions for different error types:

```python
# Configure with custom recovery actions
config = TransmogConfig().with_error_handling(
    strategy=ErrorStrategy.PARTIAL_RECOVERY,
    recovery_actions={
        "TypeError": RecoveryAction.SKIP_FIELD,      # Skip fields with type errors
        "KeyError": RecoveryAction.USE_DEFAULT,      # Use defaults for missing keys
        "ValueError": RecoveryAction.SKIP_RECORD     # Skip records with value errors
    },
    default_values={
        "items": [],                                 # Default value for items
        "price": 0.0,                                # Default value for price
        "quantity": 1                                # Default value for quantity
    }
)

processor = TransmogProcessor(config)
result = processor.process_data(problematic_data)
```

## Error Logging and Reporting

Create comprehensive error reports:

```python
# Configure with detailed error reporting
config = TransmogConfig().with_error_handling(
    strategy=ErrorStrategy.SKIP_AND_LOG,
    log_level="DEBUG",                              # Set log level
    include_record_in_errors=True                   # Include record in error report
)

processor = TransmogProcessor(config)
result = processor.process_data(problematic_data)

# Generate an error report
def generate_error_report(result):
    """Generate a detailed error report from processing results"""
    report = {
        "summary": {
            "total_records": result.total_records,
            "error_count": result.error_count,
            "success_rate": f"{((result.total_records - result.error_count) / result.total_records) * 100:.2f}%"
        },
        "errors": []
    }

    for error in result.errors:
        error_detail = {
            "type": error.error_type,
            "message": error.message,
            "path": error.path,
            "record_id": error.record_id,
            "value": str(error.value)
        }
        report["errors"].append(error_detail)

    return report

# Generate and print the report
error_report = generate_error_report(result)
import json
print(json.dumps(error_report, indent=2))
```

## Real-World Example: Processing a File with Errors

Let's process a file containing problematic data:

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

# Configure for partial recovery
config = TransmogConfig().with_error_handling(
    strategy=ErrorStrategy.PARTIAL_RECOVERY,
    recovery_actions={
        "TypeError": RecoveryAction.SKIP_FIELD
    }
)

# Process the file with partial recovery
processor = TransmogProcessor(config)
result = processor.process_file("problematic_data.json")

# Write the successfully processed data
result.write_all_json("output_directory")

# Write error information to a separate file
with open("output_directory/errors.json", "w") as f:
    json.dump(generate_error_report(result), f, indent=2)
```

## Combining Error Strategies with Streaming

For large datasets, combine error handling with streaming:

```python
# Configure for streaming with error handling
config = TransmogConfig().with_error_handling(
    strategy=ErrorStrategy.SKIP_AND_LOG
)
processor = TransmogProcessor(config)

# Function to stream records from file
def stream_records(file_path):
    with open(file_path, "r") as f:
        for line in f:
            yield json.loads(line.strip())

# Process stream with error handling
streaming_result = processor.process_stream(
    stream_records("large_problematic_data.json"),
    streaming_output=True
)

# Write to files
streaming_result.write_streaming_json("output_directory")

# Report errors after processing completes
print(f"Processed with {streaming_result.error_count} errors")
```

## Best Practices for Error Handling

1. **Match Strategy to Environment**:
   - Use STRICT during development
   - Use SKIP_AND_LOG or PARTIAL_RECOVERY in production

2. **Understand Recovery Actions**:
   - SKIP_FIELD - Removes problematic fields
   - SKIP_RECORD - Removes entire records with errors
   - USE_DEFAULT - Substitutes default values for errors

3. **Monitor Error Rates**:
   - Track error_count and total_records
   - Set thresholds for acceptable error rates

4. **Document Error Patterns**:
   - Keep error reports to identify recurring issues
   - Use error patterns to improve upstream data quality

## Next Steps

- Learn about [optimizing memory usage](../../user/advanced/performance-optimization.md)
- Explore [deterministic ID generation](../intermediate/customizing-id-generation.md)
- Read the [error handling guide](../../user/advanced/error-handling.md) for more details
