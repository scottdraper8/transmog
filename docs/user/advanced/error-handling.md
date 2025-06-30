---
title: Error Handling
---

# Error Handling

> **API Reference**: For detailed API documentation, see the [Error API Reference](../../api/error.md).

This document describes error handling capabilities in Transmog.

Transmog provides error handling for managing errors during data processing. This guide covers strategies
for handling different types of errors.

## Error Types

Transmog defines several exception types for specific error conditions:

```text
TransmogError              - Base class for all Transmog errors
├── ProcessingError        - Errors during data processing
├── ValidationError        - Input validation failures
├── ParsingError           - JSON parsing problems
├── FileError              - File operations issues
├── MissingDependencyError - Missing optional dependencies
├── ConfigurationError     - Configuration problems
└── OutputError            - Errors writing output
```

## Basic Error Handling

Example of handling Transmog errors:

```python
import transmog as tm
from transmog.error import TransmogError, ProcessingError, ParsingError

try:
    result = tm.flatten(my_data, name="customers")
except ParsingError as e:
    print(f"JSON parsing error: {e}")
    # Handle parsing error specifically
except ProcessingError as e:
    print(f"Processing error: {e}")
    # Handle processing error specifically
except TransmogError as e:
    print(f"Other Transmog error: {e}")
    # Handle other Transmog errors
except Exception as e:
    print(f"Unexpected error: {e}")
    # Handle unexpected errors
```

## Error Handling Options

Transmog provides several options for error handling:

```python
import transmog as tm

# Process with "skip" error handling (skip problematic records)
result = tm.flatten(
    data, 
    name="customers", 
    error_handling="skip"
)

# Process with "warn" error handling (log warnings but continue)
result = tm.flatten(
    data, 
    name="customers", 
    error_handling="warn"
)

# Process with strict error handling (default)
result = tm.flatten(
    data, 
    name="customers", 
    error_handling="raise"
)

# Specify an error log file
result = tm.flatten(
    data, 
    name="customers", 
    error_handling="skip",
    error_log="errors.log"
)
```

## Error Handling Modes

Transmog provides three error handling modes:

### 1. Raise Mode (`"raise"`)

The raise mode (default) raises all errors without attempting recovery. This ensures data integrity
but fails processing if issues are encountered.

**When to use:**

- When data integrity is critical and errors indicate problems
- During development and testing to catch issues
- For applications where data quality must be guaranteed
- When processing sensitive information

**Configuration:**

```python
# Using the default (raise) mode
result = tm.flatten(data, name="customers")

# Explicitly specifying raise mode
result = tm.flatten(data, name="customers", error_handling="raise")
```

### 2. Skip Mode (`"skip"`)

This mode skips problematic records and continues processing with the remaining records.

**When to use:**

- For batch processing of large datasets where errors are expected
- When partial results are acceptable
- For data exploration tasks where complete coverage isn't required
- When processing non-critical data

**Configuration:**

```python
# Using skip mode
result = tm.flatten(data, name="customers", error_handling="skip")

# Skip with error logging
result = tm.flatten(
    data, 
    name="customers", 
    error_handling="skip",
    error_log="errors.log"
)
```

### 3. Warn Mode (`"warn"`)

The warn mode logs warnings for problematic records but continues processing.

**When to use:**

- For data exploration and analysis
- When you want to be aware of issues but still process all data
- During development and testing
- For monitoring data quality

**Configuration:**

```python
# Using warn mode
result = tm.flatten(data, name="customers", error_handling="warn")
```

## Advanced Error Handling

### Type Casting for Error Prevention

Transmog can automatically cast values to strings to prevent type errors:

```python
# Enable automatic type casting
result = tm.flatten(
    data, 
    name="customers", 
    cast_to_string=True
)
```

### Handling Malformed JSON

When processing JSON files that might be malformed:

```python
# Process potentially malformed JSON
result = tm.flatten_file(
    "potentially_malformed.json",
    name="customers", 
    error_handling="skip",
    allow_malformed=True
)
```

### Combining Error Handling Strategies

You can combine different error handling options:

```python
# Comprehensive error handling
result = tm.flatten(
    data, 
    name="customers", 
    error_handling="skip",
    error_log="errors.log",
    cast_to_string=True,
    allow_malformed=True
)
```

## Error Handling with Streaming

Error handling works with streaming processing as well:

```python
# Stream process with error handling
tm.flatten_stream(
    file_path="large_data.json",
    name="customers",
    output_path="output_dir",
    output_format="parquet",
    error_handling="skip",
    error_log="stream_errors.log"
)
```

## Examples

### Example: Processing Data with Skip Error Handling

```python
import transmog as tm

# Sample data with problematic records
data = [
    {"id": 1, "name": "Valid Record"},
    {"id": 2, "name": None, "values": [float('nan')]},  # Problematic
    {"id": 3, "name": "Another Valid Record"}
]

# Process with skip error handling
result = tm.flatten(
    data, 
    name="records", 
    error_handling="skip",
    error_log="errors.log"
)

# Only valid records will be in the result
print(f"Processed {len(result.main)} records")
```

### Example: Handling Malformed JSON Files

```python
import transmog as tm

try:
    # Try to process a potentially malformed JSON file
    result = tm.flatten_file(
        "malformed.json",
        name="data",
        error_handling="skip",
        allow_malformed=True
    )
    print(f"Successfully processed {len(result.main)} records")
    
except tm.error.TransmogError as e:
    print(f"Failed to process file: {e}")
```

### Example: Handling CSV Type Errors

```python
import transmog as tm

# Process a CSV file with potential type issues
result = tm.flatten_file(
    "data_with_mixed_types.csv",
    name="data",
    cast_to_string=True,  # Convert numeric fields to strings to avoid type errors
    error_handling="warn"  # Log warnings but continue processing
)

# Save the result
result.save("cleaned_data.csv")
```

## Best Practices

1. **Choose the right error handling mode** for your use case:
   - Use `"raise"` for critical data where quality is essential
   - Use `"skip"` for large datasets where some errors are acceptable
   - Use `"warn"` for exploratory analysis and debugging

2. **Always log errors** when using skip or warn modes:
   ```python
   result = tm.flatten(data, name="entity", error_handling="skip", error_log="errors.log")
   ```

3. **Use cast_to_string** when dealing with mixed data types:
   ```python
   result = tm.flatten(data, name="entity", cast_to_string=True)
   ```

4. **Enable allow_malformed** when processing external data sources:
   ```python
   result = tm.flatten_file("external_data.json", name="entity", allow_malformed=True)
   ```

5. **Combine with validation** for better data quality:
   ```python
   # Validate data before processing
   if all(validate_record(record) for record in data):
       result = tm.flatten(data, name="entity")
   ```

6. **Review error logs** regularly to identify patterns and improve data quality.
