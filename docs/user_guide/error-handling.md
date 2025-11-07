# Error Handling

This guide covers Transmog's error handling capabilities, including error recovery strategies and
robust data processing techniques.

## Error Handling Overview

Transmog provides error handling through the `recovery_strategy` configuration parameter:

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `"strict"` | Stop processing and raise exception (default) | Development, strict data validation |
| `"skip"` | Skip problematic records and continue | Production, data quality issues |
| `"partial"` | Process what's possible, skip errors | Partial data recovery |
| `"raise"` | Raise exceptions immediately | Strict validation |
| `"warn"` | Log warnings but continue processing | Monitoring |

## Error Handling Modes

### Strict Mode (Default)

```python
import transmog as tm

# Default behavior: raise exceptions on errors
config = tm.TransmogConfig(recovery_strategy="strict")
try:
    result = tm.flatten(problematic_data, name="strict", config=config)
except tm.TransmogError as e:
    print(f"Processing failed: {e}")
    # Handle the error appropriately
```

### Skip Mode

```python
# Skip problematic records and continue
config = tm.TransmogConfig(recovery_strategy="skip", allow_malformed_data=True)
result = tm.flatten(messy_data, name="tolerant", config=config)

print(f"Successfully processed {len(result.main)} records")
# Some records may have been skipped due to errors
```

### Error-Tolerant Configuration

```python
# Use error-tolerant factory method
config = tm.TransmogConfig.error_tolerant()
result = tm.flatten(noisy_data, name="monitored", config=config)

# Processed with error tolerance
print(f"Processed {len(result.main)} records")
```

## Common Error Scenarios

### Data Type Issues

```python
# Mixed data types that cause processing issues
problematic_data = [
    {"id": 1, "value": "normal_string"},
    {"id": 2, "value": {"nested": "object"}},  # Unexpected nesting
    {"id": 3, "value": [1, 2, 3]},             # Unexpected array
    {"id": 4, "value": None}                   # Null value
]

# Handle with error tolerance
config = tm.TransmogConfig(
    recovery_strategy="skip",
    allow_malformed_data=True,
    skip_null=True,
    cast_to_string=False
)
result = tm.flatten(problematic_data, name="mixed_types", config=config)

print(f"Processed {len(result.main)} out of {len(problematic_data)} records")
```

### Malformed JSON Structures

```python
# Data with inconsistent structure
inconsistent_data = [
    {"user": {"name": "Alice", "email": "alice@example.com"}},
    {"user": "Bob"},                           # String instead of object
    {"user": {"name": "Charlie"}},             # Missing email field
    {"different_field": {"name": "Dave"}}      # Different field name
]

# Process with error recovery
config = tm.TransmogConfig(
    recovery_strategy="skip",
    allow_malformed_data=True,
    include_empty=False,
    nested_threshold=2
)
result = tm.flatten(inconsistent_data, name="users", config=config)
```

### Missing Required Fields

```python
# Data with missing ID fields
data_with_missing_ids = [
    {"product_id": "PROD1", "name": "Laptop"},
    {"name": "Mouse"},                         # Missing product_id
    {"product_id": "PROD3", "name": "Keyboard"}
]

# Use natural IDs with fallback
config = tm.TransmogConfig(
    id_field="product_id",
    recovery_strategy="skip",
    allow_malformed_data=True
)
result = tm.flatten(data_with_missing_ids, name="products", config=config)

# Records without product_id get generated IDs or are skipped
```

## File Processing Error Handling

### Robust File Processing

```python
def safe_file_processing(file_path, **options):
    """Process file with comprehensive error handling."""
    try:
        # Attempt to process the file
        result = tm.flatten_file(file_path, **options)
        return result, None

    except FileNotFoundError:
        return None, f"File not found: {file_path}"

    except tm.ValidationError as e:
        return None, f"Configuration error: {e}"

    except tm.TransmogError as e:
        return None, f"Processing error: {e}"

    except Exception as e:
        return None, f"Unexpected error: {e}"

# Use safe processing
result, error = safe_file_processing(
    "data.json",
    name="data",
    config=tm.TransmogConfig.error_tolerant()
)

if error:
    print(f"Failed to process file: {error}")
else:
    print(f"Successfully processed {len(result.main)} records")
```

### Batch File Processing with Recovery

```python
import glob
from pathlib import Path

def process_files_with_recovery(file_pattern, output_dir, **options):
    """Process multiple files with error recovery."""
    successful = []
    failed = []

    for file_path in glob.glob(file_pattern):
        try:
            # Process each file with error tolerance
            config = options.pop("config", tm.TransmogConfig.error_tolerant())
            result = tm.flatten_file(
                file_path,
                name=Path(file_path).stem,
                config=config,
                **options
            )

            # Save successful results
            output_file = Path(output_dir) / f"{Path(file_path).stem}"
            result.save(output_file, output_format="json")
            successful.append(file_path)

        except Exception as e:
            failed.append((file_path, str(e)))
            print(f"Failed to process {file_path}: {e}")

    print(f"Successfully processed {len(successful)} files")
    print(f"Failed to process {len(failed)} files")

    return successful, failed

# Process with recovery
successful, failed = process_files_with_recovery(
    "data/*.json",
    "output/"
)
```

## Streaming Error Handling

### Resilient Streaming

```python
# Stream processing with error tolerance
config = tm.TransmogConfig(
    recovery_strategy="skip",
    allow_malformed_data=True,
    batch_size=1000,
    batch_size=100, cache_size=1000
)

try:
    tm.flatten_stream(
        large_problematic_dataset,
        output_path="streaming_output/",
        name="large_data",
        output_format="parquet",
        config=config
    )
except tm.TransmogError as e:
    print(f"Streaming failed: {e}")
    # Implement recovery strategy
```

### Partial Processing Recovery

```python
def streaming_with_checkpoints(data, output_path, checkpoint_interval=10000):
    """Stream processing with checkpoint recovery."""
    processed_count = 0

    # Process data in chunks with error handling
    for i in range(0, len(data), checkpoint_interval):
        chunk = data[i:i + checkpoint_interval]
        chunk_name = f"chunk_{i // checkpoint_interval}"

        try:
            config = tm.TransmogConfig(
                recovery_strategy="skip",
                allow_malformed_data=True,
                batch_size=1000
            )
            tm.flatten_stream(
                chunk,
                output_path=f"{output_path}/{chunk_name}/",
                name="data",
                output_format="parquet",
                config=config
            )
            processed_count += len(chunk)
            print(f"Processed chunk {chunk_name}: {len(chunk)} records")

        except Exception as e:
            print(f"Failed to process chunk {chunk_name}: {e}")
            # Continue with next chunk
            continue

    print(f"Total processed: {processed_count} records")
```

## Data Quality and Validation

### Pre-Processing Validation

```python
def validate_data_structure(data):
    """Validate data structure before processing."""
    issues = []

    if not data:
        issues.append("Empty dataset")
        return issues

    # Check if data is a list
    if isinstance(data, list):
        if not data:
            issues.append("Empty list")
        else:
            # Check first item structure
            sample = data[0]
            if not isinstance(sample, dict):
                issues.append("List items must be dictionaries")

    elif isinstance(data, dict):
        # Single object is acceptable
        pass

    else:
        issues.append("Data must be dict or list of dicts")

    return issues

# Validate before processing
issues = validate_data_structure(user_data)
if issues:
    print(f"Data validation issues: {issues}")
    # Decide whether to proceed or abort
else:
    result = tm.flatten(user_data, name="validated")
```

### Post-Processing Validation

```python
def validate_results(result, expected_min_records=1):
    """Validate processing results."""
    validation_issues = []

    # Check main table
    if len(result.main) < expected_min_records:
        validation_issues.append(f"Too few main records: {len(result.main)}")

    # Check for orphaned child records
    if result.tables:
        main_ids = {r["_id"] for r in result.main}
        for table_name, records in result.tables.items():
            orphaned = [r for r in records if r["_parent_id"] not in main_ids]
            if orphaned:
                validation_issues.append(
                    f"Orphaned records in {table_name}: {len(orphaned)}"
                )

    # Check for empty tables
    empty_tables = [name for name, records in result.tables.items() if not records]
    if empty_tables:
        validation_issues.append(f"Empty tables: {empty_tables}")

    return validation_issues

# Validate results
config = tm.TransmogConfig.error_tolerant()
result = tm.flatten(data, name="validated", config=config)
issues = validate_results(result)

for issue in issues:
    print(f"Validation warning: {issue}")
```

## Error Recovery Strategies

### Graceful Degradation

```python
def process_with_fallback(data, primary_config, fallback_config):
    """Process data with fallback configuration."""
    try:
        # Try primary configuration
        return tm.flatten(data, **primary_config)

    except tm.TransmogError as e:
        print(f"Primary processing failed: {e}")
        print("Attempting fallback configuration...")

        try:
            # Try fallback configuration
            return tm.flatten(data, **fallback_config)

        except tm.TransmogError as fallback_error:
            print(f"Fallback processing also failed: {fallback_error}")
            raise

# Define configurations
primary = {
    "name": "data",
    "errors": "raise",
    "preserve_types": True,
    "arrays": "separate"
}

fallback = {
    "name": "data",
    "errors": "skip",
    "preserve_types": False,
    "arrays": "inline"
}

# Process with fallback
result = process_with_fallback(problematic_data, primary, fallback)
```

### Data Cleaning Pipeline

```python
def clean_and_process(data, cleaning_steps=None):
    """Clean data and process with error handling."""
    if cleaning_steps is None:
        cleaning_steps = [
            "remove_nulls",
            "flatten_nested_strings",
            "normalize_types"
        ]

    cleaned_data = data.copy() if isinstance(data, list) else [data.copy()]

    # Apply cleaning steps
    for step in cleaning_steps:
        if step == "remove_nulls":
            cleaned_data = remove_null_records(cleaned_data)
        elif step == "flatten_nested_strings":
            cleaned_data = flatten_string_objects(cleaned_data)
        elif step == "normalize_types":
            cleaned_data = normalize_data_types(cleaned_data)

    # Process cleaned data
    try:
        config = tm.TransmogConfig(
            recovery_strategy="skip",
            allow_malformed_data=True,
            cast_to_string=False
        )
        return tm.flatten(cleaned_data, name="cleaned", config=config)
    except tm.TransmogError as e:
        print(f"Processing failed even after cleaning: {e}")
        raise

def remove_null_records(data):
    """Remove records that are completely null or empty."""
    return [record for record in data if record and any(record.values())]

def flatten_string_objects(data):
    """Handle cases where objects are serialized as strings."""
    import json

    for record in data:
        for key, value in record.items():
            if isinstance(value, str):
                try:
                    # Try to parse as JSON
                    parsed = json.loads(value)
                    if isinstance(parsed, dict):
                        record[key] = parsed
                except (json.JSONDecodeError, TypeError):
                    # Keep as string if not valid JSON
                    pass

    return data

def normalize_data_types(data):
    """Normalize common data type inconsistencies."""
    for record in data:
        for key, value in record.items():
            # Convert string representations of numbers
            if isinstance(value, str):
                if value.isdigit():
                    record[key] = int(value)
                elif value.replace('.', '').replace('-', '').isdigit():
                    try:
                        record[key] = float(value)
                    except ValueError:
                        pass

    return data
```

## Monitoring and Logging

### Comprehensive Error Monitoring

```python
import logging
from datetime import datetime

def setup_error_monitoring():
    """Set up comprehensive error monitoring."""
    # Configure logging
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('transmog_errors.log'),
            logging.StreamHandler()
        ]
    )

def process_with_monitoring(data, name, **options):
    """Process data with detailed monitoring."""
    setup_error_monitoring()

    start_time = datetime.now()
    original_count = len(data) if isinstance(data, list) else 1

    try:
        config = options.pop("config", tm.TransmogConfig.error_tolerant())
        result = tm.flatten(data, name=name, config=config, **options)

        # Log success metrics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        processed_count = len(result.main)
        success_rate = (processed_count / original_count) * 100

        logging.info(f"Processing completed:")
        logging.info(f"  - Input records: {original_count}")
        logging.info(f"  - Output records: {processed_count}")
        logging.info(f"  - Success rate: {success_rate:.2f}%")
        logging.info(f"  - Duration: {duration:.2f} seconds")
        logging.info(f"  - Child tables: {len(result.tables)}")

        return result

    except Exception as e:
        logging.error(f"Processing failed: {e}")
        raise

# Use monitored processing
config = tm.TransmogConfig(
    array_mode=tm.ArrayMode.SEPARATE,
    cast_to_string=False
)
result = process_with_monitoring(large_dataset, name="monitored_data", config=config)
```

## Best Practices

### Error Handling Strategy Selection

```python
# Development and testing: Use strict error handling
if environment == "development":
    config = tm.TransmogConfig(recovery_strategy="strict")

# Production with high data quality: Use partial recovery
elif environment == "production" and data_quality == "high":
    config = tm.TransmogConfig(recovery_strategy="partial")

# Production with poor data quality: Skip errors
elif environment == "production" and data_quality == "low":
    config = tm.TransmogConfig.error_tolerant()

result = tm.flatten(data, name="adaptive", config=config)
```

### Configuration Templates

```python
# Error-tolerant configuration for messy data
MESSY_DATA_CONFIG = tm.TransmogConfig(
    recovery_strategy="skip",
    allow_malformed_data=True,
    skip_null=True,
    include_empty=False,
    nested_threshold=3,
    array_mode=tm.ArrayMode.INLINE
)

# Strict configuration for clean data
CLEAN_DATA_CONFIG = tm.TransmogConfig(
    recovery_strategy="strict",
    skip_null=False,
    include_empty=False,
    cast_to_string=False,
    array_mode=tm.ArrayMode.SEPARATE
)

# Production configuration with timestamps
PRODUCTION_CONFIG = tm.TransmogConfig(
    recovery_strategy="partial",
    skip_null=True,
    include_empty=False,
    cast_to_string=False,
    array_mode=tm.ArrayMode.SEPARATE,
    time_field="_timestamp"
)

# Use appropriate configuration
config = MESSY_DATA_CONFIG if data_is_messy else CLEAN_DATA_CONFIG
result = tm.flatten(data, name="configured", config=config)
```

## Next Steps

- **[Performance Guide](../developer_guide/performance.md)** - Optimize error handling for large datasets
- **[Streaming Guide](../developer_guide/streaming.md)** - Error handling in streaming scenarios
- **[API Reference](../api_reference/api.md)** - Complete error handling parameter documentation
