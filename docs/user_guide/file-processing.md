# File Processing

This guide covers processing files directly with Transmog, including JSON and CSV files with various options and configurations.

## File Processing Basics

Transmog provides the `flatten_file()` function for direct file processing:

```python
import transmog as tm

# Process JSON file
result = tm.flatten_file("data.json", name="records")

# Process with auto-detected name from filename
result = tm.flatten_file("products.json")  # name="products"

# Process with custom configuration
result = tm.flatten_file(
    "data.json",
    name="entities",
    separator=".",
    arrays="separate"
)
```

## JSON File Processing

### Basic JSON Processing

```python
# Simple JSON file processing
result = tm.flatten_file("products.json")

# Access results
print(f"Processed {len(result.main)} records")
print(f"Created {len(result.tables)} child tables")

# Save processed results
result.save("output", output_format="csv")
```

### JSON File Structure Support

Transmog handles various JSON file structures:

#### Single Object

```json
{
  "company": "TechCorp",
  "employees": ["Alice", "Bob"]
}
```

#### Array of Objects

```json
[
  {"id": 1, "name": "Product A"},
  {"id": 2, "name": "Product B"}
]
```

#### Nested Structure

```json
{
  "data": {
    "records": [
      {"nested": {"values": [1, 2, 3]}}
    ]
  }
}
```

### Large JSON Files

For large JSON files, consider streaming:

```python
# Stream large JSON files directly to output
tm.flatten_stream(
    "large_data.json",
    output_path="processed/",
    name="large_dataset",
    output_format="parquet",
    batch_size=1000,
    low_memory=True
)
```

## CSV File Processing

### Basic CSV Processing

```python
# Process CSV file
result = tm.flatten_file("employees.csv", name="employees")

# CSV files typically produce only a main table
print(f"Processed {len(result.main)} CSV records")
```

### CSV Configuration Options

The `flatten_file()` function auto-detects CSV format and handles it appropriately:

```python
# Process CSV with custom entity name
result = tm.flatten_file("data.csv", name="custom_name")

# All flatten() options work with CSV files
result = tm.flatten_file(
    "data.csv",
    name="records",
    preserve_types=True,
    skip_null=False
)
```

### CSV File Requirements

CSV files should follow these guidelines:

- First row contains column headers
- Consistent column structure throughout
- Standard CSV delimiters (comma, semicolon, tab)
- UTF-8 encoding preferred

## File Format Detection

Transmog automatically detects file formats based on extensions:

| Extension | Format | Processing |
|-----------|--------|------------|
| `.json` | JSON | Full nested processing |
| `.jsonl`, `.ndjson` | JSON Lines | Line-by-line processing |
| `.csv` | CSV | Tabular processing |

```python
# Automatic format detection
result = tm.flatten_file("data.json")    # Processed as JSON
result = tm.flatten_file("data.csv")     # Processed as CSV
result = tm.flatten_file("data.jsonl")   # Processed as JSON Lines
```

## Advanced File Processing

### Custom Naming from Filename

```python
from pathlib import Path

# Auto-extract entity name from filename
file_path = "customer_orders.json"
name = Path(file_path).stem  # "customer_orders"
result = tm.flatten_file(file_path, name=name)
```

### Processing Multiple Files

```python
import glob

# Process multiple files with consistent configuration
results = []
for file_path in glob.glob("data/*.json"):
    name = Path(file_path).stem
    result = tm.flatten_file(
        file_path,
        name=name,
        separator="_",
        arrays="separate"
    )
    results.append(result)

# Combine or process results as needed
total_records = sum(len(r.main) for r in results)
print(f"Processed {total_records} total records")
```

### Batch File Processing

```python
def process_data_directory(input_dir, output_dir):
    """Process all JSON files in a directory."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    for file_path in input_path.glob("*.json"):
        print(f"Processing {file_path.name}...")

        # Process file
        result = tm.flatten_file(file_path, name=file_path.stem)

        # Save results
        output_file = output_path / file_path.stem
        result.save(output_file, output_format="csv")

# Process entire directory
process_data_directory("raw_data/", "processed_data/")
```

## Error Handling with Files

### Robust File Processing

```python
def safe_file_processing(file_path, **options):
    """Process file with comprehensive error handling."""
    try:
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
result, error = safe_file_processing("data.json", name="data")
if error:
    print(f"Error: {error}")
else:
    print(f"Success: {len(result.main)} records processed")
```

### Skip Problematic Files

```python
def process_files_with_recovery(file_patterns, output_dir):
    """Process files with error recovery."""
    successful = 0
    failed = 0

    for pattern in file_patterns:
        for file_path in glob.glob(pattern):
            try:
                result = tm.flatten_file(
                    file_path,
                    name=Path(file_path).stem,
                    errors="skip"  # Skip problematic records
                )

                # Save successful results
                output_file = Path(output_dir) / f"{Path(file_path).stem}.json"
                result.save(output_file)
                successful += 1

            except Exception as e:
                print(f"Failed to process {file_path}: {e}")
                failed += 1

    print(f"Processed {successful} files successfully, {failed} failed")

# Process with recovery
process_files_with_recovery(["data/*.json", "backup/*.json"], "output/")
```

## Performance Optimization

### Memory-Efficient File Processing

```python
# For large files, use low memory mode
result = tm.flatten_file(
    "large_file.json",
    name="large_data",
    low_memory=True,
    batch_size=500
)
```

### Streaming for Very Large Files

```python
# Stream very large files directly to output
tm.flatten_stream(
    "huge_dataset.json",
    output_path="streaming_output/",
    name="huge_data",
    output_format="parquet",
    batch_size=1000,
    low_memory=True,
    errors="skip"
)
```

## File Validation

### Pre-Processing Validation

```python
import json
from pathlib import Path

def validate_json_file(file_path):
    """Validate JSON file before processing."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        if not data:
            return False, "Empty file"

        if not isinstance(data, (dict, list)):
            return False, "Invalid JSON structure"

        return True, "Valid"

    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {e}"
    except Exception as e:
        return False, f"Error reading file: {e}"

# Validate before processing
file_path = "data.json"
is_valid, message = validate_json_file(file_path)

if is_valid:
    result = tm.flatten_file(file_path)
else:
    print(f"Cannot process file: {message}")
```

### Post-Processing Validation

```python
def validate_results(result, expected_min_records=1):
    """Validate processing results."""
    issues = []

    # Check main table
    if len(result.main) < expected_min_records:
        issues.append(f"Too few main records: {len(result.main)}")

    # Check for orphaned child records
    if result.tables:
        main_ids = {r["_id"] for r in result.main}
        for table_name, records in result.tables.items():
            orphaned = [r for r in records if r["_parent_id"] not in main_ids]
            if orphaned:
                issues.append(f"Orphaned records in {table_name}: {len(orphaned)}")

    return issues

# Validate results
result = tm.flatten_file("data.json")
issues = validate_results(result)
if issues:
    for issue in issues:
        print(f"Warning: {issue}")
```

## Integration with Other Tools

### Database Loading

```python
# Process for database import
result = tm.flatten_file(
    "export.json",
    name="entities",
    id_field="id",
    preserve_types=False,    # Convert to strings for SQL
    skip_null=True
)

# Save as CSV for database import
result.save("db_import", output_format="csv")
```

### Data Pipeline Integration

```python
def file_to_pipeline(input_file, pipeline_config):
    """Process file for data pipeline."""
    result = tm.flatten_file(
        input_file,
        name=pipeline_config.get("entity_name", "data"),
        separator=pipeline_config.get("separator", "_"),
        arrays=pipeline_config.get("array_handling", "separate"),
        preserve_types=pipeline_config.get("preserve_types", False)
    )

    # Apply pipeline-specific transformations
    for table_name, records in result.all_tables.items():
        # Apply transformations based on pipeline requirements
        transformed = apply_pipeline_transforms(records, pipeline_config)
        result._result.replace_table(table_name, transformed)

    return result
```

## Best Practices

### File Organization

```text
project/
├── raw_data/           # Original files
│   ├── products.json
│   └── orders.csv
├── processed/          # Flattened results
│   ├── products/
│   └── orders/
└── scripts/           # Processing scripts
    └── process_files.py
```

### Configuration Management

```python
# Use configuration files for consistent processing
import yaml

def load_processing_config(config_file):
    """Load processing configuration from YAML."""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

# config.yaml
"""
default:
  separator: "_"
  arrays: "separate"
  preserve_types: false

products:
  separator: "."
  id_field: "product_id"

orders:
  arrays: "inline"
  preserve_types: true
"""

# Use configuration
config = load_processing_config("config.yaml")
result = tm.flatten_file("products.json", **config["products"])
```

## Next Steps

- **[Array Handling](array-handling.md)** - Advanced array processing options
- **[Output Formats](output-formats.md)** - Choosing and configuring output formats
- **[Streaming Guide](../developer_guide/streaming.md)** - Memory-efficient processing for large files
