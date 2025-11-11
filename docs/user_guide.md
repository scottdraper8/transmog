# User Guide

This guide covers all aspects of using Transmog for data transformation, from basic usage to advanced configuration.

## Core Functions

Transmog provides three main functions:

| Function | Purpose | Use Case |
|----------|---------|----------|
| `tm.flatten(data)` | Transform data in memory | Data fits in memory |
| `tm.flatten_file(path)` | Process files directly | Working with files |
| `tm.flatten_stream(data, output_path)` | Stream to files | Large datasets |

### flatten()

Transform nested data structures into flat tables:

```python
import transmog as tm

data = {
    "user": {"name": "Alice", "email": "alice@example.com"},
    "orders": [
        {"id": 101, "amount": 99.99},
        {"id": 102, "amount": 45.50}
    ]
}

result = tm.flatten(data, name="customer")

# Access results
print(result.main)          # Main table
print(result.tables)        # Child tables
print(result.all_tables)    # All tables including main
```

### flatten_file()

Process JSON and JSONL files directly:

```python
# Process JSON file
result = tm.flatten_file("data.json", name="records")

# Auto-detect name from filename
result = tm.flatten_file("products.json")  # name="products"

# With custom configuration
config = tm.TransmogConfig(separator=".")
result = tm.flatten_file("data.json", config=config)
```

Supported file formats:

| Extension | Format | Processing |
|-----------|--------|------------|
| `.json` | JSON | Full nested processing |
| `.jsonl`, `.ndjson` | JSON Lines | Line-by-line processing |

### flatten_stream()

Stream large datasets directly to files:

```python
# Stream to CSV files
tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="csv"
)

# Stream to compressed Parquet
tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="parquet",
    compression="snappy"
)
```

Streaming processes data in batches without keeping all data in memory.
Use `TransmogConfig.for_memory()` for memory-constrained environments.

## Configuration

### TransmogConfig Parameters

The `TransmogConfig` class controls all processing behavior:

```python
config = tm.TransmogConfig(
    # Naming
    separator="_",              # Character to join nested field names

    # Processing
    cast_to_string=False,       # Convert all values to strings
    null_handling=tm.NullHandling.SKIP,  # How to handle nulls
    array_mode=tm.ArrayMode.SMART,       # How to handle arrays
    batch_size=1000,            # Records to process at once
    max_depth=100,              # Maximum recursion depth

    # Metadata
    id_field="_id",             # Field name for record IDs
    parent_field="_parent_id",  # Field name for parent references
    time_field="_timestamp",    # Field name for timestamps (None to disable)

    # ID Discovery
    id_patterns=None,           # Field names to check for natural IDs

    # Deterministic IDs
    deterministic_ids=False,    # Generate deterministic IDs
    id_fields=None,             # Fields for composite deterministic IDs

    # Error Handling
    recovery_mode=tm.RecoveryMode.STRICT  # Error recovery strategy
)

result = tm.flatten(data, config=config)
```

### Configuration Presets

Factory methods provide optimized configurations:

```python
# Default: types preserved, optimized for analytics
result = tm.flatten(data)

# CSV: strings, includes empty/null values
config = tm.TransmogConfig.for_csv()
result = tm.flatten(data, config=config)

# Memory: small batches (100)
config = tm.TransmogConfig.for_memory()
result = tm.flatten(data, config=config)

# Error-tolerant: skip malformed records
config = tm.TransmogConfig.error_tolerant()
result = tm.flatten(data, config=config)
```

(array-handling)=
## Array Handling

Arrays are processed according to the `array_mode` configuration parameter.

### Array Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `ArrayMode.SMART` | Simple arrays as native, complex arrays extracted | Parquet output |
| `ArrayMode.SEPARATE` | All arrays to child tables | Relational analysis |
| `ArrayMode.INLINE` | Arrays as JSON strings | Document storage |
| `ArrayMode.SKIP` | Ignore arrays | Focus on scalars |

### Smart Mode (Default)

Intelligently handles arrays based on content:

```python
data = {
    "product": {
        "name": "Laptop",
        "tags": ["electronics", "computers"],  # Simple array - kept as native
        "reviews": [  # Complex array - extracted to child table
            {"rating": 5, "comment": "Excellent"},
            {"rating": 4, "comment": "Good value"}
        ]
    }
}

result = tm.flatten(data, name="products")

print(result.main)
# [
#   {
#     'product_name': 'Laptop',
#     'product_tags': ['electronics', 'computers'],  # Native array
#     '_id': '...',
#     '_timestamp': '...'
#   }
# ]

print(result.tables["products_reviews"])
# [
#   {'rating': 5, 'comment': 'Excellent', '_parent_id': '...', '_id': '...'},
#   {'rating': 4, 'comment': 'Good value', '_parent_id': '...', '_id': '...'}
# ]
```

Smart mode provides:

- Native array storage for Parquet output with efficient querying
- Automatic handling of both simple and complex arrays
- Avoids unnecessary table creation for simple arrays
- Sensible defaults for common use cases

### Separate Mode

Extract all arrays into child tables:

```python
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="products", config=config)

# All arrays become separate tables
print(result.tables.keys())
# ['products_tags', 'products_reviews']
```

Separate mode applies when:

- Full relational structure is required
- All array relationships need explicit tracking
- Database normalization is the goal
- Arrays will be queried independently

### Inline Mode

Keep arrays as JSON strings:

```python
config = tm.TransmogConfig(array_mode=tm.ArrayMode.INLINE)
result = tm.flatten(data, name="products", config=config)

print(result.main)
# [
#   {
#     'product_name': 'Laptop',
#     'product_tags': '["electronics", "computers"]',
#     'product_reviews': '[{"rating": 5, ...}]',
#     '_id': '...'
#   }
# ]
```

Inline mode applies when:

- Document-oriented storage is required
- Array relationships are not needed for analysis
- Minimizing table count is a requirement
- Arrays will be processed by downstream tools

### Skip Mode

Ignore arrays entirely:

```python
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SKIP)
result = tm.flatten(data, name="products", config=config)

# Only scalar fields are included
print(result.main)
# [{'product_name': 'Laptop', '_id': '...'}]
```

Skip mode applies when:

- Only scalar data is required
- Arrays contain unstructured or excluded data
- Simplified data structure is required
- Array processing is handled separately

### Nested Arrays

Arrays can contain objects with nested arrays:

```python
data = {
    "company": "TechCorp",
    "departments": [
        {
            "name": "Engineering",
            "teams": [
                {"name": "Frontend", "size": 5},
                {"name": "Backend", "size": 8}
            ]
        }
    ]
}

config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)
result = tm.flatten(data, name="company", config=config)

# Creates multi-level hierarchy
print(list(result.all_tables.keys()))
# ['company', 'company_departments', 'company_departments_teams']
```

(id-management)=
## ID Management

### Automatic ID Generation

By default, Transmog generates unique IDs for all records:

```python
data = {"product": {"name": "Laptop"}}
result = tm.flatten(data, name="products")

print(result.main[0])
# {'product_name': 'Laptop', '_id': 'generated_unique_id', '_timestamp': '...'}
```

### Natural ID Fields

Use existing ID fields from data:

```python
data = {
    "product": {
        "product_id": "PROD123",
        "name": "Gaming Laptop",
        "reviews": [
            {"review_id": "REV456", "rating": 5},
            {"review_id": "REV789", "rating": 4}
        ]
    }
}

config = tm.TransmogConfig(id_field="product_id")
result = tm.flatten(data, name="products", config=config)

print(result.main[0])
# {'product_id': 'PROD123', 'product_name': 'Gaming Laptop', '_id': 'PROD123'}

print(result.tables["products_reviews"][0])
# {'review_id': 'REV456', 'rating': 5, '_parent_id': 'PROD123', '_id': 'REV456'}
```

When specified ID fields are missing, Transmog generates IDs automatically.

### ID Discovery

Automatically discover natural IDs from data:

```python
data = {
    "company": {
        "company_id": "COMP123",
        "employees": [
            {"employee_id": "EMP001", "name": "Alice"},
            {"employee_id": "EMP002", "name": "Bob"}
        ]
    }
}

config = tm.TransmogConfig(
    id_patterns=["company_id", "employee_id"]
)
result = tm.flatten(data, name="company", config=config)

# Automatically uses company_id and employee_id where present
```

### Deterministic IDs

Generate reproducible IDs based on record content:

```python
# Enable deterministic IDs
config = tm.TransmogConfig(deterministic_ids=True)
data = {"name": "Laptop", "price": 999}

result1 = tm.flatten(data, name="products", config=config)
result2 = tm.flatten(data, name="products", config=config)

# Same data produces same ID
assert result1.main[0]["_id"] == result2.main[0]["_id"]
```

Create composite deterministic IDs from multiple fields:

```python
data1 = {"region": "US", "store": "001", "product": "laptop", "price": 999}
data2 = {"region": "US", "store": "001", "product": "laptop", "price": 899}

config = tm.TransmogConfig(
    deterministic_ids=True,
    id_fields=["region", "store", "product"]
)

result1 = tm.flatten(data1, name="sales", config=config)
result2 = tm.flatten(data2, name="sales", config=config)

# Same composite key produces same ID (price is ignored)
assert result1.main[0]["_id"] == result2.main[0]["_id"]
```

### Custom ID Field Names

Customize metadata field names:

```python
config = tm.TransmogConfig(
    id_field="record_id",
    parent_field="parent_ref",
    time_field="_created_at"
)
result = tm.flatten(data, config=config)

# Records use custom field names
print(result.main[0])
# {'name': 'Product', 'record_id': '...', '_created_at': '...'}
```

Disable timestamp tracking:

```python
config = tm.TransmogConfig(time_field=None)
result = tm.flatten(data, config=config)

# No timestamp field added
```

### Parent-Child Relationships

Child records reference their parents through the parent ID field:

```python
result = tm.flatten(data, name="products")

# Main record
main_id = result.main[0]["_id"]

# Child records reference main record
for review in result.tables["products_reviews"]:
    assert review["_parent_id"] == main_id
```

(error-handling)=
## Error Handling

Control error handling behavior through `recovery_mode`:

### Recovery Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `RecoveryMode.STRICT` | Stop on first error (default) | Development, data validation |
| `RecoveryMode.SKIP` | Skip problematic records | Production, noisy data |

### Strict Mode

Raise exceptions on errors:

```python
config = tm.TransmogConfig(recovery_mode=tm.RecoveryMode.STRICT)

try:
    result = tm.flatten(problematic_data, config=config)
except tm.TransmogError as e:
    print(f"Processing failed: {e}")
```

### Skip Mode

Continue processing when errors occur:

```python
config = tm.TransmogConfig(recovery_mode=tm.RecoveryMode.SKIP)
result = tm.flatten(messy_data, config=config)

# Problematic records are skipped
print(f"Successfully processed {len(result.main)} records")
```

Use the error-tolerant preset:

```python
config = tm.TransmogConfig.error_tolerant()
result = tm.flatten(noisy_data, config=config)
```

### Common Error Scenarios

Handle data type inconsistencies:

```python
problematic_data = [
    {"id": 1, "value": "normal_string"},
    {"id": 2, "value": {"nested": "object"}},  # Unexpected nesting
    {"id": 3, "value": [1, 2, 3]},             # Unexpected array
]

config = tm.TransmogConfig(
    recovery_mode=tm.RecoveryMode.SKIP,
    null_handling=tm.NullHandling.SKIP
)
result = tm.flatten(problematic_data, name="mixed", config=config)
```

Handle file processing errors:

```python
def safe_file_processing(file_path, **options):
    """Process file with error handling."""
    try:
        result = tm.flatten_file(file_path, **options)
        return result, None
    except FileNotFoundError:
        return None, f"File not found: {file_path}"
    except tm.ValidationError as e:
        return None, f"Validation error: {e}"
    except tm.TransmogError as e:
        return None, f"Processing error: {e}"

result, error = safe_file_processing("data.json")
if error:
    print(f"Error: {error}")
```

### Error Handling Strategy

Select strategy based on environment:

```python
# Development: Use strict mode
if environment == "development":
    config = tm.TransmogConfig(recovery_mode=tm.RecoveryMode.STRICT)

# Production: Skip problematic records
elif environment == "production":
    config = tm.TransmogConfig(recovery_mode=tm.RecoveryMode.SKIP)
```

## Output Formats

### Supported Formats

| Format | Use Case | Characteristics |
|--------|----------|-----------------|
| **CSV** | Spreadsheets, databases | Wide compatibility, compact |
| **Parquet** | Analytics, data lakes | Columnar, compressed, fast queries |

### Saving Results

Save flattened data to files:

```python
result = tm.flatten(data, name="products")

# Save to directory (multiple tables)
result.save("output/", output_format="csv")
# Creates: output/products.csv, output/products_reviews.csv

# Save single table to file
result.save("products.csv")

# Auto-detect format from extension
result.save("products.parquet")
```

### CSV Output

CSV format characteristics:

- Opens in Excel, databases, and analytics tools
- Efficient storage for large datasets
- Fast read and write operations
- Direct import into relational databases

CSV considerations:

- Flat structure only - cannot represent nested data
- All values become strings by default
- Limited metadata - no built-in type information
- UTF-8 encoding recommended for international data

Optimize for CSV output:

```python
config = tm.TransmogConfig.for_csv()
result = tm.flatten(data, config=config)
result.save("output/", output_format="csv")
```

### Parquet Output

Parquet format characteristics:

- Columnar storage for efficient analytics queries
- Compression results in smaller file sizes than CSV
- Maintains data types natively
- Optimized for analytical workloads
- Supports schema changes over time

Parquet requires `pyarrow`:

```bash
pip install pyarrow
```

Optimize for Parquet output:

```python
# Default configuration is already optimized for Parquet
result = tm.flatten(data, name="analytics")
result.save("analytics_data/", output_format="parquet")

# With compression
tm.flatten_stream(
    large_data,
    output_path="output/",
    output_format="parquet",
    compression="snappy"
)
```

### Format Selection

Choose CSV when:

- Loading data into relational databases
- Working with Excel or spreadsheet applications
- Maximum compatibility across tools is required
- Working with legacy systems

Choose Parquet when:

- Building analytical data pipelines
- Working with big data tools (Spark, Hadoop)
- Fast query performance on large datasets is required
- Type preservation is critical
- Working with data lakes or warehouses

### Streaming Output

Stream large datasets directly to files:

```python
# Stream to CSV
config = tm.TransmogConfig.for_memory()
tm.flatten_stream(
    large_data,
    output_path="output/",
    output_format="csv",
    config=config
)

# Stream to compressed Parquet
tm.flatten_stream(
    large_data,
    output_path="output/",
    output_format="parquet",
    compression="snappy",
    row_group_size=50000
)
```

## Null and Empty Value Handling

Control how null and empty values are processed:

### Null Handling Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `NullHandling.SKIP` | Omit null values and empty strings (default) | Clean output |
| `NullHandling.INCLUDE` | Include null values as empty strings | CSV output |

### Skip Mode (Default)

Omit null and empty values from output:

```python
data = {"name": "Product", "description": None, "notes": ""}

config = tm.TransmogConfig(null_handling=tm.NullHandling.SKIP)
result = tm.flatten(data, config=config)

print(result.main[0])
# {'name': 'Product', '_id': '...'}
# description and notes are omitted
```

### Include Mode

Include null and empty values:

```python
config = tm.TransmogConfig(null_handling=tm.NullHandling.INCLUDE)
result = tm.flatten(data, config=config)

print(result.main[0])
# {'name': 'Product', 'description': '', 'notes': '', '_id': '...'}
```

The CSV preset uses `INCLUDE` mode for consistent column structure:

```python
config = tm.TransmogConfig.for_csv()
# Equivalent to: null_handling=tm.NullHandling.INCLUDE, cast_to_string=True
```

## Working with Results

### FlattenResult Properties

Access flattened data through the `FlattenResult` object:

```python
result = tm.flatten(data, name="products")

# Main table
main_data = result.main

# Child tables dictionary
child_tables = result.tables

# All tables including main
all_tables = result.all_tables
```

### Container Operations

`FlattenResult` supports standard container operations:

```python
# Length - number of records in main table
count = len(result)

# Iteration - iterate over main table
for record in result:
    print(record)

# Key access - get specific table
reviews = result["products_reviews"]
main = result["main"]

# Membership - check if table exists
if "products_tags" in result:
    print("Has tags table")

# Keys, values, items
table_names = list(result.keys())
table_data = list(result.values())
table_pairs = list(result.items())
```

### Table Information

Get metadata about tables:

```python
info = result.table_info()
print(info)
# {
#     "products": {
#         "records": 100,
#         "fields": ["name", "price", "_id"],
#         "is_main": True
#     },
#     "products_reviews": {
#         "records": 250,
#         "fields": ["rating", "comment", "_parent_id"],
#         "is_main": False
#     }
# }
```

## Batch Processing

### Batch Size Configuration

Control memory usage through batch size:

```python
# Memory-efficient: small batches
config = tm.TransmogConfig.for_memory()  # batch_size=100
result = tm.flatten(large_data, config=config)

# Performance-optimized: large batches
config = tm.TransmogConfig(batch_size=10000)
result = tm.flatten(large_data, config=config)
```

Batch size guidelines:

- Small datasets (< 10K records): Default (1000)
- Large datasets (10K - 1M records): 5000-10000
- Very large datasets (> 1M records): Use streaming with 100-1000
- Memory-constrained: Use `for_memory()` preset (100)

### Processing Multiple Files

Process multiple files with consistent configuration:

```python
import glob
from pathlib import Path

config = tm.TransmogConfig(separator="_", array_mode=tm.ArrayMode.SEPARATE)
results = []

for file_path in glob.glob("data/*.json"):
    name = Path(file_path).stem
    result = tm.flatten_file(file_path, name=name, config=config)
    results.append(result)

total_records = sum(len(r.main) for r in results)
print(f"Processed {total_records} total records")
```

## Field Naming

### Separator Configuration

Control how nested field names are joined:

```python
# Default: underscore separator
result = tm.flatten(data)
# user_profile_name

# Dot separator
config = tm.TransmogConfig(separator=".")
result = tm.flatten(data, config=config)
# user.profile.name
```

### Path Simplification

Deeply nested paths (4+ components) are automatically simplified to keep first, second-to-last, and last components:

```python
# Input: level1.level2.level3.level4
# Output: level1.level3.level4
```

## Type Handling

### Type Preservation

By default, native types are preserved:

```python
data = {
    "name": "Product",
    "price": 99.99,
    "in_stock": True,
    "quantity": 42
}

result = tm.flatten(data)
print(result.main[0])
# {'name': 'Product', 'price': 99.99, 'in_stock': True, 'quantity': 42}
```

### String Conversion

Convert all values to strings:

```python
config = tm.TransmogConfig(cast_to_string=True)
result = tm.flatten(data, config=config)

print(result.main[0])
# {'name': 'Product', 'price': '99.99', 'in_stock': 'True', 'quantity': '42'}
```

The CSV preset uses string conversion:

```python
config = tm.TransmogConfig.for_csv()
# Equivalent to: cast_to_string=True, null_handling=NullHandling.INCLUDE
```

## Integration Examples

### Database Import

Prepare data for database loading:

```python
config = tm.TransmogConfig(
    id_field="customer_id",
    null_handling=tm.NullHandling.SKIP,
    array_mode=tm.ArrayMode.SEPARATE
)
result = tm.flatten(api_data, name="customers", config=config)
result.save("postgres_import/", output_format="csv")

# SQL import commands:
# COPY customers FROM 'postgres_import/customers.csv' CSV HEADER;
# COPY customers_orders FROM 'postgres_import/customers_orders.csv' CSV HEADER;
```

### Analytics Pipeline

Prepare data for analytics tools:

```python
config = tm.TransmogConfig(
    cast_to_string=False,
    array_mode=tm.ArrayMode.SEPARATE,
    batch_size=10000
)
tm.flatten_stream(
    analytics_data,
    output_path="spark_input/",
    name="events",
    output_format="parquet",
    config=config
)

# Use with Spark:
# df = spark.read.parquet("spark_input/events.parquet")
```

### Data Lake Storage

Prepare data for data lake:

```python
config = tm.TransmogConfig(
    deterministic_ids=True,
    id_fields=["user_id", "timestamp"],
    array_mode=tm.ArrayMode.SMART
)
result = tm.flatten(streaming_data, name="events", config=config)
result.save("data_lake/events/", output_format="parquet")
```

## Next Steps

- **[API Reference](api_reference/api.md)** - Complete function and parameter documentation
- **[Developer Guide](developer_guide/streaming.md)** - Advanced streaming patterns and performance optimization
- **[Contributing](developer_guide/contributing.md)** - Contributing to the project
