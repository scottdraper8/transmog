# Transmog

[![PyPI version](https://img.shields.io/pypi/v/transmog.svg?logo=pypi)](https://pypi.org/project/transmog/)
[![Python versions](https://img.shields.io/badge/python-3.9%2B-blue?logo=python)](https://pypi.org/project/transmog/)
[![License](https://img.shields.io/github/license/scottdraper8/transmog.svg?logo=github)](https://github.com/scottdraper8/transmog/blob/main/LICENSE)

Transform nested data into flat tables with a simple, intuitive API.

## Overview

Transmog transforms nested JSON data into flat, tabular formats while preserving relationships between parent and child records.

**Key Features:**

- Simple one-function API with smart defaults
- Multiple output formats (JSON, CSV, Parquet)
- Automatic relationship preservation
- Memory-efficient streaming for large datasets

## Quick Start

```bash
pip install transmog
```

```python
import transmog as tm

# Transform nested data into flat tables
data = {"product_id": "PROD-123", "name": "Gaming Laptop", "specs": {"cpu": "i7", "ram": "16GB"}}
result = tm.flatten(data, name="products")

# Access flattened data in memory (list of dicts)
print(result.main)
# [{'product_id': 'PROD-123', 'name': 'Gaming Laptop', 'specs_cpu': 'i7', 'specs_ram': '16GB'}]

# Save to files in different formats
result.save("products.csv")        # Single CSV file
result.save("products.parquet")    # Single Parquet file
result.save("products.json")       # Single JSON file (only main table)
```

## Example: Nested JSON to Multiple Tables

Transform complex nested data with arrays intelligently using smart mode (default):

```python
data = {
    "user": {"name": "Alice", "email": "alice@example.com"},
    "tags": ["premium", "verified"],  # Simple array - kept as native array
    "orders": [  # Complex array - exploded to child table
        {"id": 101, "amount": 99.99, "items": ["laptop", "mouse"]},
        {"id": 102, "amount": 45.50, "items": ["keyboard"]}
    ]
}

result = tm.flatten(data, name="customer")

# Main table - flattened user data with native arrays
print(result.main)
# [
#   {
#     'user_name': 'Alice',
#     'user_email': 'alice@example.com',
#     'tags': ['premium', 'verified'],  # Native array!
#     '_id': 'a1b2c3d4-e5f6-4789-abc1-23456789def0'
#   }
# ]

# Complex arrays become separate tables with parent references
print(result.tables["customer_orders"])
# [
#   {'id': '101', 'amount': '99.99', 'items': ['laptop', 'mouse'], '_parent_id': 'a1b2c3d4...', '_id': 'b2c3d4...'},
#   {'id': '102', 'amount': '45.50', 'items': ['keyboard'], '_parent_id': 'a1b2c3d4...', '_id': 'c3d4...'}
# ]

# Access all tables in memory
print(f"Created {len(result.all_tables)} tables:")
print(list(result.all_tables.keys()))
# ['customer', 'customer_orders', 'customer_orders_items']

# Save to different formats for analysis
result.save("analytics/", "csv")       # CSV files for database import
result.save("warehouse/", "parquet")   # Parquet files for data warehouse
result.save("api/", "json")           # JSON files for web applications
```

**Key Options:**

- Custom field separators: `separator="."`
- Use existing IDs: `id_field="customer_id"`
- Error handling: `errors="skip"`
- File processing: `tm.flatten_file("data.json")`

## Advanced Options

For more control over the flattening process:

```python
result = tm.flatten(
    data,
    name="products",
    # Naming options
    separator=".",              # Use dots: user.name instead of user_name
    nested_threshold=3,         # Simplify deeply nested field names
    # ID management
    id_field="sku",            # Use existing field as primary ID
    parent_id_field="_parent",  # Customize parent reference field name
    add_timestamp=True,         # Add processing timestamp to records
    # Array handling (default is "smart")
    arrays="separate",         # Extract all arrays to child tables (vs "smart", "inline", "skip")
    # Data processing
    preserve_types=True,       # Keep original data types (not strings)
    skip_null=False,           # Include null values in output
    skip_empty=False,          # Include empty strings/lists
    # Performance tuning
    batch_size=5000,           # Process more records per batch
    low_memory=True,           # Optimize for memory usage over speed
)
```

## Documentation

Complete documentation is available at
[scottdraper8.github.io/transmog](https://scottdraper8.github.io/transmog), including:

- [Getting Started Guide](https://scottdraper8.github.io/transmog/getting_started.html)
- [User Guide](https://scottdraper8.github.io/transmog/user_guide/file-processing.html)
- [API Reference](https://scottdraper8.github.io/transmog/api_reference/api.html)
- [Developer Guide](https://scottdraper8.github.io/transmog/developer_guide/contributing.html)

## Contributing

For contribution guidelines, development setup, and coding standards,
see the [Contributing Guide](https://scottdraper8.github.io/transmog/development/contributing.html)
in the documentation.

## License

MIT License
