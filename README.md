# Transmog

[![PyPI version](https://img.shields.io/pypi/v/transmog.svg?logo=pypi)](https://pypi.org/project/transmog/)
[![Python versions](https://img.shields.io/badge/python-3.10%2B-blue?logo=python)](https://pypi.org/project/transmog/)
[![License](https://img.shields.io/github/license/scottdraper8/transmog.svg?logo=github)](https://github.com/scottdraper8/transmog/blob/main/LICENSE)

Transform nested data into flat tables with a simple, intuitive API.

## Overview

Transmog transforms nested JSON data into flat, tabular formats while preserving relationships between parent and child records.

**Key Features:**

- Simple one-function API with smart defaults
- Multiple output formats (CSV, Parquet)
- Automatic relationship preservation
- Memory-efficient streaming for large datasets

## Installation

**Standard install** (includes Parquet support):

```bash
pip install transmog
```

**Minimal install** (CSV only):

```bash
pip install transmog[minimal]
```

## Quick Start

```python
import transmog as tm

# Transform nested data into flat tables
data = {"product_id": "PROD-123", "name": "Gaming Laptop", "specs": {"cpu": "i7", "ram": "16GB"}}
result = tm.flatten(data, name="products")

# Access flattened data in memory (list of dicts)
print(result.main)
# [{'product_id': 'PROD-123', 'name': 'Gaming Laptop', 'specs_cpu': 'i7', 'specs_ram': '16GB', '_id': '...', '_timestamp': '...'}]

# Save to files in different formats
result.save("products.csv")        # Single CSV file
result.save("products.parquet")    # Single Parquet file
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
#     '_id': '...',
#     '_timestamp': '...'
#   }
# ]

# Complex arrays become separate tables with parent references
print(result.tables["customer_orders"])
# [
#   {'id': 101, 'amount': 99.99, 'items': ['laptop', 'mouse'], '_parent_id': '...', '_id': '...', '_timestamp': '...'},
#   {'id': 102, 'amount': 45.50, 'items': ['keyboard'], '_parent_id': '...', '_id': '...', '_timestamp': '...'}
# ]

# Access all tables in memory
print(f"Created {len(result.all_tables)} tables:")
print(list(result.all_tables.keys()))
# ['customer', 'customer_orders', 'customer_orders_items']

# Save to different formats for analysis
result.save("analytics/", "csv")       # CSV files for database import
result.save("warehouse/", "parquet")   # Parquet files for data warehouse
```

## Configuration

Customize processing behavior with `TransmogConfig`:

```python
# Default configuration
result = tm.flatten(data)

# Include nulls for CSV export (consistent columns)
result = tm.flatten(data, config=tm.TransmogConfig(include_nulls=True))

# Memory-efficient processing (smaller batches)
result = tm.flatten(data, config=tm.TransmogConfig(batch_size=100))

# High-performance processing (larger batches)
result = tm.flatten(data, config=tm.TransmogConfig(batch_size=10000))

```

**File Processing:**

```python
result = tm.flatten("data.json")
```

## Advanced Configuration

For more control over the flattening process:

```python
# Create custom configuration
config = tm.TransmogConfig(
    # Array handling
    array_mode=tm.ArrayMode.SEPARATE,  # Extract all arrays to child tables
    # Options: SMART (default), SEPARATE, INLINE, SKIP

    # ID management
    id_generation="natural",           # Use existing ID field (options: random, natural, hash, or list)
    id_field="sku",                    # Name of ID field to use/create
    parent_field="_parent",            # Customize parent reference field name
    time_field="_timestamp",           # Add processing timestamp to records


    # Data processing
    include_nulls=False,               # Skip null and empty values (default: False)
    max_depth=100,                     # Maximum nesting depth

    # Performance tuning
    batch_size=5000,                   # Process more records per batch
)

result = tm.flatten(data, name="products", config=config)

# ID generation options
config = tm.TransmogConfig(id_generation="random")              # Always generate new UUIDs (default)
config = tm.TransmogConfig(id_generation="natural")             # Use existing ID field (fail if missing)
config = tm.TransmogConfig(id_generation="hash")                # Hash entire record (deterministic)
config = tm.TransmogConfig(id_generation=["user_id", "date"])   # Composite key (deterministic)

# Customize configuration as needed
config = tm.TransmogConfig(include_nulls=True)  # For consistent CSV columns
config.id_field = "product_id"
result = tm.flatten(data, config=config)
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
see the [Contributing Guide](https://scottdraper8.github.io/transmog/developer_guide/contributing.html)
in the documentation.

## License

MIT License
