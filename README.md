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
result = tm.flatten({"name": "Product", "specs": {"cpu": "i7", "ram": "16GB"}})
print(result.main)  # Flattened data: [{'name': 'Product', 'specs_cpu': 'i7', ...}]

# Save in any format
result.save("output.json")
```

## Example

Transform complex nested data into relational tables:

```python
data = {
    "user": {"name": "Alice", "email": "alice@example.com"},
    "orders": [
        {"id": 101, "amount": 99.99},
        {"id": 102, "amount": 45.50}
    ]
}

result = tm.flatten(data, name="customer")

# Main table: [{'user_name': 'Alice', 'user_email': 'alice@...', '_id': '...'}]
# Orders table: [{'id': 101, 'amount': 99.99, '_parent_id': '...'}, ...]
```

**Key Options:**

- Custom field separators: `separator="."`
- Use existing IDs: `id_field="customer_id"`
- Error handling: `errors="skip"`
- File processing: `tm.flatten_file("data.json")`

## Advanced Options

For more control:

```python
result = tm.flatten(
    data,
    name="products",
    # Naming
    separator=".",              # Use dots: user.name
    nested_threshold=3,         # Simplify deeply nested names
    # IDs
    id_field="sku",            # Use existing field as ID
    parent_id_field="_parent",  # Customize parent reference name
    add_timestamp=True,         # Add timestamp to records
    # Arrays
    arrays="inline",           # Keep arrays as JSON instead of separate tables
    # Data handling
    preserve_types=True,       # Keep original types (don't convert to strings)
    skip_null=False,           # Include null values
    skip_empty=False,          # Include empty strings/lists
    # Performance
    batch_size=5000,           # Process in larger batches
    low_memory=True,           # Optimize for low memory usage
)
```

## Documentation

Complete documentation is available at
[scottdraper8.github.io/transmog](https://scottdraper8.github.io/transmog), including:

- [Quick Start Guide](https://scottdraper8.github.io/transmog/quickstart.html)
- [User Guides](https://scottdraper8.github.io/transmog/guides/)
- [API Reference](https://scottdraper8.github.io/transmog/reference/)
- [Advanced Topics](https://scottdraper8.github.io/transmog/advanced/)

## Contributing

For contribution guidelines, development setup, and coding standards,
see the [Contributing Guide](https://scottdraper8.github.io/transmog/development/contributing.html)
in the documentation.

## License

MIT License
