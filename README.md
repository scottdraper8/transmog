# Transmog

[![PyPI version](https://img.shields.io/pypi/v/transmog.svg?logo=pypi)](https://pypi.org/project/transmog/)
[![Python versions](https://img.shields.io/badge/python-3.9%2B-blue?logo=python)](https://pypi.org/project/transmog/)
[![License](https://img.shields.io/github/license/scottdraper8/transmog.svg?logo=github)](https://github.com/scottdraper8/transmog/blob/main/LICENSE)

Transform nested data into flat tables with a simple, intuitive API.

## Features

- **Simple API**: One function does it all - `flatten()`
- **Smart Defaults**: Works out of the box for 90% of use cases
- **Multiple Formats**: JSON, CSV, Parquet, and more
- **Preserves Relationships**: Parent-child links maintained automatically
- **Flexible**: Customize separators, IDs, error handling, and more when needed

## Installation

```bash
pip install transmog
```

## Quick Start

```python
import transmog as tm

# Flatten nested data with one line
result = tm.flatten({"name": "Product", "specs": {"cpu": "i7", "ram": "16GB"}})

# Access the flattened data
print(result.main)
# [{'name': 'Product', 'specs_cpu': 'i7', 'specs_ram': '16GB', '_id': '...'}]

# Save to any format
result.save("output.json")
result.save("output.csv")
result.save("output.parquet")
```

## Common Use Cases

### Flatten Complex JSON

```python
data = {
    "id": 1,
    "user": {
        "name": "Alice",
        "email": "alice@example.com"
    },
    "orders": [
        {"id": 101, "amount": 99.99},
        {"id": 102, "amount": 45.50}
    ]
}

result = tm.flatten(data, name="customer")

# Main table has user data
print(result.main)
# [{'id': 1, 'user_name': 'Alice', 'user_email': 'alice@...', '_id': '...'}]

# Orders are in a separate table with parent reference
print(result.tables['customer_orders'])
# [{'id': 101, 'amount': 99.99, '_parent_id': '...'}, ...]
```

### Use Existing IDs

```python
# Use an existing field as ID instead of generating synthetic ones
result = tm.flatten(data, id_field="customer_id")

# Or map different fields for different tables
result = tm.flatten(data, id_field={
    "customers": "customer_id",
    "customers_orders": "order_id"
})
```

### Custom Separators

```python
# Use dots instead of underscores
result = tm.flatten(data, separator=".")
print(result.main[0]['user.name'])  # Instead of 'user_name'
```

### Error Handling

```python
# Skip bad records instead of failing
result = tm.flatten(messy_data, errors="skip")

# Or just warn about issues
result = tm.flatten(messy_data, errors="warn")
```

### Working with Files

```python
# Automatically detects format
result = tm.flatten_file("data.json")
result = tm.flatten_file("data.csv")

# Save with format detection
result.save("output.parquet")
```



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

- [API Reference](https://scottdraper8.github.io/transmog/api.html)
- [Examples](examples/)
- [Migration from v1.0](docs/migration.md)

## License

MIT License
