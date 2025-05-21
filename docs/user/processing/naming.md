# Naming System

Transmog provides a simplified naming system that maintains a consistent structure for field and table names
when processing nested JSON data.

## Overview

When transforming nested JSON structures to flat tables, Transmog constructs field names by combining path components:

```json
{
  "customer": {
    "address": {
      "street": "123 Main St"
    }
  }
}
```

This becomes a flat field: `customer_address_street` (using the default `_` separator).

## Table Naming Convention

Transmog uses a consistent naming convention for tables:

1. **Main table**: Named after the entity (e.g., `customer`)
2. **First level arrays**: `<entity>_<arrayname>` (e.g., `customer_orders`)
3. **Nested arrays**: `<entity>_<path_to_array>_<arrayname>` (e.g., `customer_orders_items`)
4. **Deeply nested arrays**: For paths with more than 4 components (configurable), a simplified convention is used:
   `<entity>_<first_component>_nested_<last_component>` (e.g., `customer_orders_nested_packages`)

The deeply nested threshold is configurable (default is 4).

### Examples

Given this hierarchical structure:

```json
{
  "customers": [
    {
      "name": "Customer 1",
      "orders": [
        {
          "id": "order1",
          "line_items": [
            { "product": "A", "quantity": 2 },
            { "product": "B", "quantity": 1 }
          ],
          "shipping": {
            "address": "123 Main St",
            "details": {
              "provider": "Express",
              "tracking": {
                "number": "TR123456",
                "status": "Shipped"
              }
            }
          }
        }
      ]
    }
  ]
}
```

The resulting tables would be:

- `root` (main table)
- `root_customers` (first level array)
- `root_customers_orders` (second level array)
- `root_customers_orders_line_items` (third level array)
- `root_customers_orders_shipping_nested_tracking` (deeply nested structure)

## Field Naming

Field names are constructed by combining path components with a separator:

- Default separator is underscore (`_`)
- Path components are lowercased for consistency
- Special characters are handled for SQL compatibility

## Configuration Options

The naming system can be controlled through the configuration API:

```python
config = (
    TransmogConfig.default()
    .with_naming(
        separator="_",                     # Character to separate path components
        deeply_nested_threshold=4,         # Threshold for when to treat paths as deeply nested
    )
)
```

## SQL Compatibility

Field names are sanitized to ensure SQL compatibility:

- Spaces and special characters are replaced by underscores
- Names starting with numbers are prefixed with "col_"
- Multiple consecutive special characters are collapsed

## When to Use Deeply Nested Handling

Special handling for deeply nested structures is particularly helpful when:

1. Working with data sources that have deeply nested structures (like complex APIs)
2. Generating field names that need to fit within database column name length limits
3. Ensuring that nested paths remain manageable and readable

## Disabling Deeply Nested Handling

If you prefer to maintain the complete path for all structures, you can increase the threshold:

```python
config = TransmogConfig.default().with_naming(deeply_nested_threshold=100)
```

This will preserve the full path components even for deeply nested structures.
