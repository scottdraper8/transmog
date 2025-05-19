# Naming and Abbreviation System

> **API Reference**: For detailed API documentation, see the [Naming API Reference](../../api/naming.md).

Transmog includes a comprehensive naming system that handles field and table naming conventions,
with intelligent abbreviation to manage lengthy path names.

## Table Naming Convention

When processing nested arrays, Transmog uses a consistent table naming convention:

- **First level arrays**: `<entity>_<arrayname>`
- **Nested arrays**: `<entity>_<intermediate_path>_<arrayname>`

For nested arrays, intermediate path components are abbreviated to 4 characters by default when their
length exceeds this limit.

### Examples

For a data structure like:

```json
{
  "customers": [
    {
      "orders": [
        {
          "items": [...]
        }
      ]
    }
  ]
}
```

The generated tables would be:

- `root_customers`
- `root_customers_orders`
- `root_customers_orders_items`

With default abbreviation settings (max 4 chars), deep nestings become:

```json
{
  "customers": [
    {
      "orders": [
        {
          "line_items": [
            {
              "product_details": [...]
            }
          ]
        }
      ]
    }
  ]
}
```

Would generate:

- `root_customers`
- `root_customers_orders`
- `root_customers_orders_line_items` (or `root_customers_orders_line` if abbreviated)
- `root_customers_orders_line_prod_details` (abbreviated intermediate components)

## Field Naming

Fields follow a similar pattern but typically include the full path:

```json
{
  "customer": {
    "billing_address": {
      "street": "123 Main St"
    }
  }
}
```

Would be flattened to:

```text
"customer_billing_address_street": "123 Main St"
```

With field abbreviation, this might become:

```text
"customer_bill_addr_street": "123 Main St"
```

## Customizing Naming Behavior

You can customize the naming behavior through the `TransmogConfig`:

```python
from transmog import TransmogConfig, Processor

# Custom naming configuration
config = TransmogConfig.default().with_naming(
    separator="_",                     # Character to separate path components
    abbreviate_table_names=True,       # Enable table name abbreviation
    abbreviate_field_names=True,       # Enable field name abbreviation
    max_table_component_length=4,      # Max length for table path components
    max_field_component_length=5,      # Max length for field path components
    preserve_root_component=True,      # Don't abbreviate root components
    preserve_leaf_component=True,      # Don't abbreviate leaf components
    custom_abbreviations={             # Custom abbreviation dictionary
        "information": "info",
        "configuration": "config"
    }
)

processor = Processor(config=config)
```

The naming system balances readability with practical length constraints, allowing you to
control how verbose or compact your output data structure will be.

## Abbreviation System

The abbreviation system handles lengthy field and table names by intelligently truncating
path components while preserving the most important parts.

## How Field Names are Generated

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

## Configuration Options

The abbreviation system can be controlled through the configuration API:

```python
config = (
    TransmogConfig.default()
    .with_naming(
        abbreviate_field_names=True,       # Enable/disable abbreviation
        abbreviate_table_names=True,       # Enable/disable for table names
        max_field_component_length=4,      # Max length for components
        max_table_component_length=4,      # Max length for table name components
        preserve_root_component=True,      # Preserve root component
        preserve_leaf_component=True,      # Preserve leaf component
        custom_abbreviations={             # Custom abbreviation dictionary
            "information": "info",
            "address": "addr"
        }
    )
)
```

## Custom Abbreviations

You can provide a dictionary of custom abbreviations to be used instead of simple truncation:

```python
custom_abbrevs = {
    "information": "info",
    "customer": "cust",
    "address": "addr"
}
```

When a component matches a key in this dictionary, the custom abbreviation will be used.

## Performance Considerations

The abbreviation system includes optimizations like LRU caching for frequently used paths to minimize
performance impact.

## SQL Compatibility

Field names are sanitized to ensure SQL compatibility, with spaces and special characters replaced by underscores.

## When to Use Abbreviation

Abbreviation is particularly helpful when:

1. Working with data sources that have deeply nested structures
2. Generating field names that need to fit within database column name length limits
3. Improving readability of flattened data by focusing on the most important parts (roots and leaves)

## Disabling Abbreviation

If you prefer to maintain the original field names, you can disable abbreviation:

```python
config = TransmogConfig.default().with_naming(abbreviate_field_names=False)
```

This will preserve the full path components in field names.
