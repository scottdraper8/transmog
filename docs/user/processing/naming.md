# Naming and Abbreviation System

> **API Reference**: For detailed API documentation, see the [Naming API Reference](../../api/naming.md).

Transmog includes a flexible naming and abbreviation system that helps manage field and table names when
converting nested structures to flattened formats.

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

## Abbreviation System

To handle long nested paths that could result in unwieldy field names, Transmog provides an abbreviation system that:

1. Preserves root components by default
2. Preserves leaf components by default
3. Abbreviates intermediate components by truncating them

### Examples

For a deeply nested path like `customer_shipping_information_address_street`:

- **Default behavior** (preserve root and leaf):
  `customer_ship_addr_street`

- **Root-only preservation**:
  `customer_ship_addr_stre`

- **Leaf-only preservation**:
  `cust_ship_addr_street`

- **No preservation**:
  `cust_ship_addr_stre`

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
