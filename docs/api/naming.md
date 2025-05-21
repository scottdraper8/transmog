# Naming API

> **User Guide**: For usage guidance and examples, see the [Naming System User Guide](../user/processing/naming.md).

This document describes the naming functionality in Transmog.

## Table Naming Convention

Transmog uses a consistent table naming convention:

```python
from transmog.naming.conventions import (
    get_standard_field_name,
    get_table_name,
    handle_deeply_nested_path,
    sanitize_name
)
```

### Field and Table Names

Transmog uses a simplified naming approach that combines field names with separators.
Special handling is only provided for deeply nested structures (>4 layers by default).

### Table Naming Convention

The naming convention simply combines field names with separators:

- First level arrays: `<entity>_<arrayname>`
- Nested arrays: `<entity>_<path_to_array>_<arrayname>`
- Deeply nested arrays: `<entity>_<first_component>_nested_<last_component>`

Examples:

```python
# Get table name for a first level array
table_name = get_table_name("orders", "customer")
# Returns "customer_orders"

# Get table name for a nested array (second level)
table_name = get_table_name("orders_items", "customer", parent_path="orders")
# Returns "customer_orders_items"

# Get table name for a deeply nested array
table_name = get_table_name("packages", "customer", parent_path="orders_shipments", deeply_nested_threshold=4)
# Returns "customer_orders_nested_packages" (deeply nested path handling)
```

## Naming Conventions

### Standard Field Names

```python
# Get standardized field name
field_name = get_standard_field_name("user.address.street")
# Returns "user_address_street"

# Get standardized field name with custom separator
field_name = get_standard_field_name("user.address.street", separator=".")
# Returns "user.address.street"
```

### Table Names

```python
# Get table name for a path
table_name = get_table_name("orders", "customer", separator="_")
# Returns "customer_orders"

# Get table name with nested path
table_name = get_table_name(
    "shipping_address",
    "customer",
    parent_path="orders",
    separator="_"
)
# Returns "customer_orders_shipping_address"
```

### Handling Deeply Nested Paths

```python
# Handle a deeply nested path
simplified_path = handle_deeply_nested_path(
    "level1_level2_level3_level4_level5",
    separator="_",
    deeply_nested_threshold=4
)
# Returns "level1_nested_level5"
```

### Name Sanitization

```python
# Sanitize a name for use in databases/files
clean_name = sanitize_name("User Name (with special chars)")
# Returns "user_name_with_special_chars"

# Sanitize with custom separator
clean_name = sanitize_name("User Name", separator=".")
# Returns "user.name"
```
