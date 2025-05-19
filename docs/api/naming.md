# Naming API

> **User Guide**: For usage guidance and examples, see the [Naming and Abbreviation System User Guide](../user/processing/naming.md).

This document describes the naming functionality in Transmog.

## Abbreviator

```python
from transmog.naming.abbreviator import (
    abbreviate_field_name,
    abbreviate_table_name,
    merge_abbreviation_dicts
)
```

### Field and Table Name Abbreviation

```python
# Abbreviate a field name
short_name = abbreviate_field_name(
    "customer_billing_information_payment_methods",
    max_component_length=5
)
# Returns "customer_bill_info_paym_methods"

# Abbreviate a table name
short_table = abbreviate_table_name(
    "customer_billing_information_payment_methods",
    parent_entity="customer",
    max_component_length=4
)
# Returns "customer_bill_info_payment_methods"
```

### Table Naming Convention

Transmog uses a consistent table naming convention:

- First level arrays: `<table>_<arrayname>`
- Nested arrays: `<table>_<arra>_<arrayname>` (intermediate nodes abbreviated to 4 chars by default)

Examples:

```python
# Get table name for a first level array
table_name = get_table_name("orders", "customer")
# Returns "customer_orders"

# Get table name for a nested array (second level)
table_name = get_table_name("customer_orders_items", "data")
# Returns "data_cust_items" (intermediate node abbreviated)

# Get table name for a deeply nested array
table_name = get_table_name("customer_orders_shipments_packages", "data")
# Returns "data_cust_orde_ship_packages" (intermediate nodes abbreviated)
```

### Abbreviation Dictionaries

```python
# Create and use custom abbreviations
custom_abbrevs = {"customer": "cust", "information": "info"}

# Use custom abbreviations
short_name = abbreviate_field_name(
    "customer_billing_information_payment_methods",
    max_component_length=5,
    abbreviation_dict=custom_abbrevs
)

# Merge abbreviation dictionaries
additional_abbrevs = {"payment": "pmt", "methods": "mthd"}
merged = merge_abbreviation_dicts(custom_abbrevs, additional_abbrevs)
```

## Naming Conventions

```python
from transmog.naming.conventions import (
    get_standard_field_name,
    get_table_name,
    sanitize_name
)
```

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
table_name = get_table_name("user_orders", separator="_")
# Returns "user_orders"

# Get table name with abbreviation
table_name = get_table_name(
    "customer_billing_information",
    separator="_",
    abbreviate=True,
    max_component_length=5
)
# Returns "cus_bil_inf"
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
