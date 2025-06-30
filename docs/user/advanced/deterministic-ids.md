# Deterministic ID Generation

Transmog generates an ID for every record during processing. By default, these IDs are random UUIDs,
but in many scenarios, you may want to ensure that the same logical record always gets the same ID, even
when processed multiple times. This is where deterministic ID generation comes in.

## Why Use Deterministic IDs?

### Data Consistency in Incremental Loading

When loading data incrementally (processing new or updated records over time), random IDs can create
consistency problems:

1. The same record processed twice gets different IDs
2. After deduplication, child records may become orphaned
3. References between records can't be maintained across processing runs

Deterministic IDs solve these problems by ensuring that the same logical record always gets the same
ID, even when processed in different batches or at different times.

### Use Cases for Deterministic IDs

Deterministic IDs are especially valuable in these scenarios:

- **Incremental data processing**: When you process data in batches over time
- **Reprocessing after failures**: When you need to reprocess data after an error
- **Data refresh scenarios**: When you periodically refresh your entire dataset
- **Cross-system consistency**: When you need IDs to be consistent across different systems
- **Audit and lineage tracking**: When you need to track the same record across different processing stages

## ID Generation Strategies

Transmog supports three approaches for ID generation:

### 1. Random UUIDs (Default)

By default, each record gets a random UUID:

```python
# Default - uses random UUIDs
result = tm.flatten(data, name="entity")
```

This is suitable for one-time processing but can create issues in incremental loading scenarios.

### 2. Field-based Deterministic IDs

You can specify which field to use for generating deterministic IDs at different tables in your data:

```python
# Configure deterministic IDs based on specific fields for different tables
result = tm.flatten(
    data,
    name="store",
    id_field={
        "": "id",                     # Main table uses "id" field
        "store_customers": "customer_id",   # Customer records use "customer_id" field
        "store_orders": "order_number"      # Order records use "order_number" field
    }
)
```

This ensures that as long as the specified field values stay the same, the generated IDs will also remain consistent.

You can also use a single field for all tables:

```python
# Use "id" field for all tables
result = tm.flatten(data, name="entity", id_field="id")
```

### 3. Custom ID Generation

For complex requirements, such as combining multiple fields for a single table, you can provide custom transformation functions:

```python
# Define transformations that generate custom IDs
def generate_customer_id(record):
    # Generate ID based on multiple fields
    return f"{record.get('company', '')}-{record.get('customer_id', '')}"

# Apply transformations during flattening
result = tm.flatten(
    data,
    name="store",
    transforms={
        "customer_id": generate_customer_id  # Transform customer_id field
    },
    id_field="customer_id"  # Use the transformed field as ID
)
```

## Table Names in Deterministic ID Configuration

When configuring deterministic IDs with a dictionary, you need to specify which tables should use which fields:

### Table Name Examples

- `""` (empty string): Represents the main table
- `"store_customers"`: Matches records in the customers table
- `"store_customers_orders"`: Matches order records nested within customers

### Table Matching Rules

Tables are matched based on the generated table names:

1. The main table is represented by an empty string `""`
2. Child tables follow the naming convention `parent_name_array_field`
3. If a table isn't specified in the mapping, random UUIDs are used

## Metadata Fields

Transmog adds several metadata fields during processing:

- `_id`: The unique identifier for each record
- `_parent_id`: A reference to the parent record's ID
- `_datetime`: The timestamp when the extraction occurred (if enabled)
- `_array_field`: The name of the original array field (for child tables)
- `_array_index`: The original index in the array (for child tables)

You can customize the metadata behavior:

```python
# Disable timestamp generation
result = tm.flatten(data, name="entity", add_metadata_timestamp=False)

# Customize ID field name
result = tm.flatten(data, name="entity", metadata_field_names={"id": "record_id"})
```

## Examples

### Basic Example: Root-level Deterministic IDs

```python
import transmog as tm

# Process data with deterministic IDs at the root level
data = {"id": "12345", "name": "Example", "value": 100}
result = tm.flatten(data, name="test", id_field="id")

# The same data processed again will have the same ID
result2 = tm.flatten(data, name="test", id_field="id")
assert result.main[0]["_id"] == result2.main[0]["_id"]
```

### Example: Nested Structure with Different ID Fields

```python
import transmog as tm

# Complex nested data
data = {
    "id": "STORE123",
    "name": "Main Store",
    "customers": [
        {
            "customer_id": "CUST001",
            "name": "Customer 1",
            "orders": [
                {"order_id": "ORD001", "total": 100.0},
                {"order_id": "ORD002", "total": 200.0}
            ]
        }
    ],
    "products": [
        {"sku": "PROD001", "name": "Product 1", "price": 25.0},
        {"sku": "PROD002", "name": "Product 2", "price": 50.0}
    ]
}

# Process with deterministic IDs for different tables
result = tm.flatten(
    data,
    name="store",
    id_field={
        "": "id",                        # Main table uses "id" field
        "store_customers": "customer_id",      # Customers use "customer_id" field
        "store_customers_orders": "order_id",  # Orders use "order_id" field
        "store_products": "sku"                # Products use "sku" field
    }
)

# Access tables
main_table = result.main
customers_table = result.tables["store_customers"]
orders_table = result.tables["store_customers_orders"]
products_table = result.tables["store_products"]

# The IDs will be consistent if the same data is processed again
```

### Example: Multi-Field Custom ID Strategy

```python
import transmog as tm

# Define custom transformation functions for ID generation
def customer_id_generator(record):
    # For customer records with multiple fields
    if "customer_id" in record and "email" in record:
        return f"CUST-{record['customer_id']}-{hash(record['email'])}"
    return record.get("customer_id", "")

def order_id_generator(record):
    # For order records with multiple fields
    if "order_id" in record and "total" in record:
        return f"ORD-{record['order_id']}-{int(record['total'])}"
    return record.get("order_id", "")

# Process with custom ID generation through transforms
result = tm.flatten(
    data,
    name="store",
    transforms={
        "customer_id": customer_id_generator,
        "order_id": order_id_generator
    },
    id_field={
        "store_customers": "customer_id",
        "store_customers_orders": "order_id"
    }
)
```

### Example: Incremental Processing with Deterministic IDs

```python
import transmog as tm

# First batch of data
batch1 = [
    {"id": "1001", "name": "Product A", "category": "Electronics"},
    {"id": "1002", "name": "Product B", "category": "Home"}
]

# Process first batch with deterministic IDs
result1 = tm.flatten(batch1, name="products", id_field="id")
result1.save("output/batch1")

# Second batch with updates and new records
batch2 = [
    {"id": "1002", "name": "Product B", "category": "Home", "price": 29.99},  # Updated
    {"id": "1003", "name": "Product C", "category": "Electronics"}  # New
]

# Process second batch with the same ID field
result2 = tm.flatten(batch2, name="products", id_field="id")
result2.save("output/batch2")

# The record with id=1002 will have the same _id in both batches
```

## Best Practices

1. **Choose appropriate ID fields**: Select fields that are truly unique and stable over time
2. **Be consistent**: Use the same ID field configuration for all processing runs of the same data
3. **Consider composite keys**: For complex data, use transforms to combine multiple fields into a single ID
4. **Test with sample data**: Verify that your ID strategy produces consistent results
5. **Document your approach**: Keep track of which fields are used for ID generation

By using deterministic IDs, you can ensure data consistency across multiple processing runs and simplify incremental data loading scenarios.
