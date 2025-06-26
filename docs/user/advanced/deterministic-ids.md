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
processor = tm.Processor()
```

This is suitable for one-time processing but can create issues in incremental loading scenarios.

### 2. Field-based Deterministic IDs

You can specify which field to use for generating deterministic IDs at different paths in your data:

```python
# Configure deterministic IDs based on specific fields for different tables
config = tm.TransmogConfig.with_deterministic_ids({
    "": "id",                     # Root level uses "id" field
    "customers": "customer_id",   # Customer records use "customer_id" field
    "orders": "order_number"      # Order records use "order_number" field
})
processor = tm.Processor(config=config)

# Or use the factory method
processor = tm.Processor.with_deterministic_ids({
    "": "id",                     # Root level uses "id" field
    "customers": "customer_id",   # Customer records use "customer_id" field
    "orders": "order_number"      # Order records use "order_number" field
})
```

This ensures that as long as the specified field values stay the same, the generated IDs will also remain consistent.

You can also use a single field for all tables:

```python
# Use "id" field for all tables
processor = tm.Processor.with_deterministic_ids("id")
```

> **Note**: The `with_deterministic_ids` method only supports using a single field per table path.
> To generate IDs based on multiple fields, you must use the custom ID generation approach described below.

### 3. Custom ID Generation

For complex requirements, such as combining multiple fields for a single table, you can provide a custom function
to generate IDs:

```python
def custom_id_strategy(record):
    # Generate ID based on multiple fields or complex logic
    company = record.get("company", "")
    record_id = record.get("id", "")
    record_type = record.get("type", "")

    return f"{company}-{record_type}-{record_id}"

# Configure with custom ID generation through config
config = tm.TransmogConfig.with_custom_id_generation(custom_id_strategy)
processor = tm.Processor(config=config)

# Or use the factory method
processor = tm.Processor.with_custom_id_generation(custom_id_strategy)
```

## Path Patterns in Deterministic ID Configuration

When configuring deterministic IDs, you need to specify the paths where each field should be used. Paths
are expressed using the separator character (typically underscore `_`).

### Path Examples

- `""` (empty string): Matches root-level records
- `"customers"`: Matches records in the "customers" array
- `"customers_orders"`: Matches order records nested within customers
- `"*"`: Wildcard that matches any path

### Path Matching Rules

Paths are matched based on the nested structure of your data:

1. The most specific matching path is used
2. If no specific path matches, the processor looks for a wildcard pattern
3. If no path matches, random UUIDs are used as a fallback

## Metadata Fields

Transmog adds several metadata fields during processing, which can be configured:

- `__transmog_id`: The unique identifier for each record
- `__parent_transmog_id`: A reference to the parent record's ID
- `__transmog_datetime`: The timestamp when the extraction occurred
- `__array_field`: The name of the original array field (for child tables)
- `__array_index`: The original index in the array (for child tables)

You can customize these field names through configuration:

```python
config = (
    tm.TransmogConfig.default()
    .with_metadata(
        id_field="record_id",
        parent_field="parent_record_id",
        time_field="processed_at"
    )
)
processor = tm.Processor(config=config)
```

## Using with Configuration Methods

You can also configure deterministic IDs after creating a processor:

```python
# Start with a default processor
processor = tm.Processor()

# Update metadata configuration with deterministic IDs
processor = processor.with_metadata(
    deterministic_id_fields={
        "": "id",
        "customers": "customer_id"
    }
)

# Or directly with the ID generation strategy for multiple fields
def custom_strategy(record):
    return f"CUSTOM-{record.get('id', 'unknown')}"

processor = processor.with_metadata(
    id_generation_strategy=custom_strategy
)
```

## Examples

### Basic Example: Root-level Deterministic IDs

```python
import transmog as tm

# Configure for deterministic IDs at the root level
processor = tm.Processor.with_deterministic_ids({"": "id"})

# Process data - the root record will use "id" field for deterministic ID
data = {"id": "12345", "name": "Example", "value": 100}
result = processor.process(data, entity_name="test")

# The same data processed again will have the same ID
result2 = processor.process(data, entity_name="test")
assert result.get_main_table()[0]["__transmog_id"] == result2.get_main_table()[0]["__transmog_id"]
```

### Example: Nested Structure with Different ID Fields

```python
import transmog as tm

# Configure for deterministic IDs at multiple levels
processor = tm.Processor.with_deterministic_ids({
    "": "id",                        # Root uses "id" field
    "customers": "customer_id",      # Customers use "customer_id" field
    "customers_orders": "order_id",  # Orders use "order_id" field
    "products": "sku"                # Products use "sku" field
})

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

# Process the data
result = processor.process(data, entity_name="store")

# Access tables
tables = result.to_dict()
main_table = result.get_main_table()
customers_table = result.get_child_table("store_customers")
orders_table = result.get_child_table("store_customers_orders")
products_table = result.get_child_table("store_products")

# The IDs will be consistent if the same data is processed again
```

### Example: Multi-Field Custom ID Strategy

```python
import transmog as tm
import uuid

# Define a custom ID generation strategy that combines multiple fields
def custom_id_strategy(record):
    # For customer records with multiple fields
    if "customer_id" in record and "email" in record:
        return f"CUST-{record['customer_id']}-{hash(record['email'])}"

    # For order records with multiple fields
    elif "order_id" in record and "total" in record:
        return f"ORD-{record['order_id']}-{int(record['total'])}"

    # For product records
    elif "sku" in record:
        return f"PROD-{record['sku']}"

    # Default fallback for other records
    else:
        # Generate a random UUID if no identifying field is found
        return f"GEN-{uuid.uuid4()}"

# Configure processor with the custom strategy
processor = tm.Processor.with_custom_id_generation(custom_id_strategy)

# Process data
result = processor.process(data, entity_name="store")
```

## Technical Details

### ID Generation Algorithm

When using field-based deterministic IDs, Transmog:

1. Looks for the configured field in the record
2. If found, hashes the value to create a consistent ID
3. If not found, falls back to a random UUID

### Consistency Guarantees

Deterministic IDs guarantee that:

- The same record processed multiple times will have the same ID
- Different records with different values in the ID field will have different IDs
- The algorithm is stable across versions of Transmog

### Performance Implications

Using deterministic IDs has minimal performance impact compared to random UUIDs.
