# Deterministic ID Generation

Transmog generates an ID for every record during processing. By default, these IDs are random UUIDs, but in many scenarios, you may want to ensure that the same logical record always gets the same ID, even when processed multiple times. This is where deterministic ID generation comes in.

## Why Use Deterministic IDs?

### Data Consistency in Incremental Loading

When loading data incrementally (processing new or updated records over time), random IDs can create consistency problems:

1. The same record processed twice gets different IDs
2. After deduplication, child records may become orphaned
3. References between records can't be maintained across processing runs

Deterministic IDs solve these problems by ensuring that the same logical record always gets the same ID, even when processed in different batches or at different times.

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
processor = Processor()
```

This is suitable for one-time processing but can create issues in incremental loading scenarios.

### 2. Field-based Deterministic IDs

You can specify which fields to use for generating deterministic IDs at different paths in your data:

```python
# Deterministic IDs based on specific fields
processor = Processor(
    deterministic_id_fields={
        "": "id",                     # Root level uses "id" field
        "customers": "customer_id",   # Customer records use "customer_id" field
        "orders": "order_number"      # Order records use "order_number" field
    }
)
```

This ensures that as long as the specified field values stay the same, the generated IDs will also remain consistent.

### 3. Custom ID Generation

For complex requirements, you can provide a custom function to generate IDs:

```python
def custom_id_generator(record):
    # Generate ID based on multiple fields or complex logic
    company = record.get("company", "")
    record_id = record.get("id", "")
    record_type = record.get("type", "")
    
    return f"{company}-{record_type}-{record_id}"

processor = Processor(id_generation_strategy=custom_id_generator)
```

## Path Patterns in Deterministic ID Configuration

When configuring deterministic IDs, you need to specify the paths where each field should be used. Paths are expressed using the separator character (typically underscore `_`).

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

## Examples

### Basic Example: Root-level Deterministic IDs

```python
from transmog import Processor

# Configure for deterministic IDs at the root level
processor = Processor(
    deterministic_id_fields={"": "id"}
)

# Process data - the root record will use "id" field for deterministic ID
data = {"id": "12345", "name": "Example", "value": 100}
result = processor.process(data, entity_name="test")

# The same data processed again will have the same ID
result2 = processor.process(data, entity_name="test")
assert result.to_dict()["main"][0]["__extract_id"] == result2.to_dict()["main"][0]["__extract_id"]
```

### Advanced Example: Nested Structure with Different ID Fields

```python
from transmog import Processor

# Configure for deterministic IDs at multiple levels
processor = Processor(
    deterministic_id_fields={
        "": "id",                        # Root uses "id" field
        "customers": "customer_id",      # Customers use "customer_id" field
        "customers_orders": "order_id",  # Orders use "order_id" field
        "products": "sku"                # Products use "sku" field
    }
)

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
main_table = tables["main"]
customers_table = tables.get("store_customers", [])
orders_table = tables.get("store_customers_orders", [])
products_table = tables.get("store_products", [])

# The IDs will be consistent if the same data is processed again
```

### Example: Custom ID Strategy

```python
from transmog import Processor

# Define a custom ID generation strategy
def custom_id_strategy(record):
    # For customer records
    if "customer_id" in record:
        return f"CUST-{record['customer_id']}"
    
    # For order records
    elif "order_id" in record:
        return f"ORD-{record['order_id']}"
    
    # For product records
    elif "sku" in record:
        return f"PROD-{record['sku']}"
    
    # For root record
    elif "id" in record:
        return f"ROOT-{record['id']}"
    
    # Default fallback
    else:
        return str(uuid.uuid4())  # Random UUID for other records

# Create processor with custom strategy
processor = Processor(id_generation_strategy=custom_id_strategy)

# Process data
data = {
    "id": "STORE123",
    "customers": [{"customer_id": "CUST001", "name": "Customer 1"}],
    "products": [{"sku": "PROD001", "name": "Product 1"}]
}

result = processor.process(data, entity_name="store")
```

## Best Practices

### Choosing ID Fields

When selecting fields for deterministic IDs:

1. **Choose fields with stable values** that don't change between processing runs
2. **Use business keys or natural identifiers** whenever possible
3. **Ensure the fields are always present** in your data
4. **Consider uniqueness requirements** within each table

### Handling Missing ID Fields

If a record doesn't have the specified field for deterministic ID generation:

1. Transmog will fall back to generating a random UUID
2. You can implement fallback logic in a custom ID generation function
3. Consider data validation or preprocessing to ensure ID fields are always present

### Performance Considerations

- Deterministic ID generation may be slightly slower than random UUIDs
- The performance impact is generally negligible for most use cases
- For high-volume processing, a custom ID generation function can be optimized

## Troubleshooting

### Inconsistent IDs

If records are getting different IDs across processing runs:

1. Verify that the specified ID fields exist in your data
2. Check if the values in those fields are actually consistent across runs
3. Ensure you're using the correct path patterns for your data structure
4. Consider using a custom ID strategy for more complex cases

### Path Matching Issues

If the path matching isn't working as expected:

1. Remember that paths use the separator character (default: `_`) between levels
2. Check path patterns for typos or mismatches with your data structure
3. Use simpler paths first, then add complexity
4. Try a wildcard (`*`) to see if it resolves the issue 