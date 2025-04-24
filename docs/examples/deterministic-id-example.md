# Deterministic ID Generation Examples

This page demonstrates how to use Transmogrify's deterministic ID generation features to maintain consistent identifiers across multiple processing runs.

## Why Use Deterministic IDs?

Deterministic IDs are especially useful in scenarios where you:

- Process data incrementally (in batches over time)
- Reprocess data after failures
- Need to maintain referential integrity across separate processing runs
- Want to avoid duplicate records when loading data into a database

## Basic Example: Root-level Deterministic IDs

The simplest use case is to generate consistent IDs for the root-level records using a specific field.

```python
from transmogrify import Processor

# Sample data with an ID field
data = {
    "id": "RECORD123",
    "name": "Example Record",
    "value": 42
}

# Create processor with root-level deterministic ID
processor = Processor(
    deterministic_id_fields={
        "": "id"  # Empty string refers to the root level
    }
)

# Process the data twice
result1 = processor.process(data, entity_name="example")
result2 = processor.process(data, entity_name="example")

# Get the extract IDs
id1 = result1.to_dict()["main"][0]["__extract_id"]
id2 = result2.to_dict()["main"][0]["__extract_id"]

# IDs will be identical
assert id1 == id2
print(f"Extract ID: {id1}")
```

## Advanced Example: Multi-level Deterministic IDs

For more complex data structures with nested arrays, you can specify different ID fields at different levels.

```python
from transmogrify import Processor

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
        },
        {
            "customer_id": "CUST002",
            "name": "Customer 2",
            "orders": []
        }
    ],
    "products": [
        {"sku": "PROD001", "name": "Product 1", "price": 25.0},
        {"sku": "PROD002", "name": "Product 2", "price": 50.0}
    ]
}

# Create processor with deterministic IDs at multiple levels
processor = Processor(
    deterministic_id_fields={
        "": "id",                       # Root level uses "id" field
        "customers": "customer_id",     # Customers array uses "customer_id" field
        "customers_orders": "order_id", # Orders array uses "order_id" field
        "products": "sku"               # Products array uses "sku" field
    }
)

# Process the data multiple times
result1 = processor.process(data, entity_name="store")
result2 = processor.process(data, entity_name="store")

# Extract tables
tables1 = result1.to_dict()
tables2 = result2.to_dict()

# Verify root ID consistency
assert tables1["main"][0]["__extract_id"] == tables2["main"][0]["__extract_id"]

# Verify customer ID consistency (after sorting)
customers1 = sorted(tables1["store_customers"], key=lambda x: x["customer_id"])
customers2 = sorted(tables2["store_customers"], key=lambda x: x["customer_id"])
for i in range(len(customers1)):
    assert customers1[i]["__extract_id"] == customers2[i]["__extract_id"]

# Verify product ID consistency (after sorting)
products1 = sorted(tables1["store_products"], key=lambda x: x["sku"])
products2 = sorted(tables2["store_products"], key=lambda x: x["sku"])
for i in range(len(products1)):
    assert products1[i]["__extract_id"] == products2[i]["__extract_id"]

print("All IDs are consistent across processing runs!")
```

## Using Wildcard Patterns

You can use wildcard patterns to apply the same ID field to multiple paths.

```python
from transmogrify import Processor

# Data with consistent ID field names
data = {
    "id": "ROOT123",
    "sections": [
        {"id": "SEC001", "name": "Section 1"},
        {"id": "SEC002", "name": "Section 2"}
    ],
    "categories": [
        {"id": "CAT001", "name": "Category 1"},
        {"id": "CAT002", "name": "Category 2"}
    ]
}

# Use wildcard pattern for all paths
processor = Processor(
    deterministic_id_fields={
        "*": "id"  # Use "id" field at all levels
    }
)

# Process the data
result = processor.process(data, entity_name="content")
tables = result.to_dict()

# All tables will use the "id" field for deterministic IDs
print("Main ID:", tables["main"][0]["__extract_id"])
print("Section IDs:", [s["__extract_id"] for s in tables["content_sections"]])
print("Category IDs:", [c["__extract_id"] for c in tables["content_categories"]])
```

## Custom ID Generation Function

For complex ID generation requirements, you can provide a custom function.

```python
import uuid
from transmogrify import Processor

# Define custom ID generation function
def custom_id_generator(record):
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
        return str(uuid.uuid4())  # Random UUID

# Create processor with custom strategy
processor = Processor(id_generation_strategy=custom_id_generator)

# Process a complex data structure
data = {
    "id": "STORE123",
    "customers": [{"customer_id": "CUST001", "name": "Customer 1"}],
    "products": [{"sku": "PROD001", "name": "Product 1"}]
}

result = processor.process(data, entity_name="store")
tables = result.to_dict()

# IDs will have custom prefixes
print("Root ID:", tables["main"][0]["__extract_id"])  # ROOT-STORE123
print("Customer ID:", tables["store_customers"][0]["__extract_id"])  # CUST-CUST001
print("Product ID:", tables["store_products"][0]["__extract_id"])  # PROD-PROD001
```

## Incremental Data Processing Example

This example demonstrates how deterministic IDs help with incremental data processing.

```python
from transmogrify import Processor

# Initial data batch
initial_data = {
    "id": "STORE001",
    "name": "Main Store",
    "customers": [
        {"customer_id": "CUST001", "name": "Customer 1"},
        {"customer_id": "CUST002", "name": "Customer 2"}
    ]
}

# Second data batch (with updates and new records)
additional_data = {
    "id": "STORE001",
    "name": "Main Store (Updated)",
    "customers": [
        {"customer_id": "CUST002", "name": "Customer 2 (Updated)"},  # Updated
        {"customer_id": "CUST003", "name": "Customer 3"}  # New
    ]
}

# Create processor with deterministic IDs
processor = Processor(
    deterministic_id_fields={
        "": "id",
        "customers": "customer_id"
    }
)

# Process initial data
initial_result = processor.process(initial_data, entity_name="store")
initial_tables = initial_result.to_dict()

# Process additional data
additional_result = processor.process(additional_data, entity_name="store")
additional_tables = additional_result.to_dict()

# Root record will have same ID in both batches
root_id_initial = initial_tables["main"][0]["__extract_id"]
root_id_additional = additional_tables["main"][0]["__extract_id"]
assert root_id_initial == root_id_additional

# Extract customer IDs for comparison
initial_customers = {c["customer_id"]: c["__extract_id"] for c in initial_tables["store_customers"]}
additional_customers = {c["customer_id"]: c["__extract_id"] for c in additional_tables["store_customers"]}

# CUST002 will have the same ID in both batches
assert initial_customers["CUST002"] == additional_customers["CUST002"]

# CUST003 only exists in the additional batch
assert "CUST003" in additional_customers
assert "CUST003" not in initial_customers

print("Incremental processing succeeded with consistent IDs!")
```

## Best Practices

1. **Choose stable fields** for deterministic IDs that don't change between processing runs
2. **Use business keys** when available (e.g., customer IDs, order numbers)
3. **Test with a small dataset first** to verify ID consistency
4. **Be aware of missing fields** - if a specified ID field is missing, Transmogrify will fall back to random UUIDs
5. **Consider using a custom ID function** for complex scenarios where multiple fields need to be combined or special formatting is required

For more details, see the [Deterministic IDs user guide](../user/deterministic-ids.md). 