# Working with Data Hierarchies

Transmogrify excels at handling nested data structures and preserving parent-child relationships when flattening complex hierarchical data.

## Understanding the Extraction Process

When processing nested data, Transmogrify:

1. Flattens the main entity (root level)
2. Extracts arrays into separate child tables
3. Preserves relationships between parent and child entities using consistent ID fields

## Parent-Child Relationships

Transmogrify automatically maintains relationships between tables through ID fields:

- Each record in the main table gets a unique `__extract_id` field
- Child records contain a `__parent_id` field that references their parent's `__extract_id`
- Each child table is named based on the parent entity name and the array field's path

```python
import transmogrify as tm

data = {
    "id": "user123",
    "name": "John Doe",
    "orders": [
        {"order_id": "A001", "total": 99.99},
        {"order_id": "A002", "total": 149.99}
    ]
}

processor = tm.Processor()
result = processor.process(data, entity_name="customer")

# Get the main and child tables
main_table = result.get_main_table()
child_table = result.get_child_table("customer_orders")

print(f"Main table has {len(main_table)} records")
print(f"Orders table has {len(child_table)} records")

# Parent record
print(main_table[0])  # Contains __extract_id, id, name

# Child records - each links back to the parent
for order in child_table:
    print(f"Order {order['order_id']} belongs to parent {order['__parent_id']}")
```

## Nested Arrays

Transmogrify handles multiple levels of nesting by creating a separate table for each array:

```python
data = {
    "id": "customer456",
    "orders": [
        {
            "order_id": "B001",
            "items": [
                {"product": "Widget", "quantity": 2},
                {"product": "Gadget", "quantity": 1}
            ]
        }
    ]
}

result = processor.process(data, entity_name="customer")

# Access the tables
main_table = result.get_main_table()
orders_table = result.get_child_table("customer_orders")
items_table = result.get_child_table("customer_orders_items")

# Each level links to its parent
print(f"Customer ID: {main_table[0]['id']}")
print(f"Customer extract ID: {main_table[0]['__extract_id']}")

for order in orders_table:
    print(f"Order {order['order_id']} has parent ID {order['__parent_id']}")
    
for item in items_table:
    print(f"Item {item['product']} has parent ID {item['__parent_id']}")
```

## ID Generation Strategies

Transmogrify offers several strategies for generating IDs:

### Random UUID Strategy (default)

```python
processor = tm.Processor()  # Uses UUID strategy by default
result = processor.process(data, entity_name="customer")
```

### Deterministic ID Strategy

This ensures the same data always produces the same IDs:

```python
from transmogrify.id_generation import DeterministicIdStrategy

processor = tm.Processor(
    id_generation_strategy=DeterministicIdStrategy(
        id_fields=["id"],  # Fields used to generate consistent IDs
        include_entity_name=True  # Include entity name in ID generation
    )
)
result = processor.process(data, entity_name="customer")
```

### Custom ID Strategy

You can implement your own ID generation strategy:

```python
from transmogrify.id_generation import IdGenerationStrategy

class SequentialIdStrategy(IdGenerationStrategy):
    def __init__(self):
        self.counter = 0
        
    def generate_id(self, record, entity_name):
        self.counter += 1
        return f"{entity_name}_{self.counter}"

processor = tm.Processor(id_generation_strategy=SequentialIdStrategy())
result = processor.process(data, entity_name="customer")
```

## Working with Multiple Processing Results

When processing data in batches, you can combine the results:

```python
# Process data in batches
result1 = processor.process_batch(batch1, entity_name="customer")
result2 = processor.process_batch(batch2, entity_name="customer")

# Combine the results
from transmogrify import ProcessingResult
combined = ProcessingResult.combine_results([result1, result2])

# Access the combined data
all_customers = combined.get_main_table()
all_orders = combined.get_child_table("customer_orders")
```

## Best Practices

- Always specify a meaningful `entity_name` when processing data
- For deterministic processing, use `DeterministicIdStrategy` with stable identifier fields
- When processing large datasets, consider batch processing and then combining results
- Use meaningful table names by choosing good entity names and field paths
- Access child tables by name using `get_child_table()` rather than attempting to construct the path manually 