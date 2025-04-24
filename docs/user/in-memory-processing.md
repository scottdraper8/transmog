# In-Memory Processing

Transmogrify offers robust in-memory processing capabilities for JSON data. This guide explains how to use the library to process JSON objects or arrays in memory efficiently.

## Basic In-Memory Processing

The simplest way to process JSON data in memory is using the `process` method:

```python
from transmogrify import Processor

processor = Processor()

# Process a single JSON object
data = {
    "id": 1, 
    "name": "John Smith",
    "age": 30,
    "address": {
        "street": "123 Main St",
        "city": "Boston"
    }
}

result = processor.process(data, entity_name="customers")

# Process a list of JSON objects
data_list = [
    {"id": 1, "name": "John Smith"},
    {"id": 2, "name": "Jane Doe"}
]

result = processor.process(data_list, entity_name="customers")
```

## Memory Optimization Modes

Transmogrify supports different processing modes to optimize memory usage and performance based on your specific needs.

### Available Processing Modes

```python
from transmogrify import Processor
from transmogrify.processor import ProcessingMode

# Create a processor with default settings
processor = Processor()

# Process data using different memory modes
result = processor._process_data(
    data,
    entity_name="customers",
    memory_mode=ProcessingMode.STANDARD  # Default - balances memory and performance
)

result = processor._process_data(
    data,
    entity_name="customers",
    memory_mode=ProcessingMode.LOW_MEMORY  # Optimizes for minimal memory usage
)

result = processor._process_data(
    data,
    entity_name="customers",
    memory_mode=ProcessingMode.HIGH_PERFORMANCE  # Optimizes for processing speed
)
```

### Choosing the Right Memory Mode

1. **Standard Mode** (`ProcessingMode.STANDARD`): The default mode that balances memory usage and performance. This is suitable for most applications.

2. **Low Memory Mode** (`ProcessingMode.LOW_MEMORY`): Optimized for processing large datasets with minimal memory footprint. Use this mode when working with very large datasets that might cause memory pressure.

3. **High Performance Mode** (`ProcessingMode.HIGH_PERFORMANCE`): Optimized for speed at the cost of higher memory usage. This mode is ideal for processing smaller datasets quickly.

## Processing Large Datasets in Chunks

For large datasets that might not fit in memory, use the `process_chunked` method:

```python
from transmogrify import Processor

processor = Processor()

# Process a large list of records in chunks
large_list = [{"id": i, "data": f"data_{i}"} for i in range(10000)]
result = processor.process_chunked(
    large_list,
    entity_name="large_dataset",
    chunk_size=1000  # Process 1000 records at a time
)

# Print the result
print(f"Processed {len(result.get_main_table())} records")
```

### Streaming Data Sources

The `process_chunked` method also supports streaming data sources (any iterable that yields dictionaries), allowing processing of datasets larger than available memory:

```python
# Process a generator of records
def record_generator(count):
    for i in range(count):
        yield {"id": i, "value": f"Item {i}"}

result = processor.process_chunked(
    record_generator(100000),  # Generator that yields 100,000 records
    entity_name="streamed_data",
    chunk_size=500
)
```

## Processing JSON Files

Transmogrify can process JSON files directly, handling both single JSON objects and JSON arrays:

```python
# Process a file containing a JSON object or array
result = processor.process_file("data.json", entity_name="customers")
```

### Processing JSONL Files

JSONL (JSON Lines) files contain one JSON object per line and are ideal for large datasets:

```python
# Process a JSONL file (one JSON object per line)
result = processor.process_file("data.jsonl", entity_name="log_events")
```

You can also use the chunked processing method for even more control:

```python
# Process a JSONL file in chunks
result = processor.process_chunked(
    "data.jsonl",
    entity_name="logs",
    input_format="jsonl",  # Explicitly specify the format
    chunk_size=1000
)
```

## Unified Data Interface

Transmogrify provides a unified interface for handling various data sources. The system automatically detects the input format:

```python
# These all work seamlessly with the same API:
result = processor.process_chunked(data_dict, entity_name="example")  # Dictionary
result = processor.process_chunked(data_list, entity_name="example")  # List of dictionaries
result = processor.process_chunked("data.json", entity_name="example")  # JSON file path
result = processor.process_chunked("data.jsonl", entity_name="example")  # JSONL file path
result = processor.process_chunked(record_generator(), entity_name="example")  # Generator

# You can also specify the format explicitly if needed:
result = processor.process_chunked(
    data_source,
    entity_name="example",
    input_format="jsonl"  # Options: "json", "jsonl", "dict", "auto"
)
```

## Working with Processing Results

The `process` method returns a `ProcessingResult` object, which contains the flattened data:

```python
result = processor.process(data, entity_name="customers")

# Get the main table (flattened records)
main_table = result.get_main_table()

# Get the names of all extracted child tables
child_table_names = result.get_child_table_names()

# Get a specific child table by name
orders_table = result.get_child_table("customers_orders")

# Export all tables to dictionaries
all_tables = result.to_dict()

# Export to JSON
json_data = result.to_json()

# Export to CSV strings
csv_data = result.to_csv()
```

## Advanced Memory Optimization Techniques

### Customize Batch Size

Adjust the batch size to balance between processing speed and memory usage:

```python
# For the entire processor
processor = Processor(batch_size=500)  # Default is 1000

# For a specific process_chunked call
result = processor.process_chunked(
    data_source,
    entity_name="example",
    chunk_size=250  # Override the default batch size
)
```

### Handling Large Arrays

When processing JSON with large arrays, the processor can extract these arrays as separate tables, reducing memory pressure:

```python
data = {
    "id": 1,
    "name": "Customer",
    "orders": [
        {"id": 101, "amount": 50},
        {"id": 102, "amount": 75},
        # ... potentially thousands of orders
    ]
}

# Arrays are automatically extracted as separate tables
result = processor.process(data, entity_name="customers")

# Access the extracted orders
orders_table = result.get_child_table("customers_orders")
```

### Combining Results

When processing large datasets in chunks, results are automatically combined:

```python
# Process a large dataset in chunks
result = processor.process_chunked(
    large_data_source,
    entity_name="events",
    chunk_size=1000
)

# The result contains all processed records combined
total_count = len(result.get_main_table())
print(f"Processed {total_count} records in total") 