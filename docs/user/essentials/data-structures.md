# Data Structures and Flow

> **API Reference**: For detailed API documentation, see the [Types API Reference](../../api/types.md).

This document explains Transmog's type system and how data flows through the processing pipeline.

## Part 1: Type System Overview

Transmog uses specific type definitions for different data structures:

### Core Data Types

- **JsonDict**: JSON-like dictionary structures (`dict[str, Any]`)
- **FlatDict**: Flattened data structures (`dict[str, Any]`)
- **ArrayDict**: Extracted arrays (`dict[str, list[dict[str, Any]]]`)

### Type Safety Benefits

The type system provides several advantages:

- Clear interfaces for data structures
- Consistency in data handling
- IDE support with type hints

### Working with Input Types

Input data can be in various formats:

```python
import transmog as tm
from typing import Dict, List, Any

# Dictionary input
data: dict[str, Any] = {
    "id": 123,
    "name": "Example",
    "items": [{"id": 1}, {"id": 2}]
}

# Python list input
records: list[dict[str, Any]] = [
    {"id": 1, "name": "First"},
    {"id": 2, "name": "Second"}
]

processor = tm.Processor()
result = processor.process(data, entity_name="record")
batch_result = processor.process(records, entity_name="records")
```

### Working with Output Types

Processing results are returned as structured data:

```python
# Main table as list of dictionaries
main_table: list[dict[str, Any]] = result.get_main_table()

# Child tables as dictionary of table names to lists of dictionaries
child_tables: dict[str, list[dict[str, Any]]] = result.get_child_tables()

# Specific child table
items_table: list[dict[str, Any]] = result.get_child_table("record_items")
```

### PyArrow Table Types

When using PyArrow integration, you'll work with PyArrow Tables:

```python
import pyarrow as pa

# Get results as PyArrow Tables
pa_tables: dict[str, pa.Table] = result.to_pyarrow_tables()

# Get a specific table
main_pa_table: pa.Table = pa_tables["main"]
```

### Advanced Type Usage

For advanced use cases, the type system provides additional specificity:

```python
from transmog.types.base import JsonDict, FlatDict, ArrayDict
from transmog.types.result_types import TableData, TableMap

# Using type aliases
def process_nested_data(data: JsonDict) -> FlatDict:
    # Processing code
    pass

def extract_child_data(data: JsonDict) -> ArrayDict:
    # Extraction code
    pass

def transform_table_data(table: TableData) -> TableData:
    # Transformation code
    pass
```

## Part 2: Data Flow Pipeline

Transmog processes data through several logical stages:

### 1. Input Stage

During the input stage, Transmog:

- Determines the source format (JSON, JSONL, CSV, etc.)
- Loads and parses data into memory
- Validates that the data is structurally sound (valid JSON syntax, etc.)

```python
import transmog as tm

# Process data directly
processor = tm.Processor()
result = processor.process({"id": 123, "name": "Example"}, entity_name="record")

# Process a file
result = processor.process_file("data.json", entity_name="records")

# Process a CSV file
result = processor.process_csv("data.csv", entity_name="records")
```

### 2. Preprocessing Stage

Preprocessing standardizes and normalizes input:

```python
# Configure preprocessing options
processor = tm.Processor(
    config=tm.TransmogConfig.default()
    .with_processing(
        cast_to_string=True,   # Convert values to strings
        include_empty=False,   # Skip empty values
        skip_null=True         # Skip null values
    )
)

# Process data with preprocessing
result = processor.process(data, entity_name="record")
```

### 3. Transformation Stage

Data can be pre-processed before Transmog processing:

```python
import transmog as tm

# Define a pre-processing function
def preprocess_data(data):
    # Add derived fields
    if "price" in data and "quantity" in data:
        data["total"] = data["price"] * data["quantity"]
    return data

# Pre-process data before passing to Transmog
preprocessed_data = preprocess_data(original_data)

# Process the pre-processed data
processor = tm.Processor()
result = processor.process(preprocessed_data, entity_name="record")
```

### 4. Processing Stage

During the core processing stage, Transmog:

- Flattens nested structures
- Extracts arrays to separate tables
- Generates IDs and establishes relationships
- Handles special formatting for deeply nested paths

```python
# Process with specific options
processor = tm.Processor(
    config=tm.TransmogConfig.default()
    .with_naming(
        separator=".",                  # Use dots in path names
        max_field_component_length=5,   # Limit component length
        deep_nesting_threshold=4        # Threshold for deep nesting
    )
)

# Process the data
result = processor.process(data, entity_name="record")
```

### 5. Output Stage

The final processed data is returned as a result object:

```python
result = processor.process(data, entity_name="user")

# Access the processed data
main_table = result.get_main_table()
child_tables = result.get_child_tables()

# Convert to different formats
result.write_all_json("output/json")
result.write_all_csv("output/csv")
result.write_all_parquet("output/parquet")
```

## Part 3: Advanced Data Flow Patterns

### Combining Multiple Stages

A typical Transmog processor will combine multiple stages:

```python
import transmog as tm

def calculate_total(data):
    """Calculate order total from items."""
    if "items" in data and isinstance(data["items"], list):
        data["total"] = sum(item.get("price", 0) * item.get("quantity", 0)
                           for item in data["items"])
    return data

# Pre-process the data
order_data = {
    "ID": "ORD-12345",
    "CUSTOMER_ID": "  CUST-789  ",
    "ITEMS": [
        {"PRODUCT_ID": "PROD-1", "QUANTITY": 2, "PRICE": 10.99},
        {"PRODUCT_ID": "PROD-2", "QUANTITY": 1, "PRICE": 24.99}
    ]
}

# Pre-process data
processed_data = calculate_total(order_data)

# Create processor with configuration
processor = tm.Processor(
    config=tm.TransmogConfig.default()
    .with_processing(
        cast_to_string=True,
        skip_null=True
    )
)

# Process the data
result = processor.process(processed_data, entity_name="order")
```

### Processing Collections

Transmog can process collections of data:

```python
# Process a list of users
users = [
    {"id": "user1", "name": "Alice"},
    {"id": "user2", "name": "Bob"},
    {"id": "user3", "name": "Charlie"}
]

processor = tm.Processor()
result = processor.process(users, entity_name="users")

# Access results
processed_users = result.get_main_table()
```

### Configuration and Reuse

Processors can be configured once and reused:

```python
# Create a reusable processor for user data
user_processor = tm.Processor(
    config=tm.TransmogConfig.default()
    .with_processing(cast_to_string=True)
)

# Process multiple data sources with the same processor
result1 = user_processor.process(data1, entity_name="user")
result2 = user_processor.process_file("users.json", entity_name="users")
```

### Working with Dynamic Data

While Transmog provides type definitions, it also handles dynamic data efficiently:

```python
# Processing data with unknown structure
result = processor.process(unknown_data, entity_name="records")

# Inspecting the structure
for table_name in result.get_table_names():
    print(f"Found table: {table_name}")

    if result.get_child_table(table_name):
        fields = list(result.get_child_table(table_name)[0].keys())
        print(f"Fields: {fields}")
```

### Error Handling in the Data Flow

Transmog provides flexible error handling:

```python
# Configure error handling
processor = tm.Processor(
    config=tm.TransmogConfig.default()
    .with_error_handling(
        recovery_strategy="partial",
        allow_malformed_data=True
    )
)

result = processor.process(data, entity_name="user")

# Access error information in records
for record in result.get_main_table():
    if "_error" in record:
        print(f"Record has error: {record['_error']}")
```

## Part 4: Best Practices

### Type System Best Practices

1. **Use Type Hints**: For clarity and IDE support

   ```python
   from transmog.types.base import JsonDict, FlatDict

   def process_data(data: JsonDict) -> FlatDict:
       # Processing code
       pass
   ```

2. **Check Types When Necessary**:

   ```python
   # Checking for expected data types
   if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
       # Process array items
       pass
   ```

3. **Leverage PyArrow Types** when working with large datasets:

   ```python
   import pyarrow as pa

   # Convert to PyArrow tables for efficient processing
   pa_tables = result.to_pyarrow_tables()

   # Use PyArrow compute functions
   import pyarrow.compute as pc
   filtered = pc.filter(pa_tables["main"], pc.greater(pa_tables["main"]["value"], 100))
   ```

### Data Flow Best Practices

1. **Consistent Entity Naming**:

   ```python
   # Use consistent entity names for related data
   customer_result = processor.process(customer_data, entity_name="customer")
   order_result = processor.process(order_data, entity_name="order")
   ```

2. **Pre-process Complex Transformations**:

   ```python
   # Handle complex transformations before Transmog processing
   def normalize_data(data):
       # Normalize field names, clean values, etc.
       return cleaned_data

   processor.process(normalize_data(data), entity_name="record")
   ```

3. **Create Reusable Configurations**:

   ```python
   # Create configurations for different data types
   customer_config = tm.TransmogConfig.default().with_naming(
       separator="/", deep_nesting_threshold=4
   )

   order_config = tm.TransmogConfig.default().with_processing(
       cast_to_string=True, skip_null=True
   )

   # Create processors with these configurations
   customer_processor = tm.Processor(config=customer_config)
   order_processor = tm.Processor(config=order_config)
   ```

4. **Optimize for Dataset Size**:

   ```python
   # For small datasets: standard processing
   result = processor.process(small_data, entity_name="record")

   # For large datasets: chunked processing
   result = processor.process_chunked(large_data, entity_name="record", chunk_size=1000)

   # For very large datasets: streaming processing
   processor.stream_process_file(
       "huge_data.jsonl",
       entity_name="record",
       output_format="parquet",
       output_destination="output_dir"
   )
   ```
