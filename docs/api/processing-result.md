# Result API Reference

> **User Guide**: For usage guidance and examples, see the [Output Formats Guide](../user/output/output-formats.md).

This document describes the result classes in Transmog.

## FlattenResult

The `FlattenResult` class is the primary result type in Transmog v1.1.0. It encapsulates the result of processing data and provides intuitive access to the processed tables and convenient methods for saving and converting data.

### Import

```python
import transmog as tm

result = tm.flatten(data, name="customers")
```

### Properties

```python
# Access the main table (root level records)
result.main -> list[dict[str, Any]]

# Access all tables as a dictionary
result.tables -> dict[str, list[dict[str, Any]]]
```

### Methods

#### Saving Results

```python
# Save all tables to files with automatic format detection from extension
result.save("output.json")  # Save as JSON
result.save("output.csv")   # Save as CSV
result.save("output.parquet")  # Save as Parquet

# Save to a directory with specified format
result.save("output_directory", format="json")
result.save("output_directory", format="csv")
result.save("output_directory", format="parquet")

# Save with format-specific options
result.save(
    "output.csv",
    format="csv",
    delimiter="|",
    include_header=True
)

result.save(
    "output.parquet",
    format="parquet",
    compression="zstd",
    row_group_size=10000
)
```

#### Converting to Other Formats

```python
# Convert to PyArrow tables
pa_tables = result.to_pyarrow()
main_table = pa_tables["main"]
```

#### Iterating Over Results

```python
# Iterate over all tables (memory-efficient)
for table_name, records in result:
    print(f"Table {table_name} has {len(records)} records")

# Iterate over specific tables
for table_name, records in result.iter_tables(["customers_orders"]):
    print(f"Processing {table_name}")
    for record in records:
        print(record)
```

### Examples

#### Basic Usage

```python
import transmog as tm

# Process data
data = {
    "id": 1,
    "name": "John Doe",
    "orders": [
        {"id": 101, "product": "Laptop", "price": 999.99},
        {"id": 102, "product": "Mouse", "price": 24.99}
    ]
}

result = tm.flatten(data, name="customer")

# Access main table
print(f"Main table has {len(result.main)} records")
print(result.main[0])  # {"id": 1, "name": "John Doe", "_id": "..."}

# Access child tables
orders = result.tables["customer_orders"]
print(f"Orders table has {len(orders)} records")

# Save results
result.save("output", format="json")  # Creates output/main.json, output/customer_orders.json
```



#### Memory-Efficient Processing

```python
# Process a large file with memory optimization
result = tm.flatten_file(
    "large_data.json",
    name="records",
    low_memory=True,
    chunk_size=1000
)

# Save directly to Parquet for efficient storage
result.save("output", format="parquet", compression="zstd")
```

## Internal ProcessingResult (Legacy)

> Note: The `ProcessingResult` class is now considered an internal implementation detail. Most users should use the new `FlattenResult` class instead.

For advanced users who need direct access to the underlying result object, the legacy `ProcessingResult` class can still be imported from the internal module:

```python
from transmog.process import ProcessingResult
```

The internal `ProcessingResult` class provides the implementation for the new `FlattenResult` class and maintains backward compatibility with existing code.

### Migration from ProcessingResult to FlattenResult

If you're currently using the `ProcessingResult` class, here's how to migrate to the new API:

| Old API (v1.0.6) | New API (v1.1.0) |
|------------------|------------------|
| `result.get_main_table()` | `result.main` |
| `result.get_child_table("customers_orders")` | `result.tables["customers_orders"]` |
| `result.get_table_names()` | `result.tables.keys()` |
| `result.write_all_json("output")` | `result.save("output", format="json")` |
| `result.write_all_csv("output")` | `result.save("output", format="csv")` |
| `result.write_all_parquet("output")` | `result.save("output", format="parquet")` |
| `result.to_dict()` | `result.tables` |
| `result.to_pyarrow_tables()` | `result.to_pyarrow()` |
