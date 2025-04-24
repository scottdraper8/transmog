# ProcessingResult API Reference

The `ProcessingResult` class encapsulates the result of processing data with Transmog. It provides access to the processed data in various formats.

## Import

```python
from transmog import ProcessingResult
```

## Properties and Methods

### Core Table Access

```python
# Get the main table (root level records)
result.get_main_table() -> List[Dict[str, Any]]

# Get names of all available tables
result.get_table_names() -> List[str]

# Get a specific child table by name
result.get_child_table(table_name: str) -> List[Dict[str, Any]]
```

### Dictionary Output

```python
# Get all tables as dictionaries
result.to_dict() -> Dict[str, List[Dict[str, Any]]]

# Get all tables as JSON-serializable dictionaries
result.to_json_objects() -> Dict[str, List[Dict[str, Any]]]
```

### PyArrow Output

```python
# Get all tables as PyArrow tables
result.to_pyarrow_tables() -> Dict[str, pyarrow.Table]
```

### Bytes Output

```python
# Get all tables as JSON bytes
result.to_json_bytes(indent: Optional[int] = None) -> Dict[str, bytes]

# Get all tables as CSV bytes
result.to_csv_bytes(delimiter: str = ",", include_header: bool = True) -> Dict[str, bytes]

# Get all tables as Parquet bytes
result.to_parquet_bytes(compression: str = "snappy") -> Dict[str, bytes]
```

### File Output

```python
# Write all tables to JSON files
result.write_all_json(
    base_path: str,
    indent: Optional[int] = None
) -> Dict[str, str]

# Write all tables to CSV files
result.write_all_csv(
    base_path: str,
    delimiter: str = ",",
    include_header: bool = True
) -> Dict[str, str]

# Write all tables to Parquet files
result.write_all_parquet(
    base_path: str,
    compression: str = "snappy"
) -> Dict[str, str]
```

### Static Methods

```python
# Combine multiple ProcessingResult objects
@staticmethod
def combine_results(
    results: List[ProcessingResult]
) -> ProcessingResult
```

## Examples

### Accessing Tables

```python
import transmog as tm

# Process some data
processor = tm.Processor()
result = processor.process(data, entity_name="example")

# Get the main table (contains root level records)
main_table = result.get_main_table()
print(f"Main table has {len(main_table)} records")

# Get all available table names
table_names = result.get_table_names()
print(f"Available tables: {table_names}")

# Access a specific child table
if "example_orders" in table_names:
    orders_table = result.get_child_table("example_orders")
    print(f"Orders table has {len(orders_table)} records")
```

### Converting to Various Formats

```python
# Get all tables as Python dictionaries
tables = result.to_dict()
main_dict = tables["main"]
orders_dict = tables.get("example_orders", [])

# Get as PyArrow tables (requires pyarrow)
pa_tables = result.to_pyarrow_tables()
pa_main = pa_tables["main"]
print(f"PyArrow table has {pa_main.num_rows} rows and {pa_main.num_columns} columns")

# Get as JSON bytes
json_bytes = result.to_json_bytes(indent=2)
with open("example.json", "wb") as f:
    f.write(json_bytes["main"])
```

### Writing to Files

```python
# Write all tables as JSON files
json_paths = result.write_all_json(
    base_path="output/json",
    indent=2
)
print(f"JSON files written to: {json_paths}")

# Write as CSV files
csv_paths = result.write_all_csv(
    base_path="output/csv",
    delimiter=",",
    include_header=True
)
print(f"CSV files written to: {csv_paths}")

# Write as Parquet files
parquet_paths = result.write_all_parquet(
    base_path="output/parquet",
    compression="snappy"
)
print(f"Parquet files written to: {parquet_paths}")
```

### Combining Multiple Results

```python
# Process data in batches
processor = tm.Processor()
result1 = processor.process_batch(batch1, entity_name="example")
result2 = processor.process_batch(batch2, entity_name="example")

# Combine the results
combined = ProcessingResult.combine_results([result1, result2])
print(f"Combined main table has {len(combined.get_main_table())} records")
```

## Performance Considerations

- For large datasets, consider using bytes methods (`to_X_bytes()`) to avoid creating intermediate Python objects
- When using PyArrow, ensure you have enough memory for the conversion
- For file output, ensure the target directories exist before writing
- For very large results, use file-based operations rather than in-memory processing 