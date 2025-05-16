# ProcessingResult API Reference

The `ProcessingResult` class encapsulates the result of processing data with Transmog. It provides access
to the processed data in various formats.

## Import

```python
from transmog import ProcessingResult, ConversionMode
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

# Get a formatted table name (with abbreviations applied)
result.get_formatted_table_name(table_name: str) -> str
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
result.to_pyarrow_tables(
    conversion_mode: ConversionMode = ConversionMode.EAGER
) -> Dict[str, pyarrow.Table]
```

### Bytes Output

```python
# Get all tables as JSON bytes
result.to_json_bytes(
    indent: Optional[int] = None,
    ensure_ascii: bool = True,
    sort_keys: bool = False,
    separators: Optional[Tuple[str, str]] = None,
    conversion_mode: ConversionMode = ConversionMode.EAGER
) -> Dict[str, bytes]

# Get all tables as CSV bytes
result.to_csv_bytes(
    dialect: str = "excel",
    delimiter: Optional[str] = None,
    include_header: bool = True,
    conversion_mode: ConversionMode = ConversionMode.EAGER
) -> Dict[str, bytes]

# Get all tables as Parquet bytes
result.to_parquet_bytes(
    compression: str = "snappy",
    row_group_size: Optional[int] = None,
    conversion_mode: ConversionMode = ConversionMode.EAGER
) -> Dict[str, bytes]
```

### File Output

```python
# Write all tables to JSON files
result.write_all_json(
    base_path: str,
    indent: Optional[int] = None,
    ensure_ascii: bool = True,
    sort_keys: bool = False,
    separators: Optional[Tuple[str, str]] = None,
    conversion_mode: ConversionMode = ConversionMode.EAGER
) -> Dict[str, str]

# Write all tables to CSV files
result.write_all_csv(
    base_path: str,
    dialect: str = "excel",
    delimiter: Optional[str] = None,
    include_header: bool = True,
    line_terminator: Optional[str] = None,
    quote_strategy: str = "minimal",
    conversion_mode: ConversionMode = ConversionMode.EAGER
) -> Dict[str, str]

# Write all tables to Parquet files
result.write_all_parquet(
    base_path: str,
    compression: str = "snappy",
    row_group_size: Optional[int] = None,
    data_page_size: Optional[int] = None,
    partition_cols: Optional[List[str]] = None,
    conversion_mode: ConversionMode = ConversionMode.EAGER
) -> Dict[str, str]
```

### Conversion Mode

```python
# Set a conversion mode for all operations
result.with_conversion_mode(mode: ConversionMode) -> ProcessingResult
```

### Static Methods

```python
# Combine multiple ProcessingResult objects
@staticmethod
def combine_results(
    results: List[ProcessingResult]
) -> ProcessingResult
```

## Conversion Modes

The `ConversionMode` enum controls how the ProcessingResult handles memory during conversion operations:

```python
from transmog import ConversionMode

# Eager mode - convert and cache immediately
mode = ConversionMode.EAGER

# Lazy mode - convert only when needed
mode = ConversionMode.LAZY

# Memory-efficient mode - discard intermediate data after conversion
mode = ConversionMode.MEMORY_EFFICIENT
```

| Mode | Description | Best For |
|------|-------------|----------|
| `EAGER` | Converts data immediately and keeps all formats in memory. | Interactive analysis, smaller datasets |
| `LAZY` | Converts data only when needed. | One-time processing |
| `MEMORY_EFFICIENT` | Minimizes memory usage by clearing intermediate data. | Very large datasets |

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

# Get the formatted table name
formatted_name = result.get_formatted_table_name("example_orders")
print(f"Formatted table name: {formatted_name}")
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
    indent=2,
    ensure_ascii=False
)
print(f"JSON files written to: {json_paths}")

# Write as CSV files
csv_paths = result.write_all_csv(
    base_path="output/csv",
    dialect="excel",
    include_header=True
)
print(f"CSV files written to: {csv_paths}")

# Write as Parquet files
parquet_paths = result.write_all_parquet(
    base_path="output/parquet",
    compression="snappy",
    partition_cols=["date"]  # Optional partitioning
)
print(f"Parquet files written to: {parquet_paths}")
```

### Memory-Efficient Conversion

```python
from transmog import Processor, ConversionMode

processor = Processor()
result = processor.process(large_data, entity_name="records")

# Use memory-efficient conversion mode
result = result.with_conversion_mode(ConversionMode.MEMORY_EFFICIENT)

# Write files with memory-efficient conversion
result.write_all_csv("output/csv")

# Or specify conversion mode for a specific operation
json_bytes = result.to_json_bytes(
    conversion_mode=ConversionMode.MEMORY_EFFICIENT
)
```

### Combining Multiple Results

```python
# Process data in batches
processor = tm.Processor()
results = []

for batch in batches:
    batch_result = processor.process_batch(batch, entity_name="example")
    results.append(batch_result)

# Combine the results
combined = ProcessingResult.combine_results(results)
print(f"Combined main table has {len(combined.get_main_table())} records")

# Write the combined result
combined.write_all_parquet("output/combined")
```

## Performance Considerations

- For large datasets, use `ConversionMode.MEMORY_EFFICIENT` to minimize memory usage
- For even larger datasets, use the streaming API instead (`stream_process`, etc.)
- When using PyArrow, ensure you have enough memory for the conversion
- For file output, ensure the target directories exist before writing
- For very large results, use Parquet format with compression for efficient storage
