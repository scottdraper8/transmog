# Working with Output Formats

Transmog provides options for outputting processed data in different formats. This guide explains how to use these output methods to fit your specific use case.

## Output Format Categories

Transmog offers three main categories of output formats:

1. **Native Data Structures** - Python objects like dictionaries and PyArrow Tables
2. **Bytes Serialization** - Raw bytes in JSON, CSV, or Parquet format for direct writing
3. **File Output** - Direct writing to files in different formats

## Native Data Structures

### Python Dictionaries

The simplest way to access processed data is as Python dictionaries:

```python
import transmog as tm

# Process data
processor = tm.Processor()
result = processor.process(data)

# Get all tables as dictionaries
tables = result.to_dict()

# Access main table records
main_table = tables["main"]
for record in main_table:
    print(record["id"], record["name"])

# Access child tables
for table_name, records in tables.items():
    if table_name != "main":
        print(f"Child table {table_name}: {len(records)} records")
```

### PyArrow Tables

For high-performance data processing, you can get results as PyArrow Tables:

```python
# Get all tables as PyArrow Tables
tables = result.to_pyarrow_tables()

# Access main table
main_table = tables["main"]
print(f"Schema: {main_table.schema}")
print(f"Rows: {main_table.num_rows}")

# Perform operations on the PyArrow Table
import pyarrow.compute as pc
filtered = main_table.filter(pc.field("score") > 80)
```

### JSON Objects

When working with JSON data, you can get JSON-serializable Python objects:

```python
# Get JSON-serializable objects
json_data = result.to_json_objects()

# Use with json module
import json
json_str = json.dumps(json_data["main"], indent=2)
print(json_str)

# Or directly write to a file
with open("output.json", "w") as f:
    json.dump(json_data["main"], f, indent=2)
```

## Bytes Serialization

### Parquet Bytes

Get raw Parquet bytes for direct writing or transmission:

```python
# Get Parquet bytes
parquet_bytes = result.to_parquet_bytes(compression="snappy")

# Write to a file
with open("output.parquet", "wb") as f:
    f.write(parquet_bytes["main"])

# Or use with an in-memory buffer
import io
buffer = io.BytesIO(parquet_bytes["main"])
```

### CSV Bytes

Get raw CSV bytes:

```python
# Get CSV bytes
csv_bytes = result.to_csv_bytes(include_header=True)

# Write to a file
with open("output.csv", "wb") as f:
    f.write(csv_bytes["main"])

# Or use in memory
import io
buffer = io.BytesIO(csv_bytes["main"])
```

### JSON Bytes

Get raw JSON bytes:

```python
# Get JSON bytes
json_bytes = result.to_json_bytes(indent=2)

# Write to a file
with open("output.json", "wb") as f:
    f.write(json_bytes["main"])
```

## File Output

You can write directly to files:

```python
# Write all tables to Parquet files
parquet_files = result.write_all_parquet(
    base_path="output/parquet",
    compression="snappy"
)

# Write all tables to CSV files
csv_files = result.write_all_csv(
    base_path="output/csv",
    include_header=True
)

# Write all tables to JSON files
json_files = result.write_all_json(
    base_path="output/json",
    indent=2
)
```

## Integration with Other Libraries

### PyArrow Direct Usage

```python
# Work directly with PyArrow tables
tables = result.to_pyarrow_tables()
main_table = tables["main"]

# Use PyArrow functionality
print(f"Number of rows: {main_table.num_rows}")
print(f"Schema: {main_table.schema}")

# Access specific columns
if "category" in main_table.column_names:
    categories = main_table.column("category").to_pylist()
    print(f"Unique categories: {set(categories)}")

# Use PyArrow compute functions
import pyarrow.compute as pc
if "amount" in main_table.column_names:
    # Calculate statistics
    total = pc.sum(main_table["amount"]).as_py()
    average = pc.mean(main_table["amount"]).as_py()
    print(f"Total: {total}, Average: {average}")
    
    # Filter data
    high_value = main_table.filter(pc.greater(main_table["amount"], pc.scalar(1000.0)))
    print(f"High value transactions: {high_value.num_rows}")
```

## Performance Characteristics

- PyArrow Tables: Suited for large datasets and analytics processing
- Bytes Output: Suitable for direct writing to files or streaming
- Dictionary Output: Appropriate for Python processing
- File Output: Provides direct file writing capability

## Memory Usage

For processing large datasets:

```python
import transmog as tm
import json
import io

# Process in chunks
processor = tm.Processor()
chunk_size = 1000
results = []

# Read data in chunks
with open("large_file.json", "r") as f:
    while True:
        chunk = []
        for _ in range(chunk_size):
            line = f.readline()
            if not line:
                break
            chunk.append(json.loads(line))
        
        if not chunk:
            break
            
        # Process the chunk
        result = processor.process_many(chunk)
        
        # Get data as bytes and write immediately
        parquet_bytes = result.to_parquet_bytes()
        with open(f"output/chunk_{len(results)}.parquet", "wb") as out:
            out.write(parquet_bytes["main"])
            
        results.append(result)
```

## Output Format Considerations

Factors to consider when selecting an output format:

1. Data Size: Data volume can affect format choice
2. Performance: Different formats have different performance profiles
3. Memory Usage: Format choices impact memory requirements
4. Integration: Your downstream processing tools may dictate format choice
5. Readability: Formats have varying levels of human readability

## Format Comparison

| Format | Characteristics | Limitations | Common Use Cases |
|--------|----------------|-------------|-----------------|
| Python Dict | Simple, native | Memory usage increases with data size | Small-medium datasets, Python processing |
| PyArrow | Memory-efficient, columnar | Requires additional dependency | Large datasets, analytics, columnar data |
| JSON | Human-readable | Larger file size | API responses, debugging |
| CSV | Widely compatible | Limited to flat data structures | Tabular data, spreadsheet imports |
| Parquet | Columnar storage, compression | Binary format | Data warehousing, analytics pipelines | 