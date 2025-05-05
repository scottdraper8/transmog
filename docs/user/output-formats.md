# Output Formats

Transmog provides flexible output options for the processed data. This guide explains the various output formats available and how to use them effectively.

## Output Format Categories

Transmog offers three main categories of output formats:

1. **Native Data Structures** - Direct access to Python objects
2. **Bytes Serialization** - Raw bytes for direct writing to files or streams
3. **File Output** - Direct writing to files in various formats

## Native Data Structures

### Python Dictionaries

```python
import transmog as tm

processor = tm.Processor()
result = processor.process(data, entity_name="records")

# Get all tables as dictionaries
tables = result.to_dict()

# Access specific tables
main_table = tables["main"]
child_table = tables["child_items"]

# Access individual records
first_record = main_table[0]
child_records = tables["child_items"]
```

### JSON-Serializable Objects

```python
# Get all tables as JSON-serializable Python objects
json_objects = result.to_json_objects()

# Access specific tables
main_table_json = json_objects["main"]
```

### PyArrow Tables

```python
# Get all tables as PyArrow Tables (requires pyarrow)
pa_tables = result.to_pyarrow_tables()

# Access a specific table
main_pa_table = pa_tables["main"]

# Use PyArrow functionality
print(f"Table has {main_pa_table.num_rows} rows and {main_pa_table.num_columns} columns")
main_pa_table.to_pandas()  # Convert to pandas DataFrame
```

## Bytes Serialization

The bytes serialization options provide raw bytes that can be directly written to files or sent over network connections without intermediate files.

### JSON Bytes

```python
# Get all tables as JSON bytes
json_bytes = result.to_json_bytes(indent=2)  # Pretty-printed JSON

# Access bytes for a specific table
main_table_bytes = json_bytes["main"]

# Write bytes directly to a file
with open("main_table.json", "wb") as f:
    f.write(main_table_bytes)
```

### CSV Bytes

```python
# Get all tables as CSV bytes
csv_bytes = result.to_csv_bytes(dialect="excel", include_header=True)

# Access bytes for a specific table
main_table_bytes = csv_bytes["main"]

# Write bytes directly to a file
with open("main_table.csv", "wb") as f:
    f.write(main_table_bytes)
```

### Parquet Bytes

```python
# Get all tables as Parquet bytes (requires pyarrow)
parquet_bytes = result.to_parquet_bytes(compression="snappy")

# Access bytes for a specific table
main_table_bytes = parquet_bytes["main"]

# Write bytes directly to a file
with open("main_table.parquet", "wb") as f:
    f.write(main_table_bytes)
```

## File Output

### JSON Files

```python
# Write all tables to JSON files
json_files = result.write_all_json(
    base_path="output_dir/json",
    indent=2,
    ensure_ascii=False
)

# Files are returned by table name
print(f"Main table written to {json_files['main']}")
```

### CSV Files

```python
# Write all tables to CSV files
csv_files = result.write_all_csv(
    base_path="output_dir/csv",
    dialect="excel",
    include_header=True
)

print(f"Main table written to {csv_files['main']}")
```

### Parquet Files

```python
# Write all tables to Parquet files (requires pyarrow)
parquet_files = result.write_all_parquet(
    base_path="output_dir/parquet",
    compression="snappy",
    partition_cols=None  # Optional partitioning
)

print(f"Main table written to {parquet_files['main']}")
```

## Memory-Efficient Output

Transmog supports memory-efficient output through the `ConversionMode` enum:

```python
from transmog import Processor, ConversionMode

processor = Processor()
result = processor.process(data, entity_name="records")

# Memory-efficient conversion
result.write_all_csv(
    base_path="output_dir/csv",
    conversion_mode=ConversionMode.MEMORY_EFFICIENT
)

# Memory-efficient bytes
parquet_bytes = result.to_parquet_bytes(
    conversion_mode=ConversionMode.MEMORY_EFFICIENT
)
```

## Conversion Modes

Transmog offers flexible memory management through three conversion modes:

### Eager Mode (Default)

`ConversionMode.EAGER` converts data immediately and caches the results for faster repeated access:

```python
# Default mode
result = processor.process(data, entity_name="records")

# Explicitly specify eager mode
csv_bytes1 = result.to_csv_bytes(conversion_mode=ConversionMode.EAGER)
# Second conversion is fast as it's cached
csv_bytes2 = result.to_csv_bytes()  # Reuses cached conversion
```

### Lazy Mode

`ConversionMode.LAZY` converts data only when needed, without caching:

```python
# Set lazy conversion mode
result = result.with_conversion_mode(ConversionMode.LAZY)

# Each conversion is performed from scratch
json_bytes = result.to_json_bytes()
csv_bytes = result.to_csv_bytes()  # Not reusing previous conversions
```

### Memory-Efficient Mode

`ConversionMode.MEMORY_EFFICIENT` minimizes memory usage by clearing intermediate data after conversion:

```python
# Set memory-efficient conversion mode
result = result.with_conversion_mode(ConversionMode.MEMORY_EFFICIENT)

# After this conversion, intermediate data is cleared
result.write_all_parquet("output_dir/parquet")

# Still usable, but requires reconversion from source data
result.write_all_csv("output_dir/csv")  # Reconverts from source
```

### Choosing the Right Mode

- **Eager Mode**: Best for interactive use, small datasets, and when converting to multiple formats
- **Lazy Mode**: Good for one-time conversions and moderate-sized datasets
- **Memory-Efficient Mode**: Best for large datasets and memory-constrained environments

```python
from transmog import Processor, ConversionMode

processor = Processor()

# Choose strategy based on dataset size
if dataset_size < 10_000:
    # Small dataset - use eager mode
    result = processor.process(data, entity_name="records")
    # Default is EAGER mode
elif dataset_size < 100_000:
    # Medium dataset - use lazy mode
    result = processor.process(data, entity_name="records")
    result = result.with_conversion_mode(ConversionMode.LAZY)
else:
    # Large dataset - use memory-efficient mode
    result = processor.process(data, entity_name="records")
    result = result.with_conversion_mode(ConversionMode.MEMORY_EFFICIENT)
```

## Format-Specific Options

### JSON Options

```python
# JSON write options
result.write_all_json(
    base_path="output_dir",
    indent=2,            # Pretty-print with 2-space indentation
    ensure_ascii=False,  # Allow non-ASCII characters
    sort_keys=True,      # Sort keys alphabetically
    separators=(',', ':')  # Custom separators
)

# JSON bytes options
json_bytes = result.to_json_bytes(
    indent=4,
    ensure_ascii=False
)
```

### CSV Options

```python
# CSV write options
result.write_all_csv(
    base_path="output_dir",
    dialect="excel",     # CSV dialect
    delimiter=",",       # Column delimiter
    include_header=True, # Include header row
    line_terminator="\n",  # Line ending
    quote_strategy="minimal"  # When to quote fields
)

# CSV bytes options
csv_bytes = result.to_csv_bytes(
    dialect="excel-tab",
    include_header=True
)
```

### Parquet Options

```python
# Parquet write options
result.write_all_parquet(
    base_path="output_dir",
    compression="snappy",  # Compression algorithm
    partition_cols=["date"],  # Hive-style partitioning
    row_group_size=100000,    # Row group size
    data_page_size=1024*1024  # Data page size
)

# Parquet bytes options
parquet_bytes = result.to_parquet_bytes(
    compression="zstd",
    row_group_size=50000
)
```

## Streaming Output

For processing large datasets, you can use streaming output that writes directly to the desired format:

```python
from transmog import Processor

processor = Processor()

# Stream directly to output format
processor.stream_process(
    data=large_data,
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir",
    compression="snappy"  # Format-specific option
)

# Stream a file directly to output format
processor.stream_process_file(
    file_path="large_data.json",
    entity_name="records",
    output_format="csv",
    output_destination="output_dir",
    include_header=True  # Format-specific option
)
```

## Writing to Memory Buffers

You can write directly to memory buffers using the bytes serialization options:

```python
import io

# Create a memory buffer
buffer = io.BytesIO()

# Write JSON bytes to the buffer
json_bytes = result.to_json_bytes()["main"]
buffer.write(json_bytes)

# Or use streaming to write directly to a buffer
processor.stream_process(
    data=data,
    entity_name="records",
    output_format="json",
    output_destination=buffer
)
```

## Converting Between Formats

You can convert from one format to another using the processing result:

```python
# Process data to get a result
result = processor.process(data, entity_name="records")

# Write to multiple formats
result.write_all_json("output_dir/json")
result.write_all_csv("output_dir/csv")
result.write_all_parquet("output_dir/parquet")

# Or process directly to a specific format
processor.process_to_format(
    data=data,
    entity_name="records",
    output_format="parquet",
    output_path="output_dir"
)
```

## Supported Format Matrix

| Format | Native Objects | Bytes | File Output | Streaming |
|--------|---------------|-------|------------|-----------|
| JSON   | ✅            | ✅     | ✅         | ✅        |
| CSV    | ✅            | ✅     | ✅         | ✅        |
| Parquet| ✅            | ✅     | ✅         | ✅        |
| PyArrow| ✅            | ❌     | ❌         | ❌        |
| Dict   | ✅            | ❌     | ❌         | ❌        |

## Best Practices

1. **Choose the right output format for your needs**
   - JSON for readability and compatibility
   - CSV for simple tabular data and Excel compatibility
   - Parquet for analytical workloads and efficient storage

2. **Use bytes serialization for direct integration**
   - Network transfers without intermediate files
   - Integration with web APIs
   - In-memory processing pipelines

3. **Use memory-efficient mode for large datasets**
   - Reduce memory usage during conversion
   - Process data larger than available memory
   - Stream directly to output format

4. **Consider format-specific optimizations**
   - Compression options for Parquet
   - Indentation for JSON readability
   - Header options for CSV

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