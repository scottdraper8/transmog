---
title: Output Formats
---

For API details, see [ProcessingResult API](../../api/processing-result.md).

# Output Formats

Transmog includes multiple output format options for working with processed data.

## Output Format Categories

The available output formats are organized into three categories:

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
child_table = tables.get("records_items", [])

# Access individual records
first_record = main_table[0] if main_table else {}
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

The bytes serialization options produce raw bytes that can be written to files or sent over network connections
without intermediate files.

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
csv_bytes = result.to_csv_bytes(include_header=True)

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
    include_header=True
)

print(f"Main table written to {csv_files['main']}")
```

### Parquet Files

```python
# Write all tables to Parquet files (requires pyarrow)
parquet_files = result.write_all_parquet(
    base_path="output_dir/parquet",
    compression="snappy"
)

print(f"Main table written to {parquet_files['main']}")
```

### Stream to Parquet

For large datasets, data can be streamed directly to Parquet files for improved memory efficiency:

```python
# Stream to Parquet files
parquet_files = result.stream_to_parquet(
    base_path="output_dir/parquet",
    compression="snappy",
    row_group_size=10000
)

print(f"Main table streamed to {parquet_files['main']}")
```

## Memory-Efficient Output

Memory-efficient output is implemented through the `ConversionMode` enum:

```python
from transmog import Processor
from transmog.process.result import ConversionMode

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

Three conversion modes are available for memory management:

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

`ConversionMode.MEMORY_EFFICIENT` minimizes memory usage by discarding intermediate data:

```python
# Set memory-efficient conversion mode
result = result.with_conversion_mode(ConversionMode.MEMORY_EFFICIENT)

# Converts and immediately discards intermediate data
result.write_all_parquet("output_dir/parquet")
```

## Direct Streaming

For large datasets, direct streaming capabilities are available:

```python
# Stream process data to output format
processor.stream_process(
    data=large_data,
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet"
)

# Stream process a file to output format
processor.stream_process_file(
    file_path="large_data.json",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet"
)

# Stream process a CSV file to output format
processor.stream_process_csv(
    file_path="data.csv",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir/parquet"
)
```

## Custom Writers

For advanced use cases, custom writers can be created using the writer factory:

```python
from transmog.io import create_writer

# Create a JSON writer
json_writer = create_writer("json", base_path="output_dir/json", indent=2)

# Write the main table
json_writer.write("main_table", result.get_main_table())

# Write child tables
for table_name in result.get_table_names():
    json_writer.write(
        table_name,
        result.get_child_table(table_name)
    )
```

## Streaming Writer API

For advanced streaming with very large datasets, the Streaming Writer API is provided:

```python
from transmog.io import create_streaming_writer

# Create a streaming Parquet writer
with create_streaming_writer(
    "parquet",
    destination="output_dir/parquet",
    compression="snappy",
    row_group_size=10000
) as writer:
    # Process data in batches
    for batch in data_batches:
        # Process each batch
        batch_result = processor.process_batch(batch, entity_name="records")

        # Write main table records
        writer.write_main_records(batch_result.get_main_table())

        # Initialize and write child tables
        for table_name in batch_result.get_table_names():
            writer.initialize_child_table(table_name)
            writer.write_child_records(
                table_name,
                batch_result.get_child_table(table_name)
            )
```

## Format Detection

File formats are automatically detected from file extensions:

```python
from transmog.io import detect_format

# Detect format from file path
format_type = detect_format("data.json")  # Returns "json"
format_type = detect_format("data.jsonl")  # Returns "jsonl"
format_type = detect_format("data.csv")  # Returns "csv"
```

## Format Availability

Format availability can be verified programmatically:

```python
from transmog.io import is_format_available, is_streaming_format_available

# Check if Parquet format is available
if is_format_available("parquet"):
    result.write_all_parquet("output_dir/parquet")
else:
    print("Parquet support not available. Install pyarrow package.")

# Check if streaming Parquet is available
if is_streaming_format_available("parquet"):
    processor.stream_process_file(
        file_path="large_data.json",
        entity_name="records",
        output_format="parquet",
        output_destination="output_dir/parquet"
    )
```

## Format Registry

Available formats are managed through a format registry:

```python
from transmog.io import FormatRegistry

# Get all registered formats
formats = FormatRegistry.get_registered_formats()
print(f"Available formats: {formats}")

# Check if a specific format is registered
if "parquet" in FormatRegistry.get_registered_formats():
    print("Parquet format is available")
```

## Result Combination

Multiple processing results can be combined:

```python
# Process multiple batches
result1 = processor.process(batch1, entity_name="records")
result2 = processor.process(batch2, entity_name="records")

# Combine results
from transmog.process import ProcessingResult
combined_result = ProcessingResult.combine_results([result1, result2], entity_name="records")

# Write combined results
combined_result.write_all_json("output_dir/combined")
```

## Supported Format Matrix

| Format | Native Objects | Bytes | File Output | Streaming |
|--------|---------------|-------|------------|-----------|
| JSON   | ✅            | ✅     | ✅         | ✅        |
| CSV    | ✅            | ✅     | ✅         | ✅        |
| Parquet| ✅            | ✅     | ✅         | ✅        |
| PyArrow| ✅            | ❌     | ❌         | ❌        |
| Dict   | ✅            | ❌     | ❌         | ❌        |

## Format Selection Factors

The following factors should be considered when selecting output formats:

1. **Output Format Applications**
   - JSON: Readability and compatibility
   - CSV: Simple tabular data and spreadsheet compatibility
   - Parquet: Analytical workloads and efficient storage

2. **Bytes Serialization Use Cases**
   - Network transfers without intermediate files
   - Integration with web APIs
   - In-memory processing pipelines

3. **Memory Efficiency Considerations**
   - Memory usage during conversion
   - Processing data larger than available memory
   - Direct streaming to output formats

4. **Format-Specific Parameters**
   - Compression options for Parquet
   - Indentation for JSON
   - Header options for CSV

## Performance Characteristics

The performance characteristics of different output formats include:

- PyArrow Tables: Optimized for large datasets and analytics processing
- Bytes Output: Optimized for direct writing to files or streaming
- Dictionary Output: Optimized for Python processing
- File Output: Provides direct file writing capability

## Memory Usage

The following pattern demonstrates memory-efficient processing for large datasets:

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

When selecting an output format, the following factors should be considered:

1. Data Size: Data volume influences format selection
2. Performance: Each format has specific performance characteristics
3. Memory Usage: Format selection affects memory requirements
4. Integration: Downstream processing tools may require specific formats
5. Readability: Formats have varying levels of human readability

## Format Comparison

| Format | Characteristics | Limitations | Common Use Cases |
|--------|----------------|-------------|-----------------|
| Python Dict | Simple, native | Memory usage increases with data size | Small-medium datasets, Python processing |
| PyArrow | Memory-efficient, columnar | Requires additional dependency | Large datasets, analytics, columnar data |
| JSON | Human-readable | Larger file size | API responses, debugging |
| CSV | Widely compatible | Limited to flat data structures | Tabular data, spreadsheet imports |
| Parquet | Columnar storage, compression | Binary format | Data warehousing, analytics pipelines |
