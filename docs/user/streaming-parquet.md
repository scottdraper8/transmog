# Streaming to Parquet

Transmog provides powerful capabilities for streaming data to Parquet files, which is especially valuable
when working with large datasets that may not fit entirely in memory.

## Why Stream to Parquet?

Parquet is a columnar storage format that offers excellent compression and efficient querying of large
datasets. However, traditional methods of writing to Parquet typically require holding all data in memory.
Transmog solves this problem by offering:

1. **Low Memory Footprint**: Process and write data in chunks without loading everything into memory
2. **Row Groups Management**: Automatically manages Parquet row groups for optimal performance
3. **Schema Evolution**: Handles schema changes between batches seamlessly
4. **Consistent Schema**: Maintains consistent schema across all row groups

## Basic Usage

### Using the ProcessingResult.stream_to_parquet Method

The simplest way to stream data to Parquet files is using the `stream_to_parquet` method:

```python
import transmog as tm

# Process data
processor = tm.Processor.memory_optimized()
result = processor.process_chunked(data_source, entity_name="records", chunk_size=1000)

# Stream to Parquet files
output_files = result.stream_to_parquet(
    base_path="output_directory",
    compression="snappy",
    row_group_size=10000  # Number of rows per row group
)

# Output files contains paths to all generated Parquet files
print(f"Main table: {output_files['main']}")
for table_name, file_path in output_files.items():
    if table_name != "main":
        print(f"Child table {table_name}: {file_path}")
```

### Advanced Direct Streaming

For more control and even more memory efficiency, you can use the `ParquetStreamingWriter` directly:

```python
from transmog import Processor
from transmog.io import create_streaming_writer

# Configure processor
processor = Processor.memory_optimized()

# Create a streaming writer
writer = create_streaming_writer(
    "parquet",
    destination="output_directory",
    entity_name="records",
    compression="snappy",
    row_group_size=10000
)

# Use context manager to ensure proper finalization
with writer:
    # Process data in batches
    for batch in data_batches:
        # Process each batch
        batch_result = processor.process_batch(batch, entity_name="records")

        # Write main table records
        writer.write_main_records(batch_result.get_main_table())

        # Write child tables
        for table_name in batch_result.get_table_names():
            writer.initialize_child_table(table_name)
            writer.write_child_records(
                table_name,
                batch_result.get_child_table(table_name)
            )

        # Free memory by releasing the batch result
        batch_result = None
```

## Configuring Parquet Streaming

Both `stream_to_parquet` and `ParquetStreamingWriter` support the following options:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `compression` | Compression codec (snappy, gzip, brotli, zstd) | "snappy" |
| `row_group_size` | Number of rows per row group | 10000 |
| `write_page_index` | Enable page indexes for better query performance | False |
| `write_page_checksum` | Enable page checksums for data integrity | False |
| `data_page_size` | Target size of data pages in bytes | None (1MB) |
| `dictionary_pagesize_limit` | Dictionary page size limit per row group | None (1MB) |

You can pass any additional options supported by PyArrow's `parquet.write_table` function.

## Memory Efficiency Tips

To maximize memory efficiency when working with large datasets:

1. **Process in small batches**: Use `process_batch` with reasonably sized batches
2. **Free memory after processing**: Set processed results to `None` after writing
3. **Configure appropriate row group size**: Smaller row groups use less memory but create more files
4. **Use zstd compression**: It offers better compression than snappy with reasonable performance

## Technical Details

### Schema Evolution

The `ParquetStreamingWriter` automatically handles schema evolution across batches:

1. When a new batch has additional columns, they are added to the schema
2. Previous batches' records will have NULL values for these new columns
3. The writer maintains a consistent schema across all row groups

### Performance Considerations

Parquet writing performance depends on several factors:

1. **Row Group Size**: Larger row groups tend to compress better but require more memory
2. **Compression Codec**:
   - `snappy`: Fast with moderate compression (default)
   - `gzip`: Higher compression but slower
   - `zstd`: Excellent balance of speed and compression
   - `brotli`: Very high compression but slower
3. **Page Size**: Smaller page sizes improve random access but may reduce compression

## Example: Processing a Large Dataset

```python
import transmog as tm
from transmog.io.writers.parquet import ParquetStreamingWriter

# Configure processor with memory optimization
processor = tm.Processor.memory_optimized()

# Create streaming writer with zstd compression for better ratio
writer = ParquetStreamingWriter(
    destination="output/large_dataset",
    entity_name="records",
    compression="zstd",
    row_group_size=100000,
    write_page_index=True,  # Enable page index for better query performance
)

# Process 10 million records in batches of 10,000
with writer:
    for i in range(1000):
        # Load batch from source (example)
        batch = load_batch_from_source(i, size=10000)

        # Process the batch
        result = processor.process_batch(batch, entity_name="records")

        # Write main records
        writer.write_main_records(result.get_main_table())

        # Write child records
        for table_name in result.get_table_names():
            writer.initialize_child_table(table_name)
            writer.write_child_records(table_name, result.get_child_table(table_name))

        # Free memory
        del result
        del batch
```

## Conclusion

The streaming Parquet capability in Transmog enables efficient processing and storage of large, complex
nested data structures without excessive memory usage. This makes it ideal for ETL pipelines, data migration
tasks, and other scenarios where you need to convert nested data to a tabular format suitable for analytics tools.
