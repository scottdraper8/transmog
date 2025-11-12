# Streaming and Batch Processing

Stream large datasets directly to files without keeping all data in memory.

## flatten_stream()

```python
import transmog as tm

# Stream to CSV files
tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="csv"
)

# Stream to compressed Parquet
tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="parquet",
    compression="snappy"
)

# Stream to compressed ORC
tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="orc",
    compression="zstd"
)
```

`flatten()` keeps results in memory and returns a `FlattenResult` object.
`flatten_stream()` writes directly to disk and returns nothing.

## Batch Size

```python
# Default batch size: 1000 for flatten()
result = tm.flatten(data)

# flatten_stream() defaults to batch_size=100 for memory efficiency
tm.flatten_stream(large_data, "output/")

# Small batches for memory-constrained environments
config = tm.TransmogConfig(batch_size=100)
tm.flatten_stream(large_data, "output/", config=config)

# Large batches for throughput
config = tm.TransmogConfig(batch_size=10000)
result = tm.flatten(data, config=config)
```

## File Processing

### JSON Files

```python
tm.flatten_stream("large_file.json", "output/", output_format="parquet")
```

### JSONL Files

```python
tm.flatten_stream("large_file.jsonl", "output/", output_format="csv")
```

JSONL files are processed line-by-line.

## Output Formats

```python
tm.flatten_stream(data, "output/", output_format="csv")
tm.flatten_stream(data, "output/", output_format="parquet")
tm.flatten_stream(data, "output/", output_format="parquet", compression="snappy")
tm.flatten_stream(data, "output/", output_format="parquet", row_group_size=50000)
tm.flatten_stream(data, "output/", output_format="orc")
tm.flatten_stream(data, "output/", output_format="orc", compression="zstd")
```

## Examples

```python
config = tm.TransmogConfig(batch_size=100)
tm.flatten_stream(
    "large_dataset.json",
    "processed/",
    name="records",
    output_format="parquet",
    compression="snappy",
    config=config
)
```

### ETL Pipeline

```python
config = tm.TransmogConfig(batch_size=5000)
tm.flatten_stream("raw_data.jsonl", "transformed/", name="events", output_format="parquet", config=config)
```
