# Streaming and Memory-Efficient Processing

For large datasets that exceed available memory, Transmog provides streaming functionality.

It processes data in batches and writes results directly to files without keeping all data in memory.

## Streaming Functions

### flatten_stream()

The `flatten_stream()` function processes data and writes results directly to output files:

```python
import transmog as tm

# Stream large dataset directly to CSV files
tm.flatten_stream(
    data=large_dataset,
    output_path="output/",
    name="dataset",
    output_format="csv",
    config=tm.TransmogConfig.for_memory()
)

# Stream to compressed Parquet files with format options
tm.flatten_stream(
    data=large_dataset,
    output_path="output/",
    output_format="parquet",
    compression="snappy"  # Format-specific option passed as keyword argument
)
```

The function accepts:

- **data**: Input data (dict, list, file path, or JSON string)
- **output_path**: Directory where output files are written
- **output_format**: "csv" or "parquet"
- **config**: Optional configuration (defaults to `TransmogConfig.for_memory()` if not provided)
- **format_options**: Format-specific options like compression, encoding, etc.

## Batch Processing Configuration

### Batch Size Tuning

Batch size controls memory usage and processing speed. Configure via `TransmogConfig`:

```python
import transmog as tm

# Memory-efficient: small batches, minimal cache
config = tm.TransmogConfig.for_memory()  # batch_size=100, cache_size=1000
result = tm.flatten(large_data, config=config)

# Performance-optimized: large batches, extended cache
config = tm.TransmogConfig.for_parquet()  # batch_size=10000, cache_size=50000
result = tm.flatten(large_data, config=config)

# Custom batch size
config = tm.TransmogConfig(batch_size=2000, cache_size=10000)
result = tm.flatten(large_data, config=config)
```

### Memory Usage Patterns

Different batch sizes impact memory consumption:

| Configuration | Memory Usage | Processing Speed | Best For |
|---------------|--------------|------------------|----------|
| `batch_size=100` | Minimal | Moderate | Limited memory |
| `batch_size=1000` | Low | High | Balanced approach |
| `batch_size=10000` | Variable | Highest | Abundant memory |

## Streaming Input Sources

## Processing Large Files

### File Input

`flatten_stream()` can process files directly:

```python
import transmog as tm

# Process large JSON file
tm.flatten_stream(
    data="large_dataset.json",  # File path
    output_path="output/",
    output_format="csv"
)

# Process JSONL file (line-delimited JSON)
tm.flatten_stream(
    data="data.jsonl",  # File path
    output_path="output/",
    name="records"
)
```

### Direct File Output

Results are written directly to files without keeping data in memory:

```python
# Stream to CSV
tm.flatten_stream(
    data=large_data,
    output_path="output/",
    output_format="csv"
)

# Stream to compressed Parquet
tm.flatten_stream(
    data=large_data,
    output_path="output/",
    output_format="parquet",
    compression="gzip"
)
```
