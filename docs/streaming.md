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

# Stream to Avro
tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="avro",
    codec="snappy"
)
```

`flatten()` keeps results in memory and returns a `FlattenResult` object.
`flatten_stream()` writes directly to disk and returns nothing.

:::{warning}
When using `flatten_stream()`, ensure the output directory has sufficient disk
space for the processed data. Large datasets can generate substantial output files.
:::

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

## Progress Tracking

Track processing progress with a callback:

```python
def on_progress(records_processed, total_records):
    if total_records:
        pct = records_processed / total_records * 100
        print(f"{records_processed}/{total_records} ({pct:.0f}%)")
    else:
        print(f"{records_processed} records processed")

# Works with both flatten() and flatten_stream()
result = tm.flatten(data, progress_callback=on_progress)

tm.flatten_stream(
    large_data,
    output_path="output/",
    progress_callback=on_progress
)
```

`total_records` is the input length when known (`list` or `dict` input), otherwise
`None` (file paths, byte strings, iterators). The callback fires once per batch, so
frequency depends on `batch_size`.

### Using with tqdm

```python
from tqdm import tqdm

data = load_data()  # list of records
bar = tqdm(total=len(data), unit="rec")

def update_bar(processed, total):
    bar.update(processed - bar.n)

result = tm.flatten(data, progress_callback=update_bar)
bar.close()
```

## File Processing

All file formats supported by `flatten()` work with `flatten_stream()`. JSONL
files are processed line-by-line, making them ideal for streaming large datasets.
See [Working with Files](working-with-files) for supported formats
and dependency requirements.

```python
tm.flatten_stream("large_file.jsonl", "output/", output_format="parquet")
```

## Output Formats

See [Output Formats](outputs.md) for full details on each format and its options.

```python
tm.flatten_stream(data, "output/", output_format="csv")
tm.flatten_stream(data, "output/", output_format="parquet", compression="snappy")
tm.flatten_stream(data, "output/", output_format="parquet", row_group_size=50000)
tm.flatten_stream(data, "output/", output_format="orc", compression="zstd")
tm.flatten_stream(data, "output/", output_format="avro", codec="snappy")
tm.flatten_stream(data, "output/", output_format="avro", codec="deflate", sync_interval=32000)
```

:::{note}
The ORC writer accepts a `batch_size` format option (e.g., `batch_size=50000`)
that controls how many rows are written per ORC stripe. This is separate from
`TransmogConfig.batch_size`, which controls how many records are processed per
processing batch.
:::

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
