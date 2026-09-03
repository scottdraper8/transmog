# Streaming and Batch Processing

Stream large datasets directly to files without keeping all data in memory.

## flatten_stream()

```python
import transmog as tm

# Stream to CSV — returns list of written paths
files = tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="csv"
)
# files: [PosixPath('output/dataset.csv'), ...]

# Stream to compressed Parquet
files = tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="parquet",
    compression="snappy"
)

# Stream to compressed ORC
files = tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="orc",
    compression="zstd"
)

# Stream to Avro
files = tm.flatten_stream(
    large_data,
    output_path="output/",
    name="dataset",
    output_format="avro",
    compression="snappy"
)
```

`flatten()` keeps results in memory and returns a `FlattenResult` object.
`flatten_stream()` writes directly to disk and returns a `list[Path]` of written
file paths.

By default, intermediate part files are consolidated into a single file per table
at close time (e.g., `dataset.csv`, `dataset_reviews.csv`). This matches data
lake conventions and produces predictable filenames for Glue crawlers, Hive
tables, and downstream consumers.

Internally, each batch flush creates a separate part file with its own
independently inferred schema, so fields that only appear in certain batches are
preserved. The consolidation step merges these into one file with a unified
schema.

To disable consolidation and keep individual part files, pass `consolidate=False`:

```python
files = tm.flatten_stream(data, "output/", name="dataset", output_format="csv", consolidate=False)
# files: [PosixPath('output/dataset_part_0000.csv'), ...]
```

When consolidation is disabled, a `_schema_log.json` file is written alongside
the part files, tracking the base schema and any deviations across parts. See
the [Schema Drift Tracking](outputs.md#schema-drift-tracking) section in Output
Formats for the file format and usage details.

:::{warning}
When using `flatten_stream()`, ensure the output directory has sufficient disk
space for the processed data. Large datasets can generate substantial output files.
:::

## Batch Size

The `batch_size` parameter controls both memory usage during streaming and the
size of intermediate part files before consolidation. See
{doc}`configuration` for sizing recommendations.

```python
config = tm.TransmogConfig(batch_size=1000)
tm.flatten_stream(large_data, "output/", config=config)
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
Large `.json` array files are parsed with constant memory using ijson. See
[Working with Files](working-with-files) for supported formats.

```python
tm.flatten_stream("large_file.jsonl", "output/", output_format="parquet")
```

## Output Formats

See [Output Formats](outputs.md) for full details on each format, compression
options, and codec dependencies.

## Examples

```python
tm.flatten_stream(
    "large_dataset.json",
    "processed/",
    name="records",
    output_format="parquet",
    compression="snappy",
)
# Creates: processed/records.parquet
```

### ETL Pipeline

```python
tm.flatten_stream("raw_data.jsonl", "transformed/", name="events", output_format="parquet")
# Creates: transformed/events.parquet
```

### Part Files for Parallel Processing

```python
config = tm.TransmogConfig(batch_size=5000)
tm.flatten_stream("raw_data.jsonl", "parts/", name="events", output_format="parquet", config=config, consolidate=False)
# Creates: parts/events_part_0000.parquet, parts/events_part_0001.parquet, ...
```
