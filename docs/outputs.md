# Output Formats

Supported formats: CSV, Parquet, ORC, and Avro.

## Saving Results

### Auto-Detection

```python
import transmog as tm

result = tm.flatten(data, name="products")

# Auto-detect format from extension
result.save("output.csv")          # CSV
result.save("output.parquet")      # Parquet
result.save("output.orc")          # ORC
result.save("output.avro")         # Avro
```

### Explicit Format

```python
# Specify format explicitly
result.save("output", output_format="csv")
result.save("output", output_format="parquet")
result.save("output", output_format="orc")
result.save("output", output_format="avro")
```

### Multiple Tables

When results contain child tables, save to a directory:

```python
# Save to directory (multiple tables) — batch writer produces single files
result.save("output/")
# Creates: output/products.csv, output/products_reviews.csv

# Single table to file
result.save("output/products.csv")
```

Streaming output (`flatten_stream`) produces numbered part files per table:

```python
tm.flatten_stream(data, "output/", name="products", output_format="csv")
# Creates: output/products_part_0000.csv, output/products_reviews_part_0000.csv, ...
```

## CSV Output

```python
result = tm.flatten(data, name="products")
result.save("output.csv")
```

:::{tip}
For consistent CSV columns across all rows, use `include_nulls=True` in
`TransmogConfig`. This ensures fields that are missing in some records appear
as empty strings in the CSV output.
:::

```python
# Include nulls for consistent columns
config = tm.TransmogConfig(include_nulls=True)
result = tm.flatten(data, config=config)
result.save("output.csv")
```

Custom options:

```python
import csv

# Custom delimiter and quoting
result.save(
    "output.csv",
    delimiter="|",
    quoting=csv.QUOTE_ALL
)
```

Streaming CSV supports the same formatting options:

```python
tm.flatten_stream(
    data, "output/",
    output_format="csv",
    delimiter="|",
    quotechar="'",
    include_header=True,       # Include column headers (default: True)
)
```

### Schema Drift Tracking

Each batch flush produces a separate part file with its own column set. If
columns differ across parts, a `_schema_log.json` file records the deviations
and a `UserWarning` is emitted at close time distinguishing structural changes
(added/removed columns) from the base schema.

Enable `coerce_schema` to automatically unify columns across part files at close
time:

```python
config = tm.TransmogConfig(coerce_schema=True)
tm.flatten_stream(data, "output/", name="events", output_format="csv", config=config)
# Part files missing columns are rewritten with null-filled columns
```

## Parquet Output

```python
result = tm.flatten(data, name="products")
result.save("output.parquet")

# Compression options
result.save("output.parquet", compression="snappy")  # Default
result.save("output.parquet", compression="gzip")
result.save("output.parquet", compression="brotli")
result.save("output.parquet", compression=None)
```

## ORC Output

```python
result = tm.flatten(data, name="products")
result.save("output.orc")

# Compression options
result.save("output.orc", compression="zstd")    # Default
result.save("output.orc", compression="snappy")
result.save("output.orc", compression="lz4")
result.save("output.orc", compression="zlib")
```

## Avro Output

```python
result = tm.flatten(data, name="products")
result.save("output.avro")

# Compression options
result.save("output.avro", compression="snappy")     # Default (via cramjam)
result.save("output.avro", compression="deflate")    # Built-in compression
result.save("output.avro", compression="null")       # No compression
result.save("output.avro", compression="bzip2")      # Via cramjam
result.save("output.avro", compression="xz")         # Via cramjam

# Additional codecs (require separate package installations):
# compression="zstandard"  # Requires: pip install zstandard
# compression="lz4"        # Requires: pip install lz4

# Advanced: customize sync interval (bytes between sync markers)
result.save("output.avro", compression="snappy", sync_interval=32000)
```

:::{note}
**Codec Dependencies:**

To use additional codecs beyond the defaults:

- **zstandard**: `pip install zstandard`
- **lz4**: `pip install lz4`

Technical detail: While cramjam (included by default) bundles these compression
algorithms, fastavro's codec interface requires the standalone packages to expose them.
:::

### Avro Schema Inference

Avro schemas are automatically inferred from your data:

```python
data = [
    {"name": "Alice", "age": 30, "score": 95.5},
    {"name": "Bob", "age": None, "score": 88.0}
]

result = tm.flatten(data, name="users")
result.save("output.avro")
```

Schema inference behavior:

- Field types are detected from values (string, long, double, boolean, bytes)
- Nullable fields use Avro union types: `["null", "type"]`
- NaN and Infinity float values are automatically converted to null
- Mixed types in a field result in union types with multiple type options
- Each streaming batch produces a separate part file with its own inferred schema
- Schema deviations across parts are tracked in `_schema_log.json`

## Null Handling

```python
data = {"name": "Product", "description": None, "notes": ""}

config = tm.TransmogConfig(include_nulls=False)
result = tm.flatten(data, config=config)

print(result.main[0])
# {'name': 'Product'}

config = tm.TransmogConfig(include_nulls=True)
result = tm.flatten(data, config=config)
print(result.main[0])
# {'name': 'Product', 'description': None, 'notes': None}
```

## Integration Examples

### PostgreSQL

```python
config = tm.TransmogConfig(include_nulls=True)
result = tm.flatten(data, name="customers", config=config)
result.save("import/")
```

### PyArrow

```python
result = tm.flatten(data, name="sales")
result.save("analysis.parquet")

import pyarrow.parquet as pq
table = pq.read_table("analysis.parquet")
```

### Polars

```python
result = tm.flatten(data, name="sales")
result.save("analysis.parquet")

import polars as pl
df = pl.read_parquet("analysis.parquet")
```

### DuckDB

```python
result = tm.flatten(data, name="transactions")
result.save("data.parquet")

import duckdb
df = duckdb.connect().execute("SELECT * FROM 'data.parquet'").df()
```
