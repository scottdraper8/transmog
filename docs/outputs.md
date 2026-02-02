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
# Save to directory (multiple tables)
result.save("output/")
# Creates: output/products.csv, output/products_reviews.csv

# Single table to file
result.save("output/products.csv")
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

# Compression options (codec parameter)
result.save("output.avro", codec="snappy")     # Default (via cramjam)
result.save("output.avro", codec="deflate")    # Built-in compression
result.save("output.avro", codec="null")       # No compression
result.save("output.avro", codec="bzip2")      # Via cramjam
result.save("output.avro", codec="xz")         # Via cramjam

# Additional codecs (require separate package installations):
# codec="zstandard"  # Requires: pip install zstandard
# codec="lz4"        # Requires: pip install lz4
```

:::{note}
The default install includes `cramjam` which provides `snappy`, `bzip2`, and `xz`
codecs. While `cramjam` also bundles `zstandard` and `lz4` algorithms, `fastavro`
requires the standalone `zstandard` and `lz4` packages to use those codecs.
:::

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
# {'name': 'Product', 'description': '', 'notes': ''}
```

## Integration Examples

### PostgreSQL

```python
config = tm.TransmogConfig(include_nulls=True)
result = tm.flatten(data, name="customers", config=config)
result.save("import/")
```

### Pandas

```python
result = tm.flatten(data, name="sales")
result.save("analysis.parquet")

import pandas as pd
df = pd.read_parquet("analysis.parquet")
```

### DuckDB

```python
result = tm.flatten(data, name="transactions")
result.save("data.parquet")

import duckdb
df = duckdb.connect().execute("SELECT * FROM 'data.parquet'").df()
```
