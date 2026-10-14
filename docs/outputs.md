# Output Formats

Supported formats: CSV, Parquet, ORC, and Avro.

## Value and Schema Semantics

- CSV writes scalar text and represents null as an empty field. List values
  are JSON strings, with Unicode preserved.
- Avro preserves scalar types, including numeric strings and booleans in
  union fields. Lists and dictionaries are JSON strings. Schema consolidation
  combines types from every part rather than selecting the first part's type.
- Parquet and ORC retain native scalars and nested lists. Numeric columns and
  list elements are promoted across all records and parts; fractional values
  are not truncated to the first observed integer type. Missing fields become
  null when schemas are unified.
- No caller-supplied schema is required. Parquet/ORC automatically use JSON
  text for a column whose values cannot share a native type, or whose numeric
  promotion would lose precision. Empty objects and sparse object keys inside
  containers also use JSON when a native struct would reject them or add keys.
  The entire column is encoded, including
  quotes around original strings, so `"1"`, `1`, and `true` remain distinct.
  Null remains a native null. Compatible numbers can be promoted to floats.
- Automatically encoded fields carry `transmog.encoding=json` in field
  metadata in both Parquet and ORC. Consumers can inspect
  `table.schema.field(name).metadata` and apply `json.loads` to non-null cells
  with that marker. Unmarked strings must not be guessed to contain JSON.
  The schema log annotates these fields as `string (json)`.
- If consolidation or schema coercion encounters a later incompatible type,
  earlier native parts are encoded too, without double-encoding existing JSON
  parts. Uncoerced, unconsolidated parts can retain different local encodings;
  each file's field metadata identifies its representation.
- `coerce_schema=True` rewrites parts to the **union** of fields and compatible
  types, not a majority schema. Uncoerced, unconsolidated parts retain their
  local schemas.

Flattening treats scalar NaN, Infinity, empty strings, and None as null-like.
Direct writers are a different boundary: CSV/Avro normalize non-finite floats,
including inside serialized containers; Arrow writers retain IEEE NaN/Infinity.
Consequently a direct Arrow write does not claim the same normalization as a
`flatten` workflow. CSV/Avro JSON strings use null for non-finite elements,
never non-standard `NaN` or `Infinity` tokens. Automatic Arrow JSON fallback
requires JSON-compatible values; non-finite floats in a heterogeneous column
are not JSON values. Homogeneous native Arrow float columns retain IEEE values.

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

Streaming output (`flatten_stream`) produces the same naming by default:

```python
tm.flatten_stream(data, "output/", name="products", output_format="csv")
# Creates: output/products.csv, output/products_reviews.csv

# With consolidate=False, produces numbered part files instead:
tm.flatten_stream(data, "output/", name="products", output_format="csv", consolidate=False)
# Creates: output/products_part_0000.csv, output/products_reviews_part_0000.csv, ...
```

## CSV Output

```python
result = tm.flatten(data, name="products")
result.save("output.csv")
```

:::{tip}
CSV headers use the union of fields in the written table. Missing values
appear as empty fields. `include_nulls=True` additionally preserves explicitly
null-like source fields; it does not invent fields absent from every record.
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

Streaming CSV supports the same formatting options, plus `include_header`:

```python
tm.flatten_stream(
    data, "output/",
    output_format="csv",
    delimiter="|",
    quotechar="'",
    include_header=True,       # Include column headers (default: True)
)
```

Set `include_header=False` to omit the header row from each output file. This
is useful when appending to existing files or when headers are managed externally.

### Schema Drift Tracking

Internally, each batch flush produces a separate part file with its own column
set. With the default `consolidate=True`, these are merged into a single file
with all columns at close time.

With `consolidate=False`, if columns differ across parts, a `_schema_log.json`
file records the deviations and a `UserWarning` is emitted at close time.
Pass `coerce_schema=True` to `flatten_stream()` to automatically unify columns
across part files:

```python
tm.flatten_stream(
    data, "output/", name="events", output_format="csv",
    consolidate=False, coerce_schema=True,
)
# Part files missing columns are rewritten with null-filled columns
```

See {doc}`streaming` for details on `consolidate` and `coerce_schema`.

#### _schema_log.json Format

When `consolidate=False`, a `_schema_log.json` file is written alongside the
part files. It contains a `tables` object keyed by table name, each with:

- **`base_schema`**: The schema of the first part file, listing field names
  and types.
- **`parts`**: A list of entries per part file. Each entry has a `file` name
  and a `deviations` object (`null` if the part matches the base schema).

Deviations, when present, contain:

- **`structural`**: `added` and `removed` field name lists (fields present in
  the part but not the base, or vice versa).
- **`type`**: A mapping of field names to `{"base": "...", "part": "..."}`
  showing type mismatches.

```json
{
  "tables": {
    "events": {
      "base_schema": {
        "fields": [
          {"name": "_id", "type": "string"},
          {"name": "event_type", "type": "string"}
        ]
      },
      "parts": [
        {"file": "events_part_0000.csv", "deviations": null},
        {"file": "events_part_0001.csv", "deviations": {
          "structural": {"added": ["new_field"], "removed": []},
          "type": {}
        }}
      ]
    }
  }
}
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

# Advanced: customize sync interval (bytes between sync markers, default: 16000)
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
- Streaming batches are consolidated into a single file per table by default
- With `consolidate=False`, each batch produces a separate part file and schema
  deviations are tracked in `_schema_log.json`

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

## Downstream examples

CSV with consistent columns (for example, database `COPY` or spreadsheet import):

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
rel = duckdb.sql("SELECT * FROM 'data.parquet'")
```
