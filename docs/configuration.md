# Configuration

Configuration options control processing behavior through the `TransmogConfig` class.

## Parameters

```python
import transmog as tm

config = tm.TransmogConfig(
    # Data Transformation
    array_mode=tm.ArrayMode.SMART,       # How to handle arrays
    include_nulls=False,                 # Include null and empty values
    stringify_values=False,              # Convert all values to strings
    max_depth=100,                       # Maximum recursion depth

    # ID and Metadata
    id_generation="random",              # ID generation strategy
    id_field="_id",                      # Field name for record IDs
    parent_field="_parent_id",           # Field name for parent references
    time_field="_timestamp",             # Field name for timestamps (None to disable)

    # Processing Control
    batch_size=5000,                     # Records to process at once
)

result = tm.flatten(data, config=config)
```

## Core Parameters

These are the parameters most users will configure.

### array_mode

**Type:** `ArrayMode`
**Default:** `ArrayMode.SMART`

Controls how arrays are processed. See [Array Handling](arrays.md) for detailed
examples of each mode.

Options: `SMART`, `SEPARATE`, `INLINE`, `SKIP`.

### id_generation

**Type:** `str | list[str]`
**Default:** `"random"`

Controls how record IDs are generated. See [ID Management](ids.md) for detailed
examples of each strategy.

Options: `"random"`, `"natural"`, `"hash"`, or a list of field names for composite keys.

### include_nulls

**Type:** `bool`
**Default:** `False`

Include null and empty values in output. Enable this for CSV output where
consistent columns across all rows are needed.

```python
config = tm.TransmogConfig(include_nulls=True)
```

### stringify_values

**Type:** `bool`
**Default:** `False`

Convert all leaf values to strings after flattening:

- Numbers become strings: `42` → `"42"`, `3.14` → `"3.14"`
- Booleans become strings: `True` → `"True"`, `False` → `"False"`
- Null values remain as None/null (not stringified)

```python
config = tm.TransmogConfig(stringify_values=True)
result = tm.flatten({"price": 19.99, "active": True}, config=config)
# Result: {"price": "19.99", "active": "True"}
```

Useful when targeting CSV output or when downstream systems expect uniform string
types. Eliminates type coercion errors in Parquet/ORC writers.

### batch_size

**Type:** `int`
**Default:** `5000`

Number of records to process in each batch. Controls both memory usage during
streaming and the size of intermediate part files before consolidation.

```python
config = tm.TransmogConfig(batch_size=1000)   # Smaller batches
config = tm.TransmogConfig(batch_size=10000)  # Large batches
```

A warning is emitted for values below 500 (many intermediate part files) or
above 100,000 (high memory usage).

:::{tip}
**Choosing batch_size**

- **Small batches (500-2000):** Use for memory-constrained environments or very
  large records.
- **Medium batches (2000-10000):** Default range, balances memory and throughput.
- **Large batches (10000+):** Use when memory is plentiful and throughput is
  critical. Reduces per-batch overhead.

:::

## Streaming Parameters

The `consolidate` and `coerce_schema` options are passed directly to
`flatten_stream()` rather than through `TransmogConfig`. See
{doc}`streaming` for details and examples.

## Advanced Parameters

These parameters have sensible defaults and rarely need adjustment.

### id_field

**Type:** `str`
**Default:** `"_id"`

Controls two things depending on `id_generation`:

- **Output field name** — the name of the ID field written to every output record,
  regardless of strategy.
- **Source field name** — when `id_generation="natural"`, the field transmog reads
  from each source record to use as that record's ID.

For all other strategies (`"random"`, `"hash"`, composite list), the value is only
used as the output field name — no source field is read.

Change this only if `_id` conflicts with your data schema.

### parent_field

**Type:** `str`
**Default:** `"_parent_id"`

Controls the **output field name** written on child records to reference their
parent's ID. This is purely an output concern — it does not read from or target
any field in the source data. The parent-child link is established automatically
from the nesting structure.

Change this only if `_parent_id` conflicts with your data schema.

:::{note}
`id_field`, `parent_field`, and `time_field` must all be distinct. Supplying the
same name for any two raises a `ConfigurationError`.
:::

### time_field

**Type:** `str | None`
**Default:** `"_timestamp"`

Field name for processing timestamps. Timestamps are UTC in
`YYYY-MM-DD HH:MM:SS.ssssss` format. Set to `None` to disable timestamp
generation entirely.

```python
config = tm.TransmogConfig(time_field=None)  # Disable timestamps
```

### max_depth

**Type:** `int`
**Default:** `100`

Maximum recursion depth for nested structures. The entire subtree below this
depth is silently omitted — not just the field at that level, but all of its
descendants. This is a safety guard; most JSON data is well under 100 levels
deep.

:::{note}
Adjust only if processing unusually deep structures or to intentionally
truncate output at a specific nesting level.
:::

## Logging

Transmog uses Python's standard `logging` module. By default no output is
produced (a `NullHandler` is attached to the root `transmog` logger). To
enable diagnostic output, configure the logger in your application:

```python
import logging

logging.basicConfig()
logging.getLogger("transmog").setLevel(logging.INFO)
```

### Log Levels

**INFO** — API entry/exit and streaming batch progress:

```text
INFO:transmog.api:flatten started, name=products, input_type=list
INFO:transmog.api:flatten completed, name=products, main_records=150, child_tables=3
INFO:transmog.streaming:stream started, entity=events, format=parquet
INFO:transmog.streaming:stream batch 1 processed, records_in_batch=100, total_records=100
INFO:transmog.streaming:stream completed, entity=events, total_batches=5, total_records=500
```

**DEBUG** — Format detection, schema inference, and batch processing internals:

```text
DEBUG:transmog.iterators:file input detected, path=data.json, extension=.json
DEBUG:transmog.iterators:string format detected as jsonl
DEBUG:transmog.flattening:processing batch, records=100, entity=products
DEBUG:transmog.writers.arrow_base:arrow schema created, fields=12, types={'name': 'string', ...}
DEBUG:transmog.writers.csv:csv schema created, table=main, fields=8
```

**WARNING** — Schema deviations and data issues:

Schema deviations across part files are emitted as `UserWarning` at close time,
distinguishing structural changes (added/removed fields) from type changes.
Details are also written to `_schema_log.json` in the output directory.

### Per-Module Loggers

Each module uses its own logger under the `transmog` namespace. Target
specific modules to reduce noise:

```python
import logging

# Only show streaming batch progress
logging.basicConfig()
logging.getLogger("transmog.streaming").setLevel(logging.INFO)

# Only show format detection decisions
logging.getLogger("transmog.iterators").setLevel(logging.DEBUG)
```

:::{tip}
Enable `DEBUG` on `transmog.writers` when troubleshooting schema issues.
Debug logs show schema inference details for each part file.
:::
