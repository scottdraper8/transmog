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
    batch_size=1000,                     # Records to process at once
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
**Default:** `1000`

Number of records to process in each batch. Affects memory usage and throughput.

```python
config = tm.TransmogConfig(batch_size=100)    # Small batches
config = tm.TransmogConfig(batch_size=10000)  # Large batches
```

:::{tip} Choosing batch_size

- **Small batches (100-500):** Use for memory-constrained environments or very
  large records. `flatten_stream()` defaults to 100 for memory efficiency.
- **Medium batches (1000-5000):** Default choice, balances memory and throughput.
- **Large batches (10000+):** Use when memory is plentiful and throughput is
  critical. Reduces per-batch overhead.

:::

## Advanced Parameters

These parameters have sensible defaults and rarely need adjustment.

### id_field

**Type:** `str`
**Default:** `"_id"`

Field name for record IDs. Change only if `_id` conflicts with your data schema.

### parent_field

**Type:** `str`
**Default:** `"_parent_id"`

Field name for parent references in child tables. Change only if `_parent_id`
conflicts with your data schema.

### time_field

**Type:** `str | None`
**Default:** `"_timestamp"`

Field name for processing timestamps. Set to `None` to disable timestamp
generation entirely.

```python
config = tm.TransmogConfig(time_field=None)  # Disable timestamps
```

### max_depth

**Type:** `int`
**Default:** `100`

Maximum recursion depth for nested structures. Fields nested deeper than this
limit are silently omitted. This is a safety guard — most JSON data is well
under 100 levels deep.

:::{note}
Adjust only if processing unusually deep structures or to intentionally
truncate output at a specific nesting level.
:::
