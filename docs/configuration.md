# Configuration

Configuration options control processing behavior through the `TransmogConfig` class.

## Parameters

```python
import transmog as tm

config = tm.TransmogConfig(
    # Data Transformation
    array_mode=tm.ArrayMode.SMART,       # How to handle arrays
    include_nulls=False,                 # Include null and empty values
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

## Parameter Details

### include_nulls

**Type:** `bool`
**Default:** `False`

Include null and empty values in output:

```python
config = tm.TransmogConfig(include_nulls=False)  # Default
config = tm.TransmogConfig(include_nulls=True)   # For CSV
```

### array_mode

**Type:** `ArrayMode`
**Default:** `ArrayMode.SMART`

Options: `SMART`, `SEPARATE`, `INLINE`, `SKIP`. See [Array Handling](arrays.md).

### batch_size

**Type:** `int`
**Default:** `1000`

```python
config = tm.TransmogConfig(batch_size=100)    # Small batches
config = tm.TransmogConfig(batch_size=10000)  # Large batches
```

### max_depth

**Type:** `int`
**Default:** `100`

Maximum recursion depth. Fields deeper than this limit are omitted.

### id_generation

**Type:** `str | list[str]`
**Default:** `"random"`

Options: `"random"`, `"natural"`, `"hash"`, or list of fields for composite keys. See [ID Management](ids.md).

### id_field

**Type:** `str`
**Default:** `"_id"`

Field name for record IDs.

### parent_field

**Type:** `str`
**Default:** `"_parent_id"`

Field name for parent references.

### time_field

**Type:** `str | None`
**Default:** `"_timestamp"`

Field name for timestamps. Set to `None` to disable.
