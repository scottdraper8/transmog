# API Reference

## Functions

### flatten()

Transform nested data structures into flat tables.

```python
flatten(
    data: dict[str, Any] | list[dict[str, Any]] | str | Path | bytes,
    name: str = "data",
    config: TransmogConfig | None = None,
) -> FlattenResult
```

**Parameters:**

- **data** (*dict[str, Any] | list[dict[str, Any]] | str | Path | bytes*): Input data. Can be
  dictionary, list of dictionaries, JSON string, file path, or bytes.
- **name** (*str*, default="data"): Base name for generated tables.
- **config** (*TransmogConfig | None*, default=None): Configuration object. Uses defaults if not provided.

**Returns:**

- **FlattenResult**: Object containing transformed tables.

**Examples:**

```python
import transmog as tm

# Basic usage
result = tm.flatten({"name": "Product", "price": 99.99})

# With configuration
config = tm.TransmogConfig(include_nulls=True, batch_size=10000)
result = tm.flatten(data, config=config)

# Custom configuration
result = tm.flatten(data, config=tm.TransmogConfig(include_nulls=True))

# Process file directly
result = tm.flatten("data.json")
result = tm.flatten("data.jsonl")
result = tm.flatten("data.json5")
result = tm.flatten("data.hjson")
```

**Supported File Formats:**

- JSON (.json)
- JSON Lines (.jsonl, .ndjson)
- JSON5 (.json5) - Supports comments, trailing commas, unquoted keys, single quotes

:::{important}
JSON5 support requires: `pip install json5`
:::

- HJSON (.hjson) - Human JSON with comments, unquoted strings, multiline strings

:::{important}
HJSON support requires: `pip install hjson`
:::

### flatten_stream()

Stream data directly to files.

```python
flatten_stream(
    data: dict[str, Any] | list[dict[str, Any]] | str | Path | bytes,
    output_path: str | Path,
    name: str = "data",
    output_format: str = "csv",
    config: TransmogConfig | None = None,
    **format_options: Any,
) -> None
```

**Parameters:**

- **data** (*dict[str, Any] | list[dict[str, Any]] | str | Path | bytes*): Input data (same as `flatten()`).
- **output_path** (*str | Path*): Directory path for output files.
- **name** (*str*, default="data"): Base name for output files.
- **output_format** (*str*, default="csv"): Output format ("csv", "parquet", "orc", "avro").
- **config** (*TransmogConfig | None*, default=None): Configuration object.
- **\*\*format_options**: Format-specific options.

**Output Formats:**

- **"csv"**: CSV files
- **"parquet"**: Parquet files (requires pyarrow)
- **"orc"**: ORC files (requires pyarrow)
- **"avro"**: Avro files (requires fastavro, cramjam)

**Returns:**

- **None**: Data is written directly to files.

**Examples:**

```python
# Stream to CSV
tm.flatten_stream(large_data, "output/", output_format="csv")

# Stream to Parquet
tm.flatten_stream(data, "output/", output_format="parquet")

# Stream to ORC with configuration
config = tm.TransmogConfig(batch_size=5000)
tm.flatten_stream(data, "output/", output_format="orc", config=config)
```

:::{note}
When `config` is not provided, `flatten_stream()` uses `batch_size=100` (instead
of the default 1000) for memory efficiency. Pass an explicit config to override.
:::

:::{seealso}
For large datasets that don't fit in memory, use `flatten_stream()` instead of
`flatten()`. It writes directly to disk without keeping all data in memory.
:::

## Classes

### TransmogConfig

Configuration class for all processing parameters.

```python
TransmogConfig(
    array_mode: ArrayMode = ArrayMode.SMART,
    include_nulls: bool = False,
    stringify_values: bool = False,
    max_depth: int = 100,
    id_generation: str | list[str] = "random",
    id_field: str = "_id",
    parent_field: str = "_parent_id",
    time_field: str | None = "_timestamp",
    batch_size: int = 1000,
)
```

**Parameters:**

**Data Transformation:**

- `array_mode` (ArrayMode, default=ArrayMode.SMART): How to handle arrays
- `include_nulls` (bool, default=False): Include null and empty values in output
- `stringify_values` (bool, default=False): Convert all leaf values to strings after
  flattening. Numbers become `"42"`, booleans become `"True"`, nulls remain as None.
- `max_depth` (int, default=100): Maximum recursion depth

**ID and Metadata:**

- `id_generation` (str | list[str], default="random"): ID generation strategy
  - String options:
    - `"random"` (default): Always generate random UUID
    - `"natural"`: Use existing `id_field` field (error if missing)
    - `"hash"`: Deterministic hash of entire record
  - List options:
    - `["field1", "field2"]`: Deterministic hash of these specific fields (composite key)

- `id_field` (str, default="_id"): Field name for record IDs (generated or existing)
- `parent_field` (str, default="_parent_id"): Field name for parent record references
- `time_field` (str | None, default="_timestamp"): Field name for timestamps. Set to None to disable timestamp tracking

**Processing Control:**

- `batch_size` (int, default=1000): Records to process at once

### FlattenResult

Container for flattened data.

#### Properties

**entity_name** (*str*): Name of the entity associated with the main table.

```python
result = tm.flatten(data, name="products")
entity = result.entity_name  # "products"
```

**main** (*list[dict[str, Any]]*): Main flattened table.

```python
result = tm.flatten(data)
main_table = result.main
```

**tables** (*dict[str, list[dict[str, Any]]]*): Child tables dictionary.

```python
child_tables = result.tables
reviews = result.tables["products_reviews"]
```

**all_tables** (*dict[str, list[dict[str, Any]]]*): All tables including main.

```python
all_data = result.all_tables
```

#### Methods

##### save()

Save tables to files.

```python
save(
    path: str | Path,
    output_format: str | None = None,
    **format_options: Any
) -> list[str] | dict[str, str]
```

**Parameters:**

- **path**: Output path (file or directory).
- **output_format**: Output format ("csv", "parquet", "orc", "avro"). Auto-detected
  from extension if not specified. Defaults to "csv" when no extension is present.
- **\*\*format_options**: Format-specific writer options (e.g., `delimiter`, `quoting` for CSV; `compression` for Parquet).

**Returns:**

- **list[str] | dict[str, str]**: Created file paths. Returns a list for
  single table output or a dictionary mapping table names to file paths for
  multiple tables.

**Behavior:**

- **With child tables:** Saves all tables to a directory. If a file path with an
  extension is given (e.g., `"output/data.csv"`), the extension is stripped and a
  directory is created instead. Returns `dict[str, str]` mapping table names to paths.
- **Without child tables:** Saves the main table to a single file. If no extension
  is present, the output format extension is appended automatically. Returns `list[str]`.

**Examples:**

```python
# Save to directory (when child tables exist)
paths = result.save("output/")
# Creates: output/products.csv, output/products_reviews.csv

# Save with explicit format
paths = result.save("output/", output_format="parquet")

# Save single table (when no child tables)
paths = result.save("data.csv")
```

#### Accessing Data

Access result data through properties:

```python
# Main table records
records = result.main
print(f"Main table records: {len(result.main)}")

# Iterate over main table records
for record in result.main:
    print(record)
```

## Error Classes

### TransmogError

Base exception class for all Transmog errors.

```python
class TransmogError(Exception):
    """Base exception for all Transmog operations."""
```

**Available as:** `tm.TransmogError`

### ValidationError

Raised for data validation failures.

```python
class ValidationError(TransmogError):
    """Raised when input data fails validation checks."""
```

**Available as:** `tm.ValidationError`

**Examples:**

```python
try:
    result = tm.flatten(invalid_data)
except tm.ValidationError as e:
    print(f"Data validation error: {e}")
```

### MissingDependencyError

Raised when an optional dependency is missing.

```python
class MissingDependencyError(TransmogError):
    """Raised when an optional dependency is missing."""
```

**Available as:** `tm.MissingDependencyError`

**Examples:**

```python
# Parquet dependency error
try:
    tm.flatten_stream(data, "output/", output_format="parquet")
except tm.MissingDependencyError as e:
    print(f"Missing dependency: {e}")
    print(f"Install with: pip install pyarrow")

# Avro dependency error
try:
    tm.flatten_stream(data, "output/", output_format="avro")
except tm.MissingDependencyError as e:
    print(f"Missing dependency: {e}")
    print(f"Install with: pip install fastavro cramjam")
```

:::{note}
Other exception types (`ConfigurationError`, `OutputError`) exist
internally but are not exported in the public API. Catch them using generic
exception handling or `TransmogError` as the base class.
:::

## Type Definitions

### ArrayMode

Enumeration for controlling array handling behavior. Available as `tm.ArrayMode` when importing `transmog as tm`.

```python
import transmog as tm

tm.ArrayMode.SMART    # Default: simple arrays inline, complex extracted
tm.ArrayMode.SEPARATE # All arrays to child tables
tm.ArrayMode.INLINE   # All arrays as JSON strings
tm.ArrayMode.SKIP     # Ignore arrays
```

## Module Information

```python
import transmog as tm

# Version
print(tm.__version__)

# Exported names
print(tm.__all__)
# ['flatten', 'flatten_stream', 'FlattenResult',
#  'TransmogConfig', 'ArrayMode',
#  'TransmogError', 'ValidationError', 'MissingDependencyError', '__version__']

# All exported types are available directly
result = tm.flatten(data)                    # Main function
config = tm.TransmogConfig()                 # Configuration
mode = tm.ArrayMode.SMART                    # Array handling mode

# Exception handling
try:
    result = tm.flatten(data)
except tm.ValidationError as e:              # Validation errors
    print(f"Validation error: {e}")
except tm.TransmogError as e:                # Base class for all errors
    print(f"Processing error: {e}")
```

## Advanced Usage

```python
# Advanced configuration usage
import transmog as tm

config = tm.TransmogConfig(
    batch_size=1000,
    array_mode=tm.ArrayMode.SEPARATE
)
result = tm.flatten(data, name="products", config=config)
```
