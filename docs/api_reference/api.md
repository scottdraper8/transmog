# API Reference

## Functions

### flatten()

Transform nested data structures into flat tables.

```python
flatten(
    data: Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes],
    name: str = "data",
    config: Optional[TransmogConfig] = None,
) -> FlattenResult
```

**Parameters:**

- **data** (*Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes]*): Input data. Can be
  dictionary, list of dictionaries, JSON string, file path, or bytes.
- **name** (*str*, default="data"): Base name for generated tables.
- **config** (*Optional[TransmogConfig]*, default=None): Configuration object. Uses defaults if not provided.

**Returns:**

- **FlattenResult**: Object containing transformed tables.

**Examples:**

```python
import transmog as tm

# Basic usage
result = tm.flatten({"name": "Product", "price": 99.99})

# With configuration
config = tm.TransmogConfig(separator=".", cast_to_string=False)
result = tm.flatten(data, config=config)

# Factory method configuration
result = tm.flatten(data, config=tm.TransmogConfig(batch_size=10000))
```

### flatten_file()

Process data from files.

```python
flatten_file(
    path: Union[str, Path],
    name: Optional[str] = None,
    config: Optional[TransmogConfig] = None,
) -> FlattenResult
```

**Parameters:**

- **path** (*Union[str, Path]*): Path to input file.
- **name** (*Optional[str]*, default=None): Table name. Defaults to filename without extension.
- **config** (*Optional[TransmogConfig]*, default=None): Configuration object.

**Supported Formats:**

- JSON (.json)
- JSON Lines (.jsonl, .ndjson)

**Returns:**

- **FlattenResult**: Object containing transformed tables.

**Examples:**

```python
# Process JSON file
result = tm.flatten_file("data.json")

# With configuration
config = tm.TransmogConfig(separator=".")
result = tm.flatten_file("data.json", config=config)
```

### flatten_stream()

Stream data directly to files.

```python
flatten_stream(
    data: Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes],
    output_path: Union[str, Path],
    name: str = "data",
    output_format: str = "csv",
    config: Optional[TransmogConfig] = None,
    **format_options: Any,
) -> None
```

**Parameters:**

- **data** (*Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes]*): Input data (same as `flatten()`).
- **output_path** (*Union[str, Path]*): Directory path for output files.
- **name** (*str*, default="data"): Base name for output files.
- **output_format** (*str*, default="csv"): Output format ("csv", "parquet").
- **config** (*Optional[TransmogConfig]*, default=None): Configuration object.
- **\*\*format_options**: Format-specific options.

**Output Formats:**

- **"csv"**: CSV files
- **"parquet"**: Parquet files (requires pyarrow)

**Returns:**

- **None**: Data is written directly to files.

**Examples:**

```python
# Stream to CSV
tm.flatten_stream(large_data, "output/", output_format="csv")

# Stream to Parquet with configuration
config = tm.TransmogConfig(batch_size=5000)
tm.flatten_stream(data, "output/", output_format="parquet", config=config)
```

## Classes

### TransmogConfig

Configuration class for all processing parameters.

```python
TransmogConfig(
    separator: str = "_",
    cast_to_string: bool = False,
    null_handling: NullHandling = NullHandling.SKIP,
    array_mode: ArrayMode = ArrayMode.SMART,
    batch_size: int = 1000,
    max_depth: int = 100,
    id_field: str = "_id",
    parent_field: str = "_parent_id",
    time_field: Optional[str] = "_timestamp",
    id_patterns: Optional[list[str]] = None,
    deterministic_ids: bool = False,
    id_fields: Optional[list[str]] = None,
    recovery_mode: RecoveryMode = RecoveryMode.STRICT,
)
```

**Parameters:**

**Naming:**

- `separator` (str, default="_"): Character to separate nested field names

**Processing:**

- `cast_to_string` (bool, default=False): Convert all values to strings
- `null_handling` (NullHandling, default=NullHandling.SKIP): How to handle null and empty values:
  - `tm.NullHandling.SKIP`: Skip null values and empty strings (default)
  - `tm.NullHandling.INCLUDE`: Include null values as empty strings and include empty string values
- `array_mode` (ArrayMode, default=ArrayMode.SMART): How to handle arrays
- `batch_size` (int, default=1000): Records to process at once
- `max_depth` (int, default=100): Maximum recursion depth

**Metadata:**

- `id_field` (str, default="_id"): Field name for record IDs
- `parent_field` (str, default="_parent_id"): Field name for parent references
- `time_field` (Optional[str], default="_timestamp"): Field name for timestamps (None to disable)

**ID Discovery:**

- `id_patterns` (Optional[list[str]]): Field names to check for natural IDs

**Deterministic IDs:**

- `deterministic_ids` (bool, default=False): Generate deterministic IDs based on record content
- `id_fields` (Optional[list[str]]): List of field names for composite deterministic IDs

**Error Handling:**

- `recovery_mode` (RecoveryMode, default=RecoveryMode.STRICT): Error recovery strategy:
  - `tm.RecoveryMode.STRICT`: Stop on first error
  - `tm.RecoveryMode.SKIP`: Skip problematic records and continue

**Factory Methods:**

```python
# Memory-optimized: small batches (100)
config = TransmogConfig.for_memory()

# CSV-optimized: strings, includes empty/null values
config = TransmogConfig.for_csv()

# Error-tolerant: skip mode, continues on errors
config = TransmogConfig.error_tolerant()
```

### FlattenResult

Container for flattened data.

#### Properties

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
    path: Union[str, Path],
    output_format: Optional[str] = None
) -> Union[list[str], dict[str, str]]
```

**Parameters:**

- **path**: Output path (file or directory).
- **output_format**: Output format ("csv", "parquet"). Auto-detected from extension if not specified.

**Returns:**

- **Union[list[str], dict[str, str]]**: Created file paths.

**Examples:**

```python
# Save to directory
paths = result.save("output/")

# Save with explicit format
paths = result.save("output/", output_format="csv")

# Save single table
paths = result.save("data.csv")
```

##### table_info()

Get metadata about tables.

```python
table_info() -> dict[str, dict[str, Any]]
```

**Returns:**

- **dict**: Table metadata including record counts, fields, and main table indicator.

**Example:**

```python
info = result.table_info()
# {
#     "products": {
#         "records": 100,
#         "fields": ["name", "price", "_id"],
#         "is_main": True
#     },
#     "products_reviews": {
#         "records": 250,
#         "fields": ["rating", "comment", "_parent_id"],
#         "is_main": False
#     }
# }
```

#### Container Operations

```python
# Length
count = len(result)

# Iteration
for record in result:
    print(record)

# Key access
reviews = result["products_reviews"]
main = result["main"]

# Membership
if "products_tags" in result:
    print("Has tags table")

# Methods
table_names = list(result.keys())
table_data = list(result.values())
table_pairs = list(result.items())
```

## Error Classes

### TransmogError

Base exception class for all Transmog errors.

```python
class TransmogError(Exception):
    """Base exception for all Transmog operations."""
```

### ValidationError

Raised for data validation failures.

```python
class ValidationError(TransmogError):
    """Raised when input data fails validation checks."""
```

**Examples:**

```python
try:
    result = tm.flatten(invalid_data)
except tm.ValidationError as e:
    print(f"Data validation error: {e}")
```

**Note:** Configuration validation errors raise `ConfigurationError` (not exported), handled internally.

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

### NullHandling

Enumeration for controlling null and empty value handling. Available as `tm.NullHandling` when importing `transmog as tm`.

```python
import transmog as tm

tm.NullHandling.SKIP    # Skip null values and empty strings (default)
tm.NullHandling.INCLUDE # Include null values as empty strings
```

### RecoveryMode

Enumeration for controlling error recovery behavior. Available as `tm.RecoveryMode` when importing `transmog as tm`.

```python
import transmog as tm

tm.RecoveryMode.STRICT  # Stop on first error (default)
tm.RecoveryMode.SKIP    # Skip problematic records
```

## Module Information

```python
import transmog as tm

# Version
print(tm.__version__)

# Exported names
print(tm.__all__)
# ['flatten', 'flatten_file', 'flatten_stream', 'FlattenResult',
#  'TransmogConfig', 'ArrayMode', 'NullHandling', 'RecoveryMode',
#  'TransmogError', 'ValidationError', '__version__']

# All exported types are available directly
result = tm.flatten(data)                    # Main function
config = tm.TransmogConfig()                 # Configuration
mode = tm.ArrayMode.SMART                    # Array handling mode
null_handling = tm.NullHandling.SKIP         # Null value handling mode
recovery = tm.RecoveryMode.STRICT            # Error recovery mode
```

## Advanced Usage

```python
# Advanced configuration usage
import transmog as tm

config = tm.TransmogConfig(
    batch_size=1000,
    array_mode=tm.ArrayMode.SEPARATE,
    recovery_mode=tm.RecoveryMode.SKIP
)
result = tm.flatten(data, name="products", config=config)
```
