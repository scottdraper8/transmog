# API Reference

## Functions

### flatten()

Transform nested data structures into flat tables.

```python
flatten(
    data: DataInput,
    name: str = "data",
    config: Optional[TransmogConfig] = None,
) -> FlattenResult
```

**Parameters:**

- **data** (*Union[dict, list[dict], str, Path, bytes]*): Input data. Can be
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
result = tm.flatten(data, config=tm.TransmogConfig.for_performance())
```

### flatten_file()

Process data from files.

```python
flatten_file(
    path: Union[str, Path],
    name: Optional[str] = None,
    file_format: Optional[str] = None,
    config: Optional[TransmogConfig] = None,
) -> FlattenResult
```

**Parameters:**

- **path** (*Union[str, Path]*): Path to input file.
- **name** (*Optional[str]*, default=None): Table name. Defaults to filename without extension.
- **file_format** (*Optional[str]*, default=None): Input format. Auto-detected from extension if not specified.
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
    data: DataInput,
    output_path: Union[str, Path],
    name: str = "data",
    output_format: str = "csv",
    config: Optional[TransmogConfig] = None,
    **format_options: Any,
) -> None
```

**Parameters:**

- **data**: Input data (same as `flatten()`).
- **output_path** (*Union[str, Path]*): Directory path for output files.
- **name** (*str*, default="data"): Base name for output files.
- **output_format** (*str*, default="csv"): Output format ("csv", "parquet").
- **config** (*Optional[TransmogConfig]*, default=None): Configuration object. Uses memory-optimized config if not provided.
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
    nested_threshold: int = 4,
    cast_to_string: bool = False,
    include_empty: bool = False,
    skip_null: bool = True,
    array_mode: ArrayMode = ArrayMode.SMART,
    batch_size: int = 1000,
    max_depth: int = 100,
    id_field: str = "_id",
    parent_field: str = "_parent_id",
    time_field: Optional[str] = "_timestamp",
    id_patterns: Optional[list[str]] = None,
    recovery_strategy: str = "strict",
    allow_malformed_data: bool = False,
    cache_size: int = 10000,
    id_generator: Optional[Callable] = None,
)
```

**Parameters:**

**Naming:**

- `separator` (str, default="_"): Character to separate nested field names
- `nested_threshold` (int, default=4): Depth threshold for path simplification

**Processing:**

- `cast_to_string` (bool, default=False): Convert all values to strings
- `include_empty` (bool, default=False): Include empty values in output
- `skip_null` (bool, default=True): Skip null values
- `array_mode` (ArrayMode, default=SMART): How to handle arrays
- `batch_size` (int, default=1000): Records to process at once
- `max_depth` (int, default=100): Maximum recursion depth

**Metadata:**

- `id_field` (str, default="_id"): Field name for record IDs
- `parent_field` (str, default="_parent_id"): Field name for parent references
- `time_field` (Optional[str], default="_timestamp"): Field name for timestamps (None to disable)

**ID Discovery:**

- `id_patterns` (Optional[list[str]]): Field names to check for natural IDs

**Error Handling:**

- `recovery_strategy` (str, default="strict"): Error recovery strategy
  ("strict", "skip", "partial", "raise", "warn")
- `allow_malformed_data` (bool, default=False): Allow malformed data

**Cache:**

- `cache_size` (int, default=10000): Maximum cache size (set to 0 to disable)

**Advanced:**

- `id_generator` (Optional[Callable]): Custom function for ID generation

**Factory Methods:**

```python
# Memory-optimized
config = TransmogConfig.for_memory()

# Performance-optimized
config = TransmogConfig.for_performance()

# CSV-optimized
config = TransmogConfig.for_csv()

# Parquet-optimized
config = TransmogConfig.for_parquet()

# Simple configuration
config = TransmogConfig.simple()

# Error-tolerant
config = TransmogConfig.error_tolerant()
```

**Loading Methods:**

```python
# From file
config = TransmogConfig.from_file("config.json")

# From environment
config = TransmogConfig.from_env()
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
paths = result.save("data.json")
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

# Safe access
tags = result.get_table("products_tags", default=[])
```

## Error Classes

### TransmogError

Base exception class.

```python
class TransmogError(Exception):
    """Base exception for Transmog operations."""
```

### ValidationError

Raised for validation failures.

```python
class ValidationError(TransmogError):
    """Raised for validation failures."""
```

**Example:**

```python
try:
    result = tm.flatten(data, config=invalid_config)
except tm.ValidationError as e:
    print(f"Validation error: {e}")
```

## Type Definitions

### ArrayMode

```python
from transmog.types import ArrayMode

ArrayMode.SMART    # Default: simple arrays inline, complex extracted
ArrayMode.SEPARATE # All arrays to child tables
ArrayMode.INLINE   # All arrays as JSON strings
ArrayMode.SKIP     # Ignore arrays
```

## Module Information

```python
import transmog

# Version
print(transmog.__version__)

# Exported names
print(transmog.__all__)
# ['flatten', 'flatten_file', 'flatten_stream', 'FlattenResult',
#  'TransmogConfig', 'TransmogError', 'ValidationError', '__version__']
```

## Advanced Usage

```python
# Direct processor usage
from transmog.process import Processor

config = tm.TransmogConfig()
processor = Processor(config)
result = processor.process(data, entity_name="products")
```
