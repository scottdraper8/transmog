# API Reference

Complete reference for all public Transmog functions, classes, and types.

## Functions

### flatten()

Transform nested data structures into flat tables.

```python
flatten(
    data: Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes],
    *,
    name: str = "data",
    # Naming options
    separator: str = "_",
    nested_threshold: int = 4,
    # ID options
    id_field: Union[str, dict[str, str], None] = None,
    parent_id_field: str = "_parent_id",
    add_timestamp: bool = False,
    # Array handling
    arrays: Literal["separate", "inline", "skip"] = "separate",
    # Data options
    preserve_types: bool = False,
    skip_null: bool = True,
    skip_empty: bool = True,
    # Error handling
    errors: Literal["raise", "skip", "warn"] = "raise",
    # Performance
    batch_size: int = 1000,
    low_memory: bool = False,
) -> FlattenResult
```

**Parameters:**

- **data** (*Union[Dict, list[Dict], str, Path, bytes]*): Input data to transform. Can be:
  - Dictionary or list of dictionaries
  - JSON string
  - File path (str or Path)
  - Raw bytes containing JSON

- **name** (*str*, default="data"): Base name for generated tables

**Naming Options:**

- **separator** (*str*, default="_"): Character to join nested field names
- **nested_threshold** (*int*, default=4): Maximum nesting depth before simplifying field names

**ID Options:**

- **id_field** (*str | dict[str, str] | None*, default=None): Field(s) to use as record IDs
- **parent_id_field** (*str*, default="_parent_id"): Name for parent reference fields
- **add_timestamp** (*bool*, default=False): Add processing timestamp metadata

**Array Handling:**

- **arrays** (*Literal["separate", "inline", "skip"]*, default="separate"): How to process arrays:
  - "separate": Extract arrays into child tables (default)
  - "inline": Keep arrays as JSON strings in main table
  - "skip": Ignore arrays completely

**Data Options:**

- **preserve_types** (*bool*, default=False): Maintain original data types vs convert to strings
- **skip_null** (*bool*, default=True): Exclude null values from output
- **skip_empty** (*bool*, default=True): Exclude empty strings and collections

**Error Handling:**

- **errors** (*Literal["raise", "skip", "warn"]*, default="raise"): Error handling strategy:
  - "raise": Stop processing and raise exception
  - "skip": Skip problematic records and continue
  - "warn": Log warnings but continue processing

**Performance:**

- **batch_size** (*int*, default=1000): Records to process in each batch
- **low_memory** (*bool*, default=False): Use memory-efficient processing (slower)

**Returns:**

- **FlattenResult**: Object containing transformed tables and metadata

**Examples:**

```python
import transmog as tm

# Basic usage
data = {"name": "Product", "price": 99.99}
result = tm.flatten(data, name="products")

# Custom configuration
result = tm.flatten(
    data,
    name="products",
    separator=".",
    arrays="inline",
    preserve_types=True
)

# Using existing ID field
result = tm.flatten(data, id_field="product_id")

# Error handling
result = tm.flatten(data, errors="skip")
```

### flatten_file()

Process data directly from files.

```python
flatten_file(
    path: Union[str, Path],
    *,
    name: Optional[str] = None,
    file_format: Optional[str] = None,
    **options: Any,
) -> FlattenResult
```

**Parameters:**

- **path** (*Union[str, Path]*): Path to input file
- **name** (*Optional[str]*, default=None): Table name (defaults to filename without extension)
- **file_format** (*Optional[str]*, default=None): Input format (auto-detected from extension)
- **\*\*options**: All options from `flatten()` function

**Supported Formats:**

- JSON (.json)
- CSV (.csv) - for files containing JSON in cells

**Returns:**

- **FlattenResult**: Object containing transformed tables

**Examples:**

```python
# Process JSON file
result = tm.flatten_file("data.json", name="products")

# Auto-detect name from filename
result = tm.flatten_file("products.json")  # name="products"

# Pass additional options
result = tm.flatten_file("data.json", arrays="inline", errors="skip")
```

### flatten_stream()

Stream large datasets directly to files without loading into memory.

```python
flatten_stream(
    data: Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes],
    output_path: Union[str, Path],
    *,
    name: str = "data",
    output_format: str = "json",
    # All options from flatten()
    separator: str = "_",
    nested_threshold: int = 4,
    id_field: Union[str, dict[str, str], None] = None,
    parent_id_field: str = "_parent_id",
    add_timestamp: bool = False,
    arrays: Literal["separate", "inline", "skip"] = "separate",
    preserve_types: bool = False,
    skip_null: bool = True,
    skip_empty: bool = True,
    errors: Literal["raise", "skip", "warn"] = "raise",
    batch_size: int = 1000,
    **format_options: Any,
) -> None
```

**Parameters:**

- **data**: Input data (same as `flatten()`)
- **output_path** (*Union[str, Path]*): Directory or file path for output
- **name** (*str*, default="data"): Base name for output files
- **output_format** (*str*, default="json"): Output format ("json", "csv", "parquet")
- **\*\*format_options**: Format-specific options

**Output Formats:**

- **"json"**: JSON Lines format for efficient streaming
- **"csv"**: CSV files with proper escaping
- **"parquet"**: Columnar format for analytics (requires pyarrow)

**Returns:**

- **None**: Data is written directly to files

**Examples:**

```python
# Stream to JSON files
tm.flatten_stream(large_data, "output/", name="products", output_format="json")

# Stream to Parquet for analytics
tm.flatten_stream(data, "output/", output_format="parquet", batch_size=5000)

# Stream with custom options
tm.flatten_stream(
    data,
    "output/",
    name="logs",
    output_format="csv",
    arrays="skip",
    errors="warn"
)
```

## Classes

### FlattenResult

Container for flattened data with convenience methods for access and export.

#### Properties

**main** (*list[dict[str, Any]]*): Main flattened table

```python
result = tm.flatten(data)
main_table = result.main
```

**tables** (*dict[str, list[dict[str, Any]]]*): Child tables dictionary

```python
child_tables = result.tables
reviews = result.tables["products_reviews"]
```

**all_tables** (*dict[str, list[dict[str, Any]]]*): All tables including main

```python
all_data = result.all_tables
```

#### Methods

##### save(path, output_format=None)

Save all tables to files.

```python
save(
    path: Union[str, Path],
    output_format: Optional[str] = None
) -> Union[list[str], dict[str, str]]
```

**Parameters:**

- **path**: Output path (file or directory)
- **output_format**: Output format ("json", "csv", "parquet", auto-detected from extension)

**Returns:**

- **Union[list[str], dict[str, str]]**: Created file paths

**Examples:**

```python
# Save as JSON files in directory
paths = result.save("output/")

# Save as CSV with explicit format
paths = result.save("output/", output_format="csv")

# Save single table as JSON file
paths = result.save("data.json")
```

##### table_info()

Get metadata about all tables.

```python
table_info() -> dict[str, dict[str, Any]]
```

**Returns:**

- **Dict**: Table metadata including record counts, fields, and main table indicator

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

#### Container Methods

FlattenResult supports standard container operations:

```python
# Length (main table record count)
count = len(result)

# Iteration (over main table)
for record in result:
    print(record)

# Key access
reviews = result["products_reviews"]
main = result["main"]  # or result[entity_name]

# Membership testing
if "products_tags" in result:
    print("Has tags table")

# Keys, values, items
table_names = list(result.keys())
table_data = list(result.values())
table_pairs = list(result.items())

# Safe access with default
tags = result.get_table("products_tags", default=[])
```

## Error Classes

### TransmogError

Base exception class for all Transmog errors.

```python
class TransmogError(Exception):
    """Base exception for Transmog operations."""
```

### ValidationError

Raised when input data or configuration is invalid.

```python
class ValidationError(TransmogError):
    """Raised for validation failures."""
```

**Common Causes:**

- Invalid configuration parameters
- Malformed input data
- Unsupported data types
- File format issues

**Example:**

```python
try:
    result = tm.flatten(data, arrays="invalid_option")
except tm.ValidationError as e:
    print(f"Configuration error: {e}")
```

## Type Aliases

### DataInput

Type alias for supported input data formats.

```python
DataInput = Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes]
```

### ArrayHandling

Type alias for array processing options.

```python
ArrayHandling = Literal["separate", "inline", "skip"]
```

### ErrorHandling

Type alias for error handling strategies.

```python
ErrorHandling = Literal["raise", "skip", "warn"]
```

### IdSource

Type alias for ID field specifications.

```python
IdSource = Union[str, dict[str, str], None]
```

## Module Information

**Version**: Access current version

```python
import transmog
print(transmog.__version__)  # "1.1.0"
```

**Available Functions**: Check what's available

```python
print(transmog.__all__)
# ['flatten', 'flatten_file', 'flatten_stream', 'FlattenResult',
#  'TransmogError', 'ValidationError', '__version__']
```

## Advanced Usage

For advanced features like custom processing or configuration objects:

```python
# Import advanced components directly
from transmog.process import Processor
from transmog.config import TransmogConfig

# Create custom processor
processor = Processor()

# Use configuration objects
config = TransmogConfig(
    separator=".",
    array_handling="inline",
    preserve_types=True
)
```

See the [Developer Guide](../developer_guide/extending.md) for more advanced usage patterns.
