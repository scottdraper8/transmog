# Core API Reference

> **User Guide**: For a user-friendly introduction, see the [Data Transformation Guide](../user/processing/data-transformation.md).

This document provides technical reference for Transmog's core components and functions.

## Main API Functions

### flatten

```python
flatten(
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    name: str,
    id_field: Optional[Union[str, Dict[str, str]]] = None,
    transforms: Optional[Dict[str, Callable]] = None,
    separator: str = "_",
    cast_to_string: bool = False,
    include_empty: bool = False,
    skip_null: bool = True,
    add_metadata: bool = True,
    add_timestamps: bool = False,
    max_depth: Optional[int] = 100,
    deep_nesting_threshold: Optional[int] = 4,
    error_handling: Literal["raise", "skip", "warn"] = "raise",
    low_memory: bool = False,
    chunk_size: Optional[int] = None,
    stream: bool = False,
    output_path: Optional[str] = None,
    output_format: Optional[Literal["json", "csv", "parquet"]] = None,
    **format_options
) -> FlattenResult
```

Flattens nested data structures into normalized tables with preserved relationships.

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | Dict[str, Any] or List[Dict[str, Any]] | Required | Data to flatten (dictionary or list of dictionaries) |
| name | str | Required | Name for the main entity (used for table naming) |
| id_field | str, Dict[str, str], or "auto" | None | Field(s) to use for deterministic IDs (string for single field, dict for table mapping, "auto" for natural ID discovery) |
| transforms | Dict[str, Callable] | None | Dictionary of field transformation functions |
| separator | str | "_" | Separator to use between path segments in field names |
| cast_to_string | bool | False | Whether to cast all values to strings |
| include_empty | bool | False | Whether to include empty values |
| skip_null | bool | True | Whether to skip null values |
| add_metadata | bool | True | Whether to add metadata fields (_id, _parent_id) |
| add_timestamps | bool | False | Whether to add timestamp metadata |
| max_depth | int | 100 | Maximum recursion depth |
| deep_nesting_threshold | int | 4 | Threshold for special handling of deeply nested structures |
| error_handling | str | "raise" | Error handling strategy ("raise", "skip", or "warn") |
| low_memory | bool | False | Whether to optimize for low memory usage |
| chunk_size | int | None | Size of chunks for processing (enables chunked processing) |
| stream | bool | False | Whether to stream results directly to output files |
| output_path | str | None | Path for output files when streaming |
| output_format | str | None | Format for output files when streaming ("json", "csv", "parquet") |
| **format_options | Any | None | Additional format-specific options |

#### Returns

A `FlattenResult` object containing the processed tables.

#### Example

```python
import transmog as tm

data = {
    "user": {
        "id": 1,
        "name": "John Doe",
        "address": {
            "street": "123 Main St",
            "city": "Anytown"
        },
        "orders": [
            {"id": 101, "product": "Laptop", "price": 999.99},
            {"id": 102, "product": "Mouse", "price": 24.99}
        ]
    }
}

# Basic usage
result = tm.flatten(data, name="user")

# Access the main table
print(result.main)
# Access child tables
print(result.tables["user_orders"])

# With deterministic IDs
result = tm.flatten(data, name="user", id_field="id")

# With table-specific ID fields
result = tm.flatten(data, name="user", id_field={
    "": "id",                # Main table uses "id" field
    "user_orders": "id"      # Orders table uses "id" field
})

# Save results to files
result.save("output_directory", format="json")
```

### flatten_file

```python
flatten_file(
    file_path: str,
    name: str,
    id_field: Optional[Union[str, Dict[str, str]]] = None,
    transforms: Optional[Dict[str, Callable]] = None,
    separator: str = "_",
    cast_to_string: bool = False,
    include_empty: bool = False,
    skip_null: bool = True,
    add_metadata: bool = True,
    add_timestamps: bool = False,
    max_depth: Optional[int] = 100,
    deep_nesting_threshold: Optional[int] = 4,
    error_handling: Literal["raise", "skip", "warn"] = "raise",
    low_memory: bool = False,
    chunk_size: Optional[int] = None,
    stream: bool = False,
    output_path: Optional[str] = None,
    output_format: Optional[Literal["json", "csv", "parquet"]] = None,
    encoding: str = "utf-8",
    has_header: bool = True,
    delimiter: str = ",",
    **format_options
) -> FlattenResult
```

Flattens data from a file (JSON or CSV) into normalized tables.

#### Parameters

Same as `flatten()` with these additions:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| file_path | str | Required | Path to the input file (JSON or CSV) |
| encoding | str | "utf-8" | File encoding (for CSV files) |
| has_header | bool | True | Whether CSV file has a header row |
| delimiter | str | "," | Delimiter for CSV files |

#### Returns

A `FlattenResult` object containing the processed tables.

#### Example

```python
import transmog as tm

# Process a JSON file
result = tm.flatten_file("data.json", name="user")

# Process a CSV file
result = tm.flatten_file(
    "data.csv", 
    name="employees",
    has_header=True,
    delimiter=",",
    null_values=["NA", ""]
)

# Save results to files
result.save("output_directory", format="parquet")
```

### flatten_stream

```python
flatten_stream(
    file_path: Optional[str] = None,
    data: Optional[Union[Iterable[Dict[str, Any]], Dict[str, Any]]] = None,
    name: str = None,
    id_field: Optional[Union[str, Dict[str, str]]] = None,
    transforms: Optional[Dict[str, Callable]] = None,
    separator: str = "_",
    cast_to_string: bool = False,
    include_empty: bool = False,
    skip_null: bool = True,
    add_metadata: bool = True,
    add_timestamps: bool = False,
    max_depth: Optional[int] = 100,
    deep_nesting_threshold: Optional[int] = 4,
    error_handling: Literal["raise", "skip", "warn"] = "raise",
    low_memory: bool = True,
    chunk_size: Optional[int] = 1000,
    output_path: str = None,
    output_format: Literal["json", "csv", "parquet"] = "json",
    encoding: str = "utf-8",
    has_header: bool = True,
    delimiter: str = ",",
    error_log: Optional[str] = None,
    **format_options
) -> None
```

Streams data processing directly to output files without loading everything into memory.

#### Parameters

Same as `flatten_file()` with these additions:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| output_path | str | Required | Path for output files |
| output_format | str | "json" | Format for output files ("json", "csv", "parquet") |
| error_log | str | None | Path to write error logs |

#### Returns

None (results are written directly to files)

#### Example

```python
import transmog as tm

# Stream process a JSON file
tm.flatten_stream(
    file_path="large_data.json",
    name="records",
    output_path="output_directory",
    output_format="parquet",
    chunk_size=1000,
    low_memory=True
)

# Stream process a CSV file
tm.flatten_stream(
    file_path="large_data.csv",
    name="employees",
    output_path="output_directory",
    output_format="csv",
    has_header=True,
    delimiter=",",
    chunk_size=500
)
```

## Result API

### FlattenResult

The `FlattenResult` class provides access to processed tables and utility methods.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| main | List[Dict[str, Any]] | The main (root) table |
| tables | Dict[str, List[Dict[str, Any]]] | Dictionary of all tables (including main) |

#### Methods

| Method | Description |
|--------|-------------|
| save(path, format="json", **options) | Save all tables to files |
| to_dict() | Convert all tables to dictionaries |
| iter_tables() | Iterate through tables (memory-efficient) |

#### Example

```python
import transmog as tm

# Process data
result = tm.flatten(data, name="user")

# Access tables
main_table = result.main
orders_table = result.tables["user_orders"]

# Save to files
result.save("output", format="json")
result.save("output", format="csv", include_header=True)
result.save("output", format="parquet", compression="snappy")
```

## Extractor Module

### extract_arrays

```python
extract_arrays(
    obj: dict[str, Any],
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "root",
    separator: str = None,
    cast_to_string: bool = None,
    include_empty: bool = None,
    skip_null: bool = None,
    transmog_time: Optional[Any] = None,
    parent_path_parts: Optional[list[str]] = None,
    max_component_length: Optional[int] = None,
    preserve_leaf: Optional[bool] = None,
    deep_nesting_threshold: Optional[int] = None,
    deterministic_id_fields: Optional[dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
) -> dict[str, list[dict[str, Any]]]
```

Extracts nested arrays from JSON structure with parent-child relationships.

## Hierarchy Module

### process_structure

```python
process_structure(
    data: dict[str, Any],
    entity_name: str,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    transmog_time: Optional[Any] = None,
    root_entity: Optional[str] = None,
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    deep_nesting_threshold: int = 4,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    id_field: str = "__transmog_id",
    parent_field: str = "__parent_transmog_id",
    time_field: str = "__transmog_datetime",
    visit_arrays: bool = True,
    streaming: bool = False,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]
```

Processes JSON structure with parent-child relationship preservation. This is the main entry point for
processing a complete JSON structure, handling both the main record and all nested arrays.

## Metadata Module

### annotate_with_metadata

```python
annotate_with_metadata(
    record: dict[str, Any],
    parent_id: Optional[str] = None,
    transmog_id: Optional[str] = None,
    transmog_time: Optional[Any] = None,
    id_field: str = "__transmog_id",
    parent_field: str = "__parent_transmog_id",
    time_field: str = "__transmog_datetime",
    extra_fields: Optional[dict[str, Any]] = None,
    in_place: bool = False,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    id_field_patterns: Optional[list[str]] = None,
    path: Optional[str] = None,
    id_field_mapping: Optional[dict[str, str]] = None,
    force_transmog_id: bool = False,
) -> dict[str, Any]
```

Annotates a record with metadata fields like transmog ID, parent ID, and timestamp.

### generate_transmog_id

```python
generate_transmog_id(
    record: Optional[dict[str, Any]] = None,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
) -> str
```

Generates a unique ID for record tracking. This function can generate IDs in three ways:

1. Random UUID (default)
2. Deterministic UUID based on record field(s)
3. Custom function-based ID generation
