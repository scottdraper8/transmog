# Core API Reference

This section covers the core low-level functions in Transmog. These functions are used internally by
the high-level API (`Processor` class) but can also be used directly for more control over the transformation process.

## Flattening Module

### flatten_json

```python
flatten_json(
    data: Dict[str, Any],
    separator: str = None,
    cast_to_string: bool = None,
    include_empty: bool = None,
    skip_null: bool = None,
    skip_arrays: bool = True,
    visit_arrays: bool = None,
    parent_path: str = "",
    path_parts: Optional[List[str]] = None,
    path_parts_optimization: bool = None,
    max_depth: Optional[int] = None,
    abbreviate_field_names: Optional[bool] = None,
    max_field_component_length: Optional[int] = None,
    preserve_leaf_component: Optional[bool] = None,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    current_depth: int = 0,
    in_place: bool = False,
    mode: Literal["standard", "streaming"] = "standard",
) -> Dict[str, Any]
```

Flattens a nested JSON object into a single-level dictionary.

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | Dict[str, Any] | Required | Dictionary to flatten |
| separator | str | None | Separator to use between path segments |
| cast_to_string | bool | None | Whether to cast all values to strings |
| include_empty | bool | None | Whether to include empty strings |
| skip_null | bool | None | Whether to skip null values |
| skip_arrays | bool | True | Whether to skip array flattening |
| visit_arrays | bool | None | Whether to visit array elements |
| parent_path | str | "" | Path prefix from parent object |
| path_parts | List[str] | None | List of path components (for optimization) |
| path_parts_optimization | bool | None | Whether to use path parts optimization |
| max_depth | int | None | Maximum recursion depth |
| abbreviate_field_names | bool | None | Whether to abbreviate field names |
| max_field_component_length | int | None | Maximum length for each component |
| preserve_leaf_component | bool | None | Whether to preserve the leaf component |
| custom_abbreviations | Dict[str, str] | None | Custom abbreviation dictionary |
| current_depth | int | 0 | Current recursion depth |
| in_place | bool | False | Whether to modify the original object in place |
| mode | str | "standard" | Processing mode ("standard" for regular processing, "streaming" for memory-efficient) |

#### Returns

A flattened dictionary where nested keys are joined with the specified separator.

#### Example

```python
from transmog.core.flattener import flatten_json

data = {
    "user": {
        "id": 1,
        "name": "John Doe",
        "address": {
            "street": "123 Main St",
            "city": "Anytown"
        }
    }
}

# Standard mode
result = flatten_json(data, separator="_", cast_to_string=True, mode="standard")
print(result)
# Output:
# {
#   "user_id": "1",
#   "user_name": "John Doe",
#   "user_address_street": "123 Main St",
#   "user_address_city": "Anytown"
# }

# Streaming mode (memory-efficient)
result = flatten_json(data, separator="_", cast_to_string=True, mode="streaming")
```

## Extractor Module

### extract_arrays

```python
extract_arrays(
    obj: Dict[str, Any],
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "root",
    separator: str = None,
    cast_to_string: bool = None,
    include_empty: bool = None,
    skip_null: bool = None,
    extract_time: Optional[Any] = None,
    parent_path_parts: Optional[List[str]] = None,
    abbreviate_enabled: Optional[bool] = None,
    max_component_length: Optional[int] = None,
    preserve_leaf: Optional[bool] = None,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> Dict[str, List[Dict[str, Any]]]
```

Extracts nested arrays from JSON structure with parent-child relationships.

## Hierarchy Module

### process_structure

```python
process_structure(
    data: Dict[str, Any],
    entity_name: str,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    extract_time: Optional[Any] = None,
    root_entity: Optional[str] = None,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> Tuple[Dict[str, Any], Dict[str, List[Dict[str, Any]]]]
```

Processes JSON structure with parent-child relationship preservation. This is the main entry point for
processing a complete JSON structure, handling both the main record and all nested arrays.

## Metadata Module

### annotate_with_metadata

```python
annotate_with_metadata(
    record: Dict[str, Any],
    parent_id: Optional[str] = None,
    extract_id: Optional[str] = None,
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    extra_fields: Optional[Dict[str, Any]] = None,
    in_place: bool = False,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> Dict[str, Any]
```

Annotates a record with metadata fields like extract ID, parent ID, and timestamp.

### generate_extract_id

```python
generate_extract_id(
    record: Optional[Dict[str, Any]] = None,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> str
```

Generates a unique ID for record tracking. This function can generate IDs in three ways:

1. Random UUID (default)
2. Deterministic UUID based on record field(s)
3. Custom function-based ID generation
