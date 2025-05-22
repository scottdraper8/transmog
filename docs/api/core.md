# Core API Reference

> **User Guide**: For a user-friendly introduction, see the [Data Transformation Guide](../user/processing/data-transformation.md).

This document provides technical reference for Transmog's core components and classes.

## Flattening Module

### flatten_json

```python
flatten_json(
    data: dict[str, Any],
    separator: str = None,
    cast_to_string: bool = None,
    include_empty: bool = None,
    skip_null: bool = None,
    skip_arrays: bool = True,
    visit_arrays: bool = None,
    parent_path: str = "",
    path_parts: Optional[list[str]] = None,
    path_parts_optimization: bool = None,
    max_depth: Optional[int] = None,
    max_field_component_length: Optional[int] = None,
    preserve_leaf_component: Optional[bool] = None,
    deep_nesting_threshold: Optional[int] = None,
    current_depth: int = 0,
    in_place: bool = False,
    mode: Literal["standard", "streaming"] = "standard",
) -> dict[str, Any]
```

Flattens a nested JSON object into a single-level dictionary.

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | dict[str, Any] | Required | Dictionary to flatten |
| separator | str | None | Separator to use between path segments |
| cast_to_string | bool | None | Whether to cast all values to strings |
| include_empty | bool | None | Whether to include empty strings |
| skip_null | bool | None | Whether to skip null values |
| skip_arrays | bool | True | Whether to skip array flattening |
| visit_arrays | bool | None | Whether to visit array elements |
| parent_path | str | "" | Path prefix from parent object |
| path_parts | list[str] | None | List of path components (for optimization) |
| path_parts_optimization | bool | None | Whether to use path parts optimization |
| max_depth | int | None | Maximum recursion depth |
| max_field_component_length | int | None | Maximum length for each component |
| preserve_leaf_component | bool | None | Whether to preserve the leaf component |
| deep_nesting_threshold | int | None | Threshold for special handling of deeply nested structures |
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
    obj: dict[str, Any],
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "root",
    separator: str = None,
    cast_to_string: bool = None,
    include_empty: bool = None,
    skip_null: bool = None,
    extract_time: Optional[Any] = None,
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
    extract_time: Optional[Any] = None,
    root_entity: Optional[str] = None,
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    deep_nesting_threshold: int = 4,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
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
    extract_id: Optional[str] = None,
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    extra_fields: Optional[dict[str, Any]] = None,
    in_place: bool = False,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
) -> dict[str, Any]
```

Annotates a record with metadata fields like extract ID, parent ID, and timestamp.

### generate_extract_id

```python
generate_extract_id(
    record: Optional[dict[str, Any]] = None,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
) -> str
```

Generates a unique ID for record tracking. This function can generate IDs in three ways:

1. Random UUID (default)
2. Deterministic UUID based on record field(s)
3. Custom function-based ID generation
