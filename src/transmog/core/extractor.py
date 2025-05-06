"""
Array extraction module for nested JSON structures.

Provides functions to extract arrays from nested JSON objects
and transform them into separate table structures with relationship preservation.
"""

from typing import Any, Dict, List, Optional, Set, Tuple, Callable, Generator

from transmog.core.flattener import flatten_json
from transmog.core.metadata import annotate_with_metadata
from transmog.naming.conventions import sanitize_name, split_path, join_path
from transmog.naming.abbreviator import (
    abbreviate_table_name,
    get_common_abbreviations,
)
from transmog.config.settings import settings

# Type aliases
JsonDict = Dict[str, Any]
FlatDict = Dict[str, Any]
ArrayDict = Dict[str, List[Dict[str, Any]]]
VisitedPath = Set[int]  # Set of object IDs to prevent circular reference processing
PathParts = List[str]  # Path components for efficient path building


def extract_arrays(
    obj: JsonDict,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "root",
    separator: str = None,
    cast_to_string: bool = None,
    include_empty: bool = None,
    skip_null: bool = None,
    extract_time: Optional[Any] = None,
    visited: Optional[VisitedPath] = None,
    parent_path_parts: Optional[PathParts] = None,
    shared_flatten_cache: Optional[Dict[int, FlatDict]] = None,
    abbreviate_enabled: Optional[bool] = None,
    max_component_length: Optional[int] = None,
    preserve_leaf: Optional[bool] = None,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    default_id_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
) -> ArrayDict:
    """
    Extract nested arrays from JSON structure with parent-child relationships.

    Args:
        obj: JSON object potentially containing arrays
        parent_id: UUID of parent record
        parent_path: Path from root for naming
        entity_name: Name of the entity
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        extract_time: Extraction timestamp
        visited: Set of object IDs to prevent circular references
        parent_path_parts: Path components for efficient path building
        shared_flatten_cache: Optional cache of already flattened objects
        abbreviate_enabled: Whether to abbreviate table names
        max_component_length: Maximum component length for abbreviation
        preserve_leaf: Whether to preserve leaf components in abbreviation
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Field name to use for deterministic ID generation
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Recovery strategy for handling circular references

    Returns:
        Dictionary of arrays keyed by their path
    """
    # Get default values from settings if not provided
    if separator is None:
        separator = settings.get_option("separator")

    if cast_to_string is None:
        cast_to_string = settings.get_option("cast_to_string")

    if include_empty is None:
        include_empty = settings.get_option("include_empty")

    if skip_null is None:
        skip_null = settings.get_option("skip_null")

    if visited is None:
        visited = set()

    if shared_flatten_cache is None:
        shared_flatten_cache = {}

    # Initialize path parts if needed
    if parent_path_parts is None:
        if parent_path:
            parent_path_parts = list(split_path(parent_path, separator))
        else:
            parent_path_parts = []

    # Get abbreviation settings
    if abbreviate_enabled is None:
        abbreviate_enabled = settings.get_option("abbreviate_table_names")

    if max_component_length is None:
        max_component_length = settings.get_option("max_table_component_length")

    if preserve_leaf is None:
        preserve_leaf = settings.get_option("preserve_leaf_component")

    if custom_abbreviations is None:
        custom_abbreviations = settings.get_option("custom_abbreviations")

    # Merge custom abbreviations with defaults if provided
    abbreviation_dict = None
    if custom_abbreviations:
        default_abbrevs = get_common_abbreviations()
        abbreviation_dict = default_abbrevs.copy()
        abbreviation_dict.update(custom_abbreviations)

    # Prevent circular references
    obj_id = id(obj)
    if obj_id in visited:
        return {}

    visited.add(obj_id)

    arrays: Dict[str, List[Dict[str, Any]]] = {}

    for key, value in obj.items():
        # Create sanitized key for path building
        sanitized_key = sanitize_name(key, separator, "")

        # Build current path parts efficiently
        current_parts = parent_path_parts.copy()
        current_parts.append(sanitized_key)

        # Create path string only when needed (for table keys)
        current_path = (
            join_path(tuple(current_parts), separator)
            if current_parts
            else sanitized_key
        )

        if isinstance(value, list):
            array_items = []

            for item in value:
                if isinstance(item, dict):
                    # Process this array item
                    item_id = None  # Will be generated in annotate_with_metadata

                    # Use cached flattened result if available
                    item_obj_id = id(item)
                    if item_obj_id in shared_flatten_cache:
                        flat_item = shared_flatten_cache[item_obj_id]
                    else:
                        # Flatten the array item
                        flat_item = flatten_json(
                            item,
                            separator=separator,
                            cast_to_string=cast_to_string,
                            include_empty=include_empty,
                            skip_arrays=True,
                            skip_null=skip_null,
                            recovery_strategy=recovery_strategy,
                        )
                        # Cache the flattened result
                        shared_flatten_cache[item_obj_id] = flat_item

                    # Add metadata - use in_place=True for better performance
                    annotated_item = annotate_with_metadata(
                        flat_item,
                        parent_id=parent_id,
                        extract_time=extract_time,
                        in_place=True,
                        source_field=default_id_field,
                        id_generation_strategy=id_generation_strategy,
                    )

                    # Get the extract ID that was generated
                    item_id = annotated_item.get("__extract_id")
                    array_items.append(annotated_item)

                    # Process nested arrays within this item
                    nested_arrays = extract_arrays(
                        item,
                        parent_id=item_id,
                        parent_path=current_path,
                        entity_name=sanitized_key,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        extract_time=extract_time,
                        visited=visited,
                        parent_path_parts=current_parts,
                        shared_flatten_cache=shared_flatten_cache,
                        abbreviate_enabled=abbreviate_enabled,
                        max_component_length=max_component_length,
                        preserve_leaf=preserve_leaf,
                        custom_abbreviations=custom_abbreviations,
                        default_id_field=default_id_field,
                        id_generation_strategy=id_generation_strategy,
                        recovery_strategy=recovery_strategy,
                    )

                    # Add nested arrays to results efficiently
                    for nested_path, nested_items in nested_arrays.items():
                        if nested_items:
                            # Get abbreviated path for table name if enabled
                            if abbreviate_enabled:
                                abbreviated_path = abbreviate_table_name(
                                    nested_path,
                                    parent_entity=entity_name,
                                    separator=separator,
                                    abbreviate_enabled=abbreviate_enabled,
                                    max_component_length=max_component_length,
                                    preserve_leaf=preserve_leaf,
                                    abbreviation_dict=abbreviation_dict,
                                )
                                # Use abbreviated path as the key
                                arrays.setdefault(abbreviated_path, []).extend(
                                    nested_items
                                )
                            else:
                                arrays.setdefault(nested_path, []).extend(nested_items)

            # Add this array to results if it has items
            if array_items:
                # Get abbreviated path for table name if enabled
                if abbreviate_enabled and entity_name:
                    if current_path:
                        table_path = f"{entity_name.lower()}_{current_path.lower()}"
                    else:
                        table_path = entity_name.lower()

                    if abbreviation_dict and max_component_length:
                        abbreviated_path = abbreviate_table_name(
                            table_path,
                            parent_entity=entity_name,
                            separator=separator,
                            max_component_length=max_component_length,
                            preserve_leaf=preserve_leaf,
                            abbreviation_dict=abbreviation_dict,
                        )
                    else:
                        abbreviated_path = table_path
                elif entity_name and current_path:
                    abbreviated_path = f"{entity_name.lower()}_{current_path.lower()}"
                elif entity_name:
                    abbreviated_path = entity_name.lower()
                else:
                    abbreviated_path = current_path

                arrays[abbreviated_path] = array_items

            # For object keys with arrays, we want to continue processing
            # other keys even after we've processed the array, because
            # they might have other nested arrays

        # Process nested objects if they might contain arrays
        elif isinstance(value, dict):
            # Recursively process nested objects for arrays
            nested_arrays = extract_arrays(
                value,
                parent_id=parent_id,
                parent_path=current_path,
                entity_name=entity_name,
                separator=separator,
                cast_to_string=cast_to_string,
                include_empty=include_empty,
                skip_null=skip_null,
                extract_time=extract_time,
                visited=visited,
                parent_path_parts=current_parts,
                shared_flatten_cache=shared_flatten_cache,
                abbreviate_enabled=abbreviate_enabled,
                max_component_length=max_component_length,
                preserve_leaf=preserve_leaf,
                custom_abbreviations=custom_abbreviations,
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=recovery_strategy,
            )

            # Add nested arrays to results
            for nested_path, nested_items in nested_arrays.items():
                arrays.setdefault(nested_path, []).extend(nested_items)

    return arrays


def stream_extract_arrays(
    obj: JsonDict,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "root",
    separator: str = None,
    cast_to_string: bool = None,
    include_empty: bool = None,
    skip_null: bool = None,
    extract_time: Optional[Any] = None,
    visited: Optional[VisitedPath] = None,
    parent_path_parts: Optional[PathParts] = None,
    shared_flatten_cache: Optional[Dict[int, FlatDict]] = None,
    abbreviate_enabled: Optional[bool] = None,
    max_component_length: Optional[int] = None,
    preserve_leaf: Optional[bool] = None,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    default_id_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    recovery_strategy: Optional[Any] = None,
) -> Generator[Tuple[str, FlatDict], None, None]:
    """
    Stream extract arrays with yield for generator-based processing.

    This version yields each flattened array item as it's processed
    rather than building a complete result in memory.

    Args:
        obj: JSON object potentially containing arrays
        parent_id: UUID of parent record
        parent_path: Path from root for naming
        entity_name: Name of the entity
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        extract_time: Extraction timestamp
        visited: Set of object IDs to prevent circular references
        parent_path_parts: Path components for efficient path building
        shared_flatten_cache: Optional cache of already flattened objects
        abbreviate_enabled: Whether to abbreviate table names
        max_component_length: Maximum component length for abbreviation
        preserve_leaf: Whether to preserve leaf components in abbreviation
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Field name to use for deterministic ID generation
        id_generation_strategy: Custom function for ID generation
        id_field: Field name for the record ID
        parent_field: Field name for the parent record ID
        time_field: Field name for the extraction timestamp
        recovery_strategy: Recovery strategy for handling circular references

    Yields:
        Tuples of (table_name, flattened_record)
    """
    # Get default values from settings if not provided
    if separator is None:
        separator = settings.get_option("separator")

    if cast_to_string is None:
        cast_to_string = settings.get_option("cast_to_string")

    if include_empty is None:
        include_empty = settings.get_option("include_empty")

    if skip_null is None:
        skip_null = settings.get_option("skip_null")

    if visited is None:
        visited = set()

    if shared_flatten_cache is None:
        shared_flatten_cache = {}

    # Initialize path parts if needed
    if parent_path_parts is None:
        if parent_path:
            parent_path_parts = list(split_path(parent_path, separator))
        else:
            parent_path_parts = []

    # Get abbreviation settings
    if abbreviate_enabled is None:
        abbreviate_enabled = settings.get_option("abbreviate_table_names")

    if max_component_length is None:
        max_component_length = settings.get_option("max_table_component_length")

    if preserve_leaf is None:
        preserve_leaf = settings.get_option("preserve_leaf_component")

    if custom_abbreviations is None:
        custom_abbreviations = settings.get_option("custom_abbreviations")

    # Prevent circular references
    obj_id = id(obj)
    if obj_id in visited:
        # For streaming, we simply return without yielding anything
        return
    visited.add(obj_id)

    for key, value in obj.items():
        # Create sanitized key for path building
        sanitized_key = sanitize_name(key, separator, "")

        # Build current path parts efficiently
        current_parts = parent_path_parts.copy()
        current_parts.append(sanitized_key)

        # Create path string only when needed (for table keys)
        current_path = (
            join_path(tuple(current_parts), separator)
            if current_parts
            else sanitized_key
        )

        if isinstance(value, list):
            # Get table name - possibly abbreviated based on settings
            if abbreviate_enabled:
                # Get parent entity for naming
                parent_entity = entity_name
                table_name = abbreviate_table_name(
                    current_path,
                    parent_entity=parent_entity,
                    separator=separator,
                    max_component_length=max_component_length,
                    preserve_leaf=preserve_leaf,
                    abbreviation_dict=custom_abbreviations,
                )
            else:
                # Use standard table naming without abbreviation
                if parent_path:
                    # This is a nested table, name it with the parent for clarity
                    table_name = f"{entity_name}{separator}{current_path}"
                else:
                    # This is a top-level array in the entity
                    table_name = f"{entity_name}{separator}{sanitized_key}"

            for item in value:
                if isinstance(item, dict):
                    # Use cached flattened result if available for performance
                    item_obj_id = id(item)
                    if item_obj_id in shared_flatten_cache:
                        flat_item = shared_flatten_cache[item_obj_id]
                    else:
                        # Flatten the array item
                        flat_item = flatten_json(
                            item,
                            separator=separator,
                            cast_to_string=cast_to_string,
                            include_empty=include_empty,
                            skip_arrays=True,
                            skip_null=skip_null,
                            mode="streaming",
                            recovery_strategy=recovery_strategy,
                        )
                        # Cache the flattened result
                        shared_flatten_cache[item_obj_id] = flat_item

                    # Add metadata
                    annotated_item = annotate_with_metadata(
                        flat_item,
                        parent_id=parent_id,
                        extract_time=extract_time,
                        in_place=True,
                        id_field=id_field,
                        parent_field=parent_field,
                        time_field=time_field,
                        source_field=default_id_field,
                        id_generation_strategy=id_generation_strategy,
                    )

                    # Yield this item with its table name
                    yield (table_name, annotated_item)

                    # Get the generated ID
                    item_id = annotated_item.get(id_field)

                    # Process nested arrays recursively, yielding each result
                    yield from stream_extract_arrays(
                        item,
                        parent_id=item_id,
                        parent_path=current_path,
                        entity_name=entity_name,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        extract_time=extract_time,
                        visited=visited,
                        parent_path_parts=current_parts,
                        shared_flatten_cache=shared_flatten_cache,
                        abbreviate_enabled=abbreviate_enabled,
                        max_component_length=max_component_length,
                        preserve_leaf=preserve_leaf,
                        custom_abbreviations=custom_abbreviations,
                        default_id_field=default_id_field,
                        id_generation_strategy=id_generation_strategy,
                        id_field=id_field,
                        parent_field=parent_field,
                        time_field=time_field,
                        recovery_strategy=recovery_strategy,
                    )
