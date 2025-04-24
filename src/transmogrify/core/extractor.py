"""
Array extraction module for nested JSON structures.

Provides functions to extract arrays from nested JSON objects
and transform them into separate table structures with relationship preservation.
"""

from typing import Any, Dict, List, Optional, Set, Tuple, Callable

from src.transmogrify.core.flattener import flatten_json
from src.transmogrify.core.metadata import annotate_with_metadata
from src.transmogrify.naming.conventions import sanitize_name, split_path, join_path
from src.transmogrify.naming.abbreviator import (
    abbreviate_table_name,
    get_common_abbreviations,
)
from src.transmogrify.config.settings import settings

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
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
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
        deterministic_id_fields: Dict mapping paths to field names for deterministic IDs
        id_generation_strategy: Custom function for ID generation

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
                        )
                        # Cache the flattened result
                        shared_flatten_cache[item_obj_id] = flat_item

                    # Determine source field for deterministic ID generation
                    source_field = None
                    if deterministic_id_fields:
                        # Normalize path for comparison
                        normalized_path = (
                            current_path.strip(separator) if current_path else ""
                        )

                        # Exact path match first
                        if normalized_path in deterministic_id_fields:
                            source_field = deterministic_id_fields[normalized_path]
                        # Then try wildcard matches
                        elif "*" in deterministic_id_fields:
                            source_field = deterministic_id_fields["*"]
                        # Try path prefix matches (most specific to least)
                        else:
                            path_components = (
                                normalized_path.split(separator)
                                if normalized_path
                                else []
                            )
                            for i in range(len(path_components), 0, -1):
                                prefix = separator.join(path_components[:i])
                                prefix_wildcard = f"{prefix}{separator}*"
                                if prefix_wildcard in deterministic_id_fields:
                                    source_field = deterministic_id_fields[
                                        prefix_wildcard
                                    ]
                                    break

                    # Add metadata - use in_place=True for better performance
                    annotated_item = annotate_with_metadata(
                        flat_item,
                        parent_id=parent_id,
                        extract_time=extract_time,
                        in_place=True,
                        source_field=source_field,
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
                        deterministic_id_fields=deterministic_id_fields,
                        id_generation_strategy=id_generation_strategy,
                    )

                    # Add nested arrays to results efficiently
                    for nested_path, nested_items in nested_arrays.items():
                        if nested_items:
                            # Get abbreviated path for table name if enabled
                            if abbreviate_enabled:
                                abbreviated_path = abbreviate_table_name(
                                    nested_path,
                                    entity_name,
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
                if abbreviate_enabled:
                    abbreviated_path = abbreviate_table_name(
                        current_path,
                        entity_name,
                        separator=separator,
                        abbreviate_enabled=abbreviate_enabled,
                        max_component_length=max_component_length,
                        preserve_leaf=preserve_leaf,
                        abbreviation_dict=abbreviation_dict,
                    )
                    # Use abbreviated path as the key
                    arrays.setdefault(abbreviated_path, []).extend(array_items)
                else:
                    arrays.setdefault(current_path, []).extend(array_items)

        elif isinstance(value, dict):
            # Process nested object (not an array)
            nested_arrays = extract_arrays(
                value,
                parent_id=parent_id,
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
                deterministic_id_fields=deterministic_id_fields,
                id_generation_strategy=id_generation_strategy,
            )

            # Add nested arrays to results efficiently
            for nested_path, nested_items in nested_arrays.items():
                if nested_items:
                    # Get abbreviated path for table name if enabled
                    if abbreviate_enabled:
                        abbreviated_path = abbreviate_table_name(
                            nested_path,
                            entity_name,
                            separator=separator,
                            abbreviate_enabled=abbreviate_enabled,
                            max_component_length=max_component_length,
                            preserve_leaf=preserve_leaf,
                            abbreviation_dict=abbreviation_dict,
                        )
                        # Use abbreviated path as the key
                        arrays.setdefault(abbreviated_path, []).extend(nested_items)
                    else:
                        arrays.setdefault(nested_path, []).extend(nested_items)

    # Remove self from visited before returning
    if obj_id in visited:
        visited.remove(obj_id)

    return arrays
