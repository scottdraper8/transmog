"""
Flattener module for converting nested JSON structures.

This module provides functions for flattening nested JSON structures
with proper error handling and recovery mechanisms.
"""

import functools
from typing import Any, Dict, List, Optional, Set, Tuple, cast

from ..exceptions import CircularReferenceError, ProcessingError
from ..naming.abbreviator import abbreviate_field_name, get_common_abbreviations
from ..config.settings import settings
from .error_handling import error_context, handle_circular_reference, logger


def _process_value(
    value: Any, cast_to_string: bool, include_empty: bool, skip_null: bool
) -> Optional[Any]:
    """
    Process a value according to configuration settings.

    Args:
        value: The value to process
        cast_to_string: Whether to cast to string
        include_empty: Whether to include empty strings
        skip_null: Whether to skip null values

    Returns:
        Processed value or None if it should be skipped
    """
    if value is None:
        if skip_null:
            return None
        return "" if cast_to_string else None

    if cast_to_string:
        value = str(value)

    if value == "" and not include_empty:
        return None

    return value


# Update the _process_value_wrapper function
def _process_value_wrapper(
    value: Any, cast_to_string: bool, include_empty: bool, skip_null: bool
) -> Optional[Any]:
    """
    Wrapper for _process_value that handles unhashable types like lists.

    Args:
        value: The value to process
        cast_to_string: Whether to cast to string
        include_empty: Whether to include empty strings
        skip_null: Whether to skip null values

    Returns:
        Processed value or None if it should be skipped
    """
    # We can't use caching for unhashable types like lists or dicts
    if isinstance(value, (list, dict, set)):
        return _process_value(value, cast_to_string, include_empty, skip_null)

    # Try to use cached version for hashable types
    try:
        return _process_value_cached(value, cast_to_string, include_empty, skip_null)
    except TypeError:
        # Fall back to uncached version for any unhashable type
        return _process_value(value, cast_to_string, include_empty, skip_null)


# Cache common value processing for performance
# Get cache size from settings
LRU_CACHE_SIZE = settings.get_option("lru_cache_size")


@functools.lru_cache(maxsize=LRU_CACHE_SIZE)
def _process_value_cached(
    value: Any, cast_to_string: bool, include_empty: bool, skip_null: bool
) -> Optional[Any]:
    """Cached version of _process_value for hashable types."""
    return _process_value(value, cast_to_string, include_empty, skip_null)


@error_context("Failed to flatten JSON", wrap_as=lambda e: ProcessingError(str(e)))
def flatten_json(
    data: Dict[str, Any],
    separator: str = None,
    cast_to_string: bool = None,
    include_empty: bool = None,
    skip_null: bool = None,
    skip_arrays: bool = True,
    visit_arrays: bool = None,
    visited: Optional[Set[int]] = None,
    parent_path: str = "",
    path_parts: Optional[List[str]] = None,
    path_parts_optimization: bool = None,
    max_depth: Optional[int] = None,
    abbreviate_field_names: Optional[bool] = None,
    max_field_component_length: Optional[int] = None,
    preserve_leaf_component: Optional[bool] = None,
    custom_abbreviations: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Flatten a nested JSON object, with circular reference detection and error handling.

    Args:
        data: The JSON object to flatten
        separator: Separator character for nested keys
        cast_to_string: Whether to cast values to strings
        include_empty: Whether to include empty strings
        skip_null: Whether to skip null values
        skip_arrays: Whether to skip arrays
        visit_arrays: Whether to treat array items as fields
        visited: Set of visited object IDs for circular reference detection
        parent_path: Path from the parent object
        path_parts: List of path components for optimization
        path_parts_optimization: Whether to use path parts optimization
        max_depth: Maximum recursion depth
        abbreviate_field_names: Whether to abbreviate field names
        max_field_component_length: Maximum length for each component
        preserve_leaf_component: Whether to preserve the leaf component
        custom_abbreviations: Custom abbreviation dictionary

    Returns:
        Flattened dictionary

    Raises:
        ProcessingError: If processing fails
        CircularReferenceError: If a circular reference is detected
    """
    result: Dict[str, Any] = {}

    # Get default values from settings if not provided
    if separator is None:
        separator = settings.get_option("separator")

    if cast_to_string is None:
        cast_to_string = settings.get_option("cast_to_string")

    if include_empty is None:
        include_empty = settings.get_option("include_empty")

    if skip_null is None:
        skip_null = settings.get_option("skip_null")

    if visit_arrays is None:
        visit_arrays = settings.get_option("visit_arrays")

    if path_parts_optimization is None:
        path_parts_optimization = settings.get_option("path_parts_optimization")

    if max_depth is None:
        max_depth = settings.get_option("max_nesting_depth")

    # Initialize visitor tracking if not provided
    if visited is None:
        visited = set()

    # Initialize path components list if using optimization
    if path_parts_optimization and path_parts is None:
        path_parts = []

    # Get abbreviation settings
    if abbreviate_field_names is None:
        abbreviate_field_names = settings.get_option("abbreviate_field_names")

    if max_field_component_length is None:
        max_field_component_length = settings.get_option("max_field_component_length")

    if preserve_leaf_component is None:
        preserve_leaf_component = settings.get_option("preserve_leaf_component")

    if custom_abbreviations is None:
        custom_abbreviations = settings.get_option("custom_abbreviations")

    # Merge custom abbreviations with defaults if provided
    abbreviation_dict = None
    if custom_abbreviations:
        default_abbrevs = get_common_abbreviations()
        abbreviation_dict = default_abbrevs.copy()
        abbreviation_dict.update(custom_abbreviations)

    # Check for circular references
    obj_id = id(data)
    try:
        current_path = (
            path_parts
            if path_parts_optimization
            else [parent_path]
            if parent_path
            else []
        )
        handle_circular_reference(obj_id, visited, current_path, max_depth)
    except CircularReferenceError as e:
        logger.warning(f"Circular reference detected at path: {parent_path}")
        raise

    # Add to visited set
    visited.add(obj_id)

    try:
        # Process each key-value pair
        for key, value in data.items():
            # Build the new key
            if path_parts_optimization and path_parts:
                new_key_parts = path_parts.copy()
                new_key_parts.append(key)
                new_key = separator.join(new_key_parts)
            else:
                new_key = f"{parent_path}{separator}{key}" if parent_path else key

            # Apply field name abbreviation if enabled
            if abbreviate_field_names:
                new_key = abbreviate_field_name(
                    new_key,
                    separator=separator,
                    abbreviate_enabled=abbreviate_field_names,
                    max_component_length=max_field_component_length,
                    preserve_leaf=preserve_leaf_component,
                    abbreviation_dict=abbreviation_dict,
                )

            # Handle nested objects
            if isinstance(value, dict):
                # Recursively process nested object
                if path_parts_optimization:
                    new_path_parts = path_parts.copy()
                    new_path_parts.append(key)
                    nested = flatten_json(
                        value,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        skip_arrays=skip_arrays,
                        visit_arrays=visit_arrays,
                        visited=visited,
                        parent_path="",  # Not needed with path_parts
                        path_parts=new_path_parts,
                        path_parts_optimization=path_parts_optimization,
                        max_depth=max_depth,
                        abbreviate_field_names=abbreviate_field_names,
                        max_field_component_length=max_field_component_length,
                        preserve_leaf_component=preserve_leaf_component,
                        custom_abbreviations=custom_abbreviations,
                    )
                else:
                    nested = flatten_json(
                        value,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        skip_arrays=skip_arrays,
                        visit_arrays=visit_arrays,
                        visited=visited,
                        parent_path=new_key,
                        path_parts_optimization=path_parts_optimization,
                        max_depth=max_depth,
                        abbreviate_field_names=abbreviate_field_names,
                        max_field_component_length=max_field_component_length,
                        preserve_leaf_component=preserve_leaf_component,
                        custom_abbreviations=custom_abbreviations,
                    )

                # Merge nested results
                result.update(nested)

            # Handle arrays
            elif isinstance(value, list):
                if not skip_arrays and visit_arrays:
                    # Process array items with numeric indices
                    for i, item in enumerate(value):
                        item_key = f"{new_key}{separator}{i}"

                        if isinstance(item, dict):
                            # Recursively process dict array items
                            if path_parts_optimization:
                                new_path_parts = path_parts.copy()
                                new_path_parts.append(f"{key}{separator}{i}")
                                nested = flatten_json(
                                    item,
                                    separator=separator,
                                    cast_to_string=cast_to_string,
                                    include_empty=include_empty,
                                    skip_null=skip_null,
                                    skip_arrays=skip_arrays,
                                    visit_arrays=visit_arrays,
                                    visited=visited,
                                    path_parts=new_path_parts,
                                    path_parts_optimization=path_parts_optimization,
                                    max_depth=max_depth,
                                    abbreviate_field_names=abbreviate_field_names,
                                    max_field_component_length=max_field_component_length,
                                    preserve_leaf_component=preserve_leaf_component,
                                    custom_abbreviations=custom_abbreviations,
                                )
                            else:
                                nested = flatten_json(
                                    item,
                                    separator=separator,
                                    cast_to_string=cast_to_string,
                                    include_empty=include_empty,
                                    skip_null=skip_null,
                                    skip_arrays=skip_arrays,
                                    visit_arrays=visit_arrays,
                                    visited=visited,
                                    parent_path=item_key,
                                    path_parts_optimization=path_parts_optimization,
                                    max_depth=max_depth,
                                    abbreviate_field_names=abbreviate_field_names,
                                    max_field_component_length=max_field_component_length,
                                    preserve_leaf_component=preserve_leaf_component,
                                    custom_abbreviations=custom_abbreviations,
                                )

                            # Merge nested results
                            result.update(nested)
                        else:
                            # Process scalar array items
                            processed = _process_value_wrapper(
                                item, cast_to_string, include_empty, skip_null
                            )
                            if processed is not None:
                                result[item_key] = processed
                else:
                    # When skip_arrays=True or visit_arrays=False, just process the array as a single field
                    processed = _process_value_wrapper(
                        value, cast_to_string, include_empty, skip_null
                    )
                    if processed is not None:
                        result[new_key] = processed

            # Handle scalar values
            else:
                processed = _process_value_wrapper(
                    value, cast_to_string, include_empty, skip_null
                )
                if processed is not None:
                    result[new_key] = processed

        # Remove self from visited before returning
        visited.remove(obj_id)
        return result

    except Exception as e:
        # Log the error
        logger.error(f"Error flattening JSON at path '{parent_path}': {str(e)}")

        # Remove self from visited to prevent memory leaks
        if obj_id in visited:
            visited.remove(obj_id)

        # Re-raise as ProcessingError
        if not isinstance(e, (ProcessingError, CircularReferenceError)):
            raise ProcessingError(f"Failed to flatten JSON: {str(e)}", data=data) from e
        raise
