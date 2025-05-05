"""
Flattener module for converting nested JSON structures.

This module provides functions for flattening nested JSON structures
with proper error handling and recovery mechanisms.
"""

import functools
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    cast,
    Generator,
    Literal,
    Callable,
)

from ..error import (
    CircularReferenceError,
    ProcessingError,
    error_context,
    handle_circular_reference,
    logger,
)
from ..naming.abbreviator import abbreviate_field_name, get_common_abbreviations
from ..naming.conventions import sanitize_name
from ..config.settings import settings


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
    # Handle None/null values
    if value is None:
        return None if skip_null else ""

    # Handle empty strings
    if value == "" and not include_empty:
        return None

    # Handle NaN values
    if isinstance(value, float) and (
        value != value or value == float("inf") or value == float("-inf")
    ):
        return "_error_invalid_float" if cast_to_string else value

    # Cast to string if configured
    if cast_to_string:
        if isinstance(value, bool):
            return "true" if value else "false"
        elif not isinstance(value, str):
            return str(value)

    return value


# Cache for process_value function to avoid frequent recomputation
# Different caches for different processing contexts
_standard_process_cache = {}
_streaming_process_cache = {}

# Type for cache context selection
CacheContext = Literal["standard", "streaming"]


def _get_process_cache(
    context: CacheContext,
) -> Dict[Tuple[Any, bool, bool, bool], Any]:
    """
    Get the appropriate process cache based on context.

    Args:
        context: The processing context

    Returns:
        Cache dictionary for the given context
    """
    if context == "streaming":
        return _streaming_process_cache
    return _standard_process_cache


def _clear_process_cache(context: CacheContext) -> None:
    """
    Clear the process cache for the given context.

    Args:
        context: The processing context
    """
    if context == "streaming":
        _streaming_process_cache.clear()
    else:
        _standard_process_cache.clear()


def _make_cache_key(
    value: Any, cast_to_string: bool, include_empty: bool, skip_null: bool
) -> Tuple[int, bool, bool, bool]:
    """
    Create a cache key for process_value.

    Args:
        value: The value to process
        cast_to_string: Whether to cast to string
        include_empty: Whether to include empty strings
        skip_null: Whether to skip null values

    Returns:
        Cache key tuple
    """
    # Use id for mutable objects to avoid hash errors
    if isinstance(value, (dict, list, set)):
        return (id(value), cast_to_string, include_empty, skip_null)
    else:
        try:
            # Try to hash the value
            hash(value)
            return (hash(value), cast_to_string, include_empty, skip_null)
        except TypeError:
            # Fall back to id for unhashable objects
            return (id(value), cast_to_string, include_empty, skip_null)


def _process_value_wrapper(
    value: Any,
    cast_to_string: bool,
    include_empty: bool,
    skip_null: bool,
    context: CacheContext = "standard",
) -> Optional[Any]:
    """
    Process a value with caching based on context.

    Args:
        value: The value to process
        cast_to_string: Whether to cast to string
        include_empty: Whether to include empty strings
        skip_null: Whether to skip null values
        context: Cache context

    Returns:
        Processed value or None if it should be skipped
    """
    # Special values are handled directly
    if value is None or value == "":
        return _process_value(value, cast_to_string, include_empty, skip_null)

    # For simple scalars, don't use cache
    if isinstance(value, (int, float, bool)):
        return _process_value(value, cast_to_string, include_empty, skip_null)

    # Get the appropriate cache
    cache = _get_process_cache(context)
    cache_key = _make_cache_key(value, cast_to_string, include_empty, skip_null)

    # Check cache first
    if cache_key in cache:
        return cache[cache_key]

    # Process and cache the result
    result = _process_value(value, cast_to_string, include_empty, skip_null)
    cache[cache_key] = result
    return result


def _flatten_json_core(
    data: Dict[str, Any],
    separator: str,
    cast_to_string: bool,
    include_empty: bool,
    skip_null: bool,
    skip_arrays: bool,
    visit_arrays: bool,
    visited: Set[int],
    parent_path: str,
    path_parts: Optional[List[str]],
    path_parts_optimization: bool,
    max_depth: Optional[int],
    abbreviate_field_names: bool,
    max_field_component_length: Optional[int],
    preserve_leaf_component: bool,
    custom_abbreviations: Optional[Dict[str, str]],
    current_depth: int,
    in_place: bool,
    context: CacheContext,
    flatten_func: Callable,
) -> Dict[str, Any]:
    """
    Core implementation of JSON flattening logic used by both standard and streaming functions.

    Args:
        data: Dictionary to flatten
        separator: Separator to use between path segments
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty strings
        skip_null: Whether to skip null values
        skip_arrays: Whether to skip array flattening
        visit_arrays: Whether to visit array elements
        visited: Set of visited object IDs (for circular reference detection)
        parent_path: Path prefix from parent object
        path_parts: List of path components (for optimization)
        path_parts_optimization: Whether to use path parts optimization
        max_depth: Maximum recursion depth
        abbreviate_field_names: Whether to abbreviate field names
        max_field_component_length: Maximum length for each component
        preserve_leaf_component: Whether to preserve the leaf component
        custom_abbreviations: Custom abbreviation dictionary
        current_depth: Current recursion depth
        in_place: Whether to modify the original object in place
        context: Cache context ("standard" or "streaming")
        flatten_func: The flattening function to call recursively

    Returns:
        Flattened dictionary
    """
    # Check for max recursion depth
    if max_depth is not None and current_depth > max_depth:
        logger.warning(
            f"Maximum nesting depth ({max_depth}) exceeded, truncating nested content"
        )
        return {}

    # Initialize result dictionary
    result = {}

    # Check for circular references by object id
    obj_id = id(data)
    if obj_id in visited:
        # Instead of raising an exception, return a placeholder for circular references
        circular_ref_value = "[Circular Reference]"
        if cast_to_string:
            circular_ref_value = str(circular_ref_value)

        if parent_path:
            key = f"{parent_path}{separator}_circular_reference"
        else:
            key = "_circular_reference"

        # Apply abbreviation if needed
        if abbreviate_field_names and custom_abbreviations:
            key = abbreviate_field_name(
                key,
                separator=separator,
                max_component_length=max_field_component_length,
                preserve_leaf=preserve_leaf_component,
                abbreviation_dict=custom_abbreviations,
            )

        result[key] = circular_ref_value
        return result

    # Add the object to visited set
    visited.add(obj_id)

    # Get abbreviation dictionary if needed
    abbreviation_dict = None
    if abbreviate_field_names and custom_abbreviations:
        default_abbrevs = get_common_abbreviations()
        abbreviation_dict = default_abbrevs.copy()
        abbreviation_dict.update(custom_abbreviations)
    elif abbreviate_field_names:
        abbreviation_dict = get_common_abbreviations()

    try:
        # Process each key in the object
        for key, value in data.items():
            # Sanitize the key to be safe for path construction
            current_key = sanitize_name(key, separator, "")

            # Build the new key path
            if parent_path:
                new_key = f"{parent_path}{separator}{current_key}"
            else:
                new_key = current_key

            # Handle nested objects
            if isinstance(value, dict):
                # Get new path parts for optimization if enabled
                new_path_parts = (
                    path_parts + [current_key] if path_parts_optimization else None
                )

                # Try to recursively flatten nested dictionaries, handling circular references
                try:
                    nested_flat = flatten_func(
                        value,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        visited=visited,
                        parent_path=new_key,
                        path_parts=new_path_parts,
                        path_parts_optimization=path_parts_optimization,
                        max_depth=max_depth,
                        abbreviate_field_names=abbreviate_field_names,
                        max_field_component_length=max_field_component_length,
                        preserve_leaf_component=preserve_leaf_component,
                        custom_abbreviations=custom_abbreviations,
                        current_depth=current_depth + 1,
                        in_place=in_place,
                        mode=context,
                    )
                    # Update result with flattened nested dictionary
                    result.update(nested_flat)
                except CircularReferenceError:
                    # Handle circular reference by adding a marker
                    circular_ref_key = f"{new_key}{separator}_circular_reference"
                    if abbreviate_field_names and abbreviation_dict:
                        circular_ref_key = abbreviate_field_name(
                            circular_ref_key,
                            separator=separator,
                            max_component_length=max_field_component_length,
                            preserve_leaf=preserve_leaf_component,
                            abbreviation_dict=abbreviation_dict,
                        )
                    result[circular_ref_key] = (
                        "[Circular Reference]"
                        if not cast_to_string
                        else "[Circular Reference]"
                    )

            # Handle array values if not skipping
            elif isinstance(value, list) and not skip_arrays:
                # Skip empty arrays
                if not value:
                    continue

                # Format array values according to configuration
                if visit_arrays:
                    # If visit_arrays is True, treat array items as object fields
                    for i, item in enumerate(value):
                        # Build array item path
                        item_key = f"{new_key}{separator}{i}"

                        # Handle different item types
                        if isinstance(item, dict):
                            # Get new path parts for optimization if enabled
                            new_path_parts = (
                                path_parts + [current_key, str(i)]
                                if path_parts_optimization
                                else None
                            )

                            # Try to recursively flatten nested dictionary in array, handling circular references
                            try:
                                nested_flat = flatten_func(
                                    item,
                                    separator=separator,
                                    cast_to_string=cast_to_string,
                                    include_empty=include_empty,
                                    skip_null=skip_null,
                                    visited=visited,
                                    parent_path=item_key,
                                    path_parts=new_path_parts,
                                    path_parts_optimization=path_parts_optimization,
                                    max_depth=max_depth,
                                    abbreviate_field_names=abbreviate_field_names,
                                    max_field_component_length=max_field_component_length,
                                    preserve_leaf_component=preserve_leaf_component,
                                    custom_abbreviations=custom_abbreviations,
                                    current_depth=current_depth + 1,
                                    in_place=in_place,
                                    mode=context,
                                )
                                # Update result with flattened nested dictionary
                                result.update(nested_flat)
                            except CircularReferenceError:
                                # Handle circular reference by adding a marker
                                circular_ref_key = (
                                    f"{item_key}{separator}_circular_reference"
                                )
                                if abbreviate_field_names and abbreviation_dict:
                                    circular_ref_key = abbreviate_field_name(
                                        circular_ref_key,
                                        separator=separator,
                                        max_component_length=max_field_component_length,
                                        preserve_leaf=preserve_leaf_component,
                                        abbreviation_dict=abbreviation_dict,
                                    )
                                result[circular_ref_key] = (
                                    "[Circular Reference]"
                                    if not cast_to_string
                                    else "[Circular Reference]"
                                )
                        else:
                            # Process non-object item as array element
                            processed_value = _process_value_wrapper(
                                item,
                                cast_to_string=cast_to_string,
                                include_empty=include_empty,
                                skip_null=skip_null,
                                context=context,
                            )

                            if processed_value is not None:
                                if abbreviate_field_names and abbreviation_dict:
                                    # Abbreviate the key
                                    abbreviated_key = abbreviate_field_name(
                                        item_key,
                                        separator=separator,
                                        max_component_length=max_field_component_length,
                                        preserve_leaf=preserve_leaf_component,
                                        abbreviation_dict=abbreviation_dict,
                                    )
                                    result[abbreviated_key] = processed_value
                                else:
                                    result[item_key] = processed_value
                else:
                    # Format the entire array as a single value
                    processed_value = _process_value_wrapper(
                        value,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        context=context,
                    )

                    if processed_value is not None:
                        if abbreviate_field_names and abbreviation_dict:
                            # Abbreviate the key
                            abbreviated_key = abbreviate_field_name(
                                new_key,
                                separator=separator,
                                max_component_length=max_field_component_length,
                                preserve_leaf=preserve_leaf_component,
                                abbreviation_dict=abbreviation_dict,
                            )
                            result[abbreviated_key] = processed_value
                        else:
                            result[new_key] = processed_value

            # Process non-array, non-object values directly
            else:
                # Process the value
                processed_value = _process_value_wrapper(
                    value,
                    cast_to_string=cast_to_string,
                    include_empty=include_empty,
                    skip_null=skip_null,
                    context=context,  # Still pass but it will be ignored
                )

                # Add to result if not skipped
                if processed_value is not None:
                    if abbreviate_field_names and abbreviation_dict:
                        # Abbreviate the key
                        abbreviated_key = abbreviate_field_name(
                            new_key,
                            separator=separator,
                            max_component_length=max_field_component_length,
                            preserve_leaf=preserve_leaf_component,
                            abbreviation_dict=abbreviation_dict,
                        )
                        result[abbreviated_key] = processed_value
                    else:
                        result[new_key] = processed_value

        return result
    finally:
        # Remove from visited set when done to allow reuse of objects in different contexts
        if in_place and obj_id in visited:
            visited.remove(obj_id)


# Define a mode type to distinguish between standard and streaming modes
FlattenMode = Literal["standard", "streaming"]


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
    current_depth: int = 0,
    in_place: bool = False,
    mode: FlattenMode = "standard",
) -> Dict[str, Any]:
    """
    Flatten a nested JSON object into a single-level dictionary.

    Args:
        data: Dictionary to flatten
        separator: Separator to use between path segments
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty strings
        skip_null: Whether to skip null values
        skip_arrays: Whether to skip array flattening
        visit_arrays: Whether to visit array elements
        visited: Set of visited object IDs (for circular reference detection)
        parent_path: Path prefix from parent object
        path_parts: List of path components (for optimization)
        path_parts_optimization: Whether to use path parts optimization
        max_depth: Maximum recursion depth
        abbreviate_field_names: Whether to abbreviate field names
        max_field_component_length: Maximum length for each component
        preserve_leaf_component: Whether to preserve the leaf component
        custom_abbreviations: Custom abbreviation dictionary
        current_depth: Current recursion depth
        in_place: Whether to modify the original object in place
        mode: Processing mode ("standard" for regular processing, "streaming" for memory-efficient)

    Returns:
        Flattened dictionary
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

    # Get abbreviation settings if not provided
    if abbreviate_field_names is None:
        abbreviate_field_names = settings.get_option("abbreviate_field_names")

    if max_field_component_length is None:
        max_field_component_length = settings.get_option("max_field_component_length")

    if preserve_leaf_component is None:
        preserve_leaf_component = settings.get_option("preserve_leaf_component")

    if custom_abbreviations is None:
        custom_abbreviations = settings.get_option("custom_abbreviations")

    # Select the context and function reference based on mode
    context: CacheContext = mode
    flatten_func_ref = flatten_json

    # Delegate to core implementation
    return _flatten_json_core(
        data=data,
        separator=separator,
        cast_to_string=cast_to_string,
        include_empty=include_empty,
        skip_null=skip_null,
        skip_arrays=skip_arrays,
        visit_arrays=visit_arrays,
        visited=visited,
        parent_path=parent_path,
        path_parts=path_parts,
        path_parts_optimization=path_parts_optimization,
        max_depth=max_depth,
        abbreviate_field_names=abbreviate_field_names,
        max_field_component_length=max_field_component_length,
        preserve_leaf_component=preserve_leaf_component,
        custom_abbreviations=custom_abbreviations,
        current_depth=current_depth,
        in_place=in_place,
        context=context,
        flatten_func=flatten_func_ref,
    )
