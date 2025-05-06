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
        if skip_null:
            return None
        else:
            # Convert None to empty string when not skipping nulls
            return ""

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
    preserve_root_component: bool,
    preserve_leaf_component: bool,
    custom_abbreviations: Optional[Dict[str, str]],
    current_depth: int,
    in_place: bool,
    context: CacheContext,
    flatten_func: Callable,
    recovery_strategy: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Core implementation of JSON flattening.

    This internal function handles the actual flattening logic.
    It's separated from the public interface to allow for proper error
    context and default parameter handling.
    """
    # Check for null or non-dictionary input
    if data is None:
        return {}

    if not isinstance(data, dict):
        raise TypeError(f"Expected dictionary, got {type(data).__name__}")

    # Detect circular references
    obj_id = id(data)
    if obj_id in visited:
        # Handle circular reference recovery if a strategy is provided
        if recovery_strategy is not None:
            try:
                # Convert path_parts to a path for the recovery strategy
                current_path = []
                if path_parts:
                    current_path = list(path_parts)
                elif parent_path:
                    current_path = [parent_path]

                # First, check if the strategy has the specialized circular reference handler
                if hasattr(recovery_strategy, "handle_circular_reference"):
                    circular_error = CircularReferenceError(
                        "Circular reference detected"
                    )
                    replacement = recovery_strategy.handle_circular_reference(
                        circular_error, current_path
                    )
                    # If replacement is a dict, return it directly; otherwise wrap it
                    if isinstance(replacement, dict):
                        return replacement
                    else:
                        return {"_circular_reference": True, "_value": replacement}
                # Otherwise try generic recovery
                elif hasattr(recovery_strategy, "recover"):
                    circular_error = CircularReferenceError(
                        "Circular reference detected"
                    )
                    replacement = recovery_strategy.recover(
                        circular_error, path=current_path
                    )
                    # If replacement is a dict, return it directly; otherwise wrap it
                    if isinstance(replacement, dict):
                        return replacement
                    else:
                        return {"_circular_reference": True, "_value": replacement}
            except Exception as e:
                # Log recovery failure but continue with standard error
                logger.warning(f"Failed to recover from circular reference: {e}")

        # No recovery strategy or recovery failed, raise the standard error
        raise CircularReferenceError("Circular reference detected")

    visited.add(obj_id)

    try:
        # Initialize result - either a new dict or use the input data
        if in_place and isinstance(data, dict):
            result = data
            # Create a copy of the keys to safely iterate while modifying
            keys_to_process = list(data.keys())

            # Store items to remove after processing (instead of removing during iteration)
            keys_to_remove = []
        else:
            result = {}
            # For non-in-place, we can directly use the keys
            keys_to_process = list(data.keys())
            keys_to_remove = []

        # Prepare abbreviation dictionary
        abbreviation_dict = None
        if abbreviate_field_names and custom_abbreviations:
            common_abbrevs = get_common_abbreviations()
            abbreviation_dict = common_abbrevs.copy()
            abbreviation_dict.update(custom_abbreviations)

        # Check max depth - only for nested recursion, not the top level
        if max_depth is not None and current_depth > max_depth:
            # Return an empty result beyond max depth
            return result

        # Track complex objects that need to be removed for in-place processing
        complex_keys_to_remove = []

        # Process each key in the dictionary
        for key in keys_to_process:
            value = data[key]

            # Skip special keys - often used for metadata or internal purposes
            if key.startswith("__"):
                if in_place:
                    result[key] = value
                continue

            # Create sanitized key name while preserving underscores
            # Only replace the separator if it's different from underscores
            if separator != "_":
                sanitized_key = sanitize_name(key, separator, "_")
            else:
                # If separator is already underscore, preserve it
                sanitized_key = sanitize_name(
                    key, separator, "_", preserve_separator=True
                )

            # Calculate new key with parent path
            if parent_path:
                new_key = f"{parent_path}{separator}{sanitized_key}"
            else:
                new_key = sanitized_key

            # Update path components if using optimization
            if path_parts_optimization and path_parts is not None:
                current_path_parts = path_parts + [sanitized_key]
            else:
                current_path_parts = None

            # Handle nested dictionaries
            if isinstance(value, dict):
                # Check if we're at the max depth before recursing further
                if max_depth is not None and current_depth >= max_depth:
                    # At max depth, include the object itself instead of recursing
                    processed_value = _process_value_wrapper(
                        value,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        context=context,
                    )

                    if processed_value is not None:
                        if abbreviate_field_names:
                            abbreviated_key = abbreviate_field_name(
                                new_key,
                                separator=separator,
                                max_component_length=max_field_component_length,
                                preserve_root=preserve_root_component,
                                preserve_leaf=preserve_leaf_component,
                                abbreviation_dict=abbreviation_dict,
                            )
                            result[abbreviated_key] = processed_value
                        else:
                            result[new_key] = processed_value
                elif len(value) == 0:
                    # Special case: empty dictionary - can be handled as a scalar
                    # This is common in JSON APIs where empty objects are placeholders
                    processed_value = _process_value_wrapper(
                        {},
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        context=context,
                    )

                    if processed_value is not None:
                        if abbreviate_field_names:
                            abbreviated_key = abbreviate_field_name(
                                new_key,
                                separator=separator,
                                max_component_length=max_field_component_length,
                                preserve_root=preserve_root_component,
                                preserve_leaf=preserve_leaf_component,
                                abbreviation_dict=abbreviation_dict,
                            )
                            result[abbreviated_key] = processed_value
                        else:
                            result[new_key] = processed_value
                else:
                    # Recursively flatten the nested dictionary
                    nested_result = flatten_func(
                        value,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        skip_arrays=skip_arrays,
                        visit_arrays=visit_arrays,
                        visited=visited,
                        parent_path=new_key,
                        path_parts=current_path_parts,
                        path_parts_optimization=path_parts_optimization,
                        max_depth=max_depth,
                        abbreviate_field_names=abbreviate_field_names,
                        max_field_component_length=max_field_component_length,
                        preserve_root_component=preserve_root_component,
                        preserve_leaf_component=preserve_leaf_component,
                        custom_abbreviations=custom_abbreviations,
                        current_depth=current_depth + 1,
                        in_place=False,  # Never use in_place for nested calls
                        mode=context,
                        recovery_strategy=recovery_strategy,
                    )

                    # Add the flattened results to our result dict
                    for nested_key, nested_value in nested_result.items():
                        result[nested_key] = nested_value

                # For in-place modification, mark the original key for removal
                if in_place:
                    # Only remove complex objects after flattening them
                    complex_keys_to_remove.append(key)

            # Handle arrays
            elif isinstance(value, list):
                # Skip arrays if configured to do so
                if skip_arrays:
                    # Mark array field for removal
                    if in_place:
                        keys_to_remove.append(key)
                    continue

                # Keep arrays as-is if not visiting or if empty
                if not visit_arrays or len(value) == 0:
                    processed_value = _process_value_wrapper(
                        value,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        context=context,
                    )

                    if processed_value is not None:
                        if abbreviate_field_names:
                            abbreviated_key = abbreviate_field_name(
                                new_key,
                                separator=separator,
                                max_component_length=max_field_component_length,
                                preserve_root=preserve_root_component,
                                preserve_leaf=preserve_leaf_component,
                                abbreviation_dict=abbreviation_dict,
                            )
                            result[abbreviated_key] = processed_value
                        else:
                            result[new_key] = processed_value

                    # For in-place modification, mark original key for removal if different
                    if in_place and key != sanitized_key:
                        keys_to_remove.append(key)

                    continue

                # Process array elements if we're visiting arrays
                for i, item in enumerate(value):
                    # Handle dictionary array elements by recursively flattening
                    if isinstance(item, dict):
                        nested_result = flatten_func(
                            item,
                            separator=separator,
                            cast_to_string=cast_to_string,
                            include_empty=include_empty,
                            skip_null=skip_null,
                            skip_arrays=skip_arrays,
                            visit_arrays=visit_arrays,
                            visited=visited,
                            parent_path=f"{new_key}{separator}{i}",
                            path_parts=current_path_parts,
                            path_parts_optimization=path_parts_optimization,
                            max_depth=max_depth,
                            abbreviate_field_names=abbreviate_field_names,
                            max_field_component_length=max_field_component_length,
                            preserve_root_component=preserve_root_component,
                            preserve_leaf_component=preserve_leaf_component,
                            custom_abbreviations=custom_abbreviations,
                            current_depth=current_depth + 1,
                            in_place=False,  # Never use in_place for nested calls
                            mode=context,
                            recovery_strategy=recovery_strategy,
                        )

                        # Add the flattened results to our result dict
                        for nested_key, nested_value in nested_result.items():
                            result[nested_key] = nested_value
                    # Handle non-dictionary array elements directly
                    else:
                        # Create item key with index
                        item_key = f"{new_key}{separator}{i}"

                        # Process the value
                        processed_value = _process_value_wrapper(
                            item,
                            cast_to_string=cast_to_string,
                            include_empty=include_empty,
                            skip_null=skip_null,
                            context=context,
                        )

                        if processed_value is not None:
                            if abbreviate_field_names:
                                # Abbreviate the key
                                abbreviated_key = abbreviate_field_name(
                                    item_key,
                                    separator=separator,
                                    max_component_length=max_field_component_length,
                                    preserve_root=preserve_root_component,
                                    preserve_leaf=preserve_leaf_component,
                                    abbreviation_dict=abbreviation_dict,
                                )
                                result[abbreviated_key] = processed_value
                            else:
                                result[item_key] = processed_value

                # For in-place modification, mark arrays for removal
                if in_place:
                    complex_keys_to_remove.append(key)

            # Handle scalar values
            else:
                # Process the scalar value
                processed_value = _process_value_wrapper(
                    value,
                    cast_to_string=cast_to_string,
                    include_empty=include_empty,
                    skip_null=skip_null,
                    context=context,
                )

                if processed_value is not None:
                    if abbreviate_field_names:
                        # Abbreviate the key
                        abbreviated_key = abbreviate_field_name(
                            new_key,
                            separator=separator,
                            max_component_length=max_field_component_length,
                            preserve_root=preserve_root_component,
                            preserve_leaf=preserve_leaf_component,
                            abbreviation_dict=abbreviation_dict,
                        )
                        result[abbreviated_key] = processed_value
                    else:
                        result[new_key] = processed_value

                # For in-place modification, mark original key for removal if different
                if in_place and key != sanitized_key:
                    keys_to_remove.append(key)

        # Remove keys outside the loop to avoid dictionary size change during iteration
        if in_place:
            # First remove all complex nested objects that have been flattened
            for key in complex_keys_to_remove:
                if key in result:
                    result.pop(key, None)

            # Then remove any other keys that need to be replaced
            for key in keys_to_remove:
                if key in result:
                    result.pop(key, None)

        return result
    finally:
        # Remove from visited set when done to allow reuse of objects in different contexts
        if obj_id in visited:
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
    preserve_root_component: Optional[bool] = None,
    preserve_leaf_component: Optional[bool] = None,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    current_depth: int = 0,
    in_place: bool = False,
    mode: FlattenMode = "standard",
    recovery_strategy: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary into a single-level structure.

    Keys in the flattened dictionary will be created by joining
    the nested path with the specified separator.

    Args:
        data: Nested JSON data to flatten
        separator: Character used to join nested keys
        cast_to_string: Convert all values to strings
        include_empty: Include empty strings in output
        skip_null: Skip null values in output
        skip_arrays: Skip array fields
        visit_arrays: Process arrays into separate fields
        visited: Set of already visited objects to prevent circular refs
        parent_path: Path prefix for flattened keys
        path_parts: Precomputed path parts for efficiency (internal)
        path_parts_optimization: Whether to use path parts optimization
        max_depth: Maximum nesting depth to process
        abbreviate_field_names: Whether to abbreviate field names
        max_field_component_length: Maximum length of field name components
        preserve_root_component: Don't abbreviate root component
        preserve_leaf_component: Don't abbreviate leaf component
        custom_abbreviations: Custom abbreviation dictionary
        current_depth: Current nesting depth (internal)
        in_place: Whether to modify the original object in place
        mode: Processing mode ("standard" for regular processing, "streaming" for memory-efficient)
        recovery_strategy: Recovery strategy for handling circular references

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

    if path_parts_optimization is None:
        path_parts_optimization = settings.get_option("path_parts_optimization")

    if visited is None:
        visited = set()

    if path_parts is None and path_parts_optimization:
        if parent_path:
            path_parts = parent_path.split(separator)
        else:
            path_parts = []

    # Check abbreviation settings
    if abbreviate_field_names is None:
        abbreviate_field_names = settings.get_option("abbreviate_field_names")

    if max_field_component_length is None:
        max_field_component_length = settings.get_option("max_field_component_length")

    if preserve_root_component is None:
        preserve_root_component = settings.get_option("preserve_root_component")

    if preserve_leaf_component is None:
        preserve_leaf_component = settings.get_option("preserve_leaf_component")

    if visit_arrays is None:
        visit_arrays = settings.get_option("visit_arrays")

    # Use a recursive reference to the function itself for nested calls
    flatten_func_ref = flatten_json

    # Choose the processing context
    context = "streaming" if mode == "streaming" else "standard"

    # Clear the process cache at the end of processing in standard mode
    if current_depth == 0 and context == "standard":
        _clear_process_cache(context)

    # Do the actual flattening
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
        preserve_root_component=preserve_root_component,
        preserve_leaf_component=preserve_leaf_component,
        custom_abbreviations=custom_abbreviations,
        current_depth=current_depth,
        in_place=in_place,
        context=context,
        flatten_func=flatten_func_ref,
        recovery_strategy=recovery_strategy,
    )
