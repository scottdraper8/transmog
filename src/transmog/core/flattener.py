"""Flatten nested JSON data into a tabular structure.

This module provides key functions for flattening JSON data,
including handling of nested objects and arrays with customizable naming.
"""

import functools
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
    TypeVar,
    cast,
)

from ..config import settings
from ..error import error_context, logger
from ..error.exceptions import ProcessingError
from ..naming.abbreviator import abbreviate_field_name
from ..naming.conventions import sanitize_name
from ..types import FlattenMode

# Define a return type variable for the decorator's generic type
R = TypeVar("R")


def _process_value(
    value: Any, cast_to_string: bool, include_empty: bool, skip_null: bool
) -> Optional[Any]:
    """Process a value according to configuration settings.

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


def _get_lru_cache_decorator(maxsize: int = 10000) -> Callable:
    """Create an LRU cache decorator with the specified maxsize.

    Args:
        maxsize: Maximum size for the LRU cache

    Returns:
        Configured functools.lru_cache decorator
    """
    return functools.lru_cache(maxsize=maxsize)


def _get_cached_process_value() -> Callable[
    [int, bool, bool, bool, Any], Optional[Any]
]:
    """Get a cached version of the process_value function.

    Returns:
        Cached function for processing values
    """
    lru_decorator = _get_lru_cache_decorator()

    @lru_decorator
    def _cached_func(
        value_hash: int,
        cast_to_string: bool,
        include_empty: bool,
        skip_null: bool,
        original_value: Any,
    ) -> Optional[Any]:
        # Process original value, not the hash
        return _process_value(original_value, cast_to_string, include_empty, skip_null)

    return cast(Callable[[int, bool, bool, bool, Any], Optional[Any]], _cached_func)


# Global cached function
_process_value_cached = _get_cached_process_value()


def refresh_cache_config() -> None:
    """Refresh the cache configuration based on current settings."""
    global _process_value_cached
    clear_caches()
    _process_value_cached = _get_cached_process_value()


def clear_caches() -> None:
    """Clear all processing caches."""
    if hasattr(_process_value_cached, "cache_clear"):
        _process_value_cached.cache_clear()


def _process_value_wrapper(
    value: Any,
    cast_to_string: bool,
    include_empty: bool,
    skip_null: bool,
    context: Literal["standard", "streaming"] = "standard",
) -> Optional[Any]:
    """Simplified wrapper that handles edge cases and uses LRU cache.

    Args:
        value: The value to process
        cast_to_string: Whether to cast to string
        include_empty: Whether to include empty strings
        skip_null: Whether to skip null values
        context: Cache context

    Returns:
        Processed value or None if it should be skipped
    """
    # Special values handled directly without caching
    if value is None or value == "":
        return _process_value(value, cast_to_string, include_empty, skip_null)

    # Check if caching is enabled in settings
    cache_enabled = getattr(settings, "cache_enabled", True)

    # Cache simple scalar values
    if cache_enabled and isinstance(value, (int, float, bool, str)):
        try:
            value_hash = hash(value)
            return _process_value_cached(
                value_hash, cast_to_string, include_empty, skip_null, value
            )
        except (TypeError, ValueError):
            # Fall back to direct processing for unhashable values
            return _process_value(value, cast_to_string, include_empty, skip_null)

    # Process complex objects directly
    return _process_value(value, cast_to_string, include_empty, skip_null)


def _flatten_json_core(
    data: dict[str, Any],
    separator: str,
    cast_to_string: bool,
    include_empty: bool,
    skip_null: bool,
    skip_arrays: bool,
    visit_arrays: bool,
    parent_path: str,
    path_parts: Optional[list[str]],
    path_parts_optimization: bool,
    max_depth: Optional[int],
    abbreviate_field_names: bool,
    max_field_component_length: Optional[int],
    preserve_root_component: bool,
    preserve_leaf_component: bool,
    custom_abbreviations: Optional[dict[str, str]],
    current_depth: int,
    in_place: bool,
    context: Literal["standard", "streaming"],
    flatten_func: Callable,
    recovery_strategy: Optional[Any] = None,
) -> dict[str, Any]:
    """Core implementation of flatten_json with all parameters.

    Args:
        data: JSON data to flatten
        separator: Path separator
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        skip_arrays: Whether to skip arrays
        visit_arrays: Whether to visit arrays
        parent_path: Current path from root
        path_parts: Components of the path for optimization
        path_parts_optimization: Whether to use path parts optimization
        max_depth: Maximum nesting depth
        abbreviate_field_names: Whether to abbreviate field names
        max_field_component_length: Maximum length for field name components
        preserve_root_component: Whether to preserve root component
        preserve_leaf_component: Whether to preserve leaf component
        custom_abbreviations: Custom abbreviation dictionary
        current_depth: Current nesting depth
        in_place: Whether to modify data in place
        context: Processing context
        flatten_func: Function to use for recursion
        recovery_strategy: Strategy for error recovery

    Returns:
        Flattened JSON data
    """
    if not isinstance(data, dict):
        raise TypeError(f"Expected dictionary, got {type(data).__name__}")

    # Apply max depth limitation
    if max_depth is None:
        max_depth = 100  # Default maximum depth

    # Stop recursion if max depth is reached
    if max_depth is not None and current_depth >= max_depth:
        logger.warning(
            f"Maximum recursion depth ({max_depth}) reached at path: {parent_path}"
        )
        return {}

    try:
        # Initialize result based on in-place flag
        if in_place and isinstance(data, dict):
            result = data
            keys_to_process = list(data.keys())
            keys_to_remove: list[str] = []
        else:
            result = {}
            keys_to_process = list(data.keys())
            keys_to_remove = []

        # Prepare abbreviation dictionary
        abbreviation_dict = None
        if abbreviate_field_names and custom_abbreviations:
            abbreviation_dict = custom_abbreviations.copy()

        # Track complex objects for in-place processing
        complex_keys_to_remove = []

        # Process each key in the dictionary
        for key in keys_to_process:
            value = data[key]

            # Skip special keys (metadata or internal)
            if key.startswith("__"):
                if in_place:
                    result[key] = value
                continue

            # Create sanitized key name
            if separator != "_":
                sanitized_key = sanitize_name(key, separator, "_")
            else:
                sanitized_key = sanitize_name(
                    key, separator, "_", preserve_separator=True
                )

            # Calculate new key with parent path
            if parent_path:
                new_key = f"{parent_path}{separator}{sanitized_key}"
            else:
                new_key = sanitized_key

            # Update path components for optimization
            if path_parts_optimization and path_parts is not None:
                current_path_parts = path_parts + [sanitized_key]
            else:
                current_path_parts = None

            # Handle nested dictionaries
            if isinstance(value, dict):
                # At max depth, treat as scalar
                if max_depth is not None and current_depth >= max_depth:
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
                    # Empty dictionary - treat as scalar
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
                    # Recursively flatten nested dictionary
                    nested_result = flatten_func(
                        value,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        skip_arrays=skip_arrays,
                        visit_arrays=visit_arrays,
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

                    # Add flattened results to result dict
                    for nested_key, nested_value in nested_result.items():
                        result[nested_key] = nested_value

                # Mark for removal if in-place
                if in_place:
                    complex_keys_to_remove.append(key)

            # Handle arrays
            elif isinstance(value, list):
                if visit_arrays and len(value) > 0:
                    # Process each item in the array
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            # Flatten dictionaries within array
                            item_key = f"{new_key}{separator}{i}"
                            item_path_parts = (
                                current_path_parts + [str(i)]
                                if current_path_parts
                                else None
                            )

                            # Flatten array item
                            flattened_item = flatten_func(
                                item,
                                separator=separator,
                                cast_to_string=cast_to_string,
                                include_empty=include_empty,
                                skip_null=skip_null,
                                skip_arrays=skip_arrays,
                                visit_arrays=visit_arrays,
                                parent_path=item_key,
                                path_parts=item_path_parts,
                                path_parts_optimization=path_parts_optimization,
                                max_depth=max_depth,
                                abbreviate_field_names=abbreviate_field_names,
                                max_field_component_length=max_field_component_length,
                                preserve_root_component=preserve_root_component,
                                preserve_leaf_component=preserve_leaf_component,
                                custom_abbreviations=custom_abbreviations,
                                current_depth=current_depth + 1,
                                in_place=False,
                                mode=context,
                                recovery_strategy=recovery_strategy,
                            )

                            # Add index to each key
                            for flat_key, flat_value in flattened_item.items():
                                base_key = flat_key.replace(item_key, new_key)
                                indexed_key = f"{base_key}{separator}idx{i}"
                                result[indexed_key] = flat_value

                        else:
                            # Add primitive array values with index
                            processed_value = _process_value_wrapper(
                                item,
                                cast_to_string=cast_to_string,
                                include_empty=include_empty,
                                skip_null=skip_null,
                                context=context,
                            )

                            if processed_value is not None:
                                item_key = f"{new_key}{separator}idx{i}"
                                if abbreviate_field_names:
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

                elif not skip_arrays:
                    # Keep raw array if not skipping
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

                # Mark for removal if in-place
                if in_place:
                    complex_keys_to_remove.append(key)

            else:
                # Process primitive values
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

                    # Mark for removal if key changed and in-place
                    if in_place and key != new_key:
                        keys_to_remove.append(key)

        # Remove processed keys for in-place modification
        if in_place:
            for key in complex_keys_to_remove:
                if key in data:
                    del data[key]

            for key in keys_to_remove:
                if key in data:
                    del data[key]

        return result

    except Exception as e:
        # Attempt recovery if strategy provided
        if recovery_strategy and hasattr(recovery_strategy, "recover"):
            try:
                # Get path for error context
                current_path = []
                if path_parts:
                    current_path = list(path_parts)
                elif parent_path:
                    current_path = parent_path.split(separator)

                recovery_result = recovery_strategy.recover(
                    e, path=current_path, entity_name=None
                )
                return {} if recovery_result is None else recovery_result
            except Exception as re:
                logger.warning(f"Recovery failed: {re}")
                raise e from None
        raise


@error_context("Failed to flatten JSON", wrap_as=lambda e: ProcessingError(str(e)))
def flatten_json(
    data: dict[str, Any],
    separator: Optional[str] = None,
    cast_to_string: Optional[bool] = None,
    include_empty: Optional[bool] = None,
    skip_null: Optional[bool] = None,
    skip_arrays: bool = True,
    visit_arrays: Optional[bool] = None,
    parent_path: str = "",
    path_parts: Optional[list[str]] = None,
    path_parts_optimization: Optional[bool] = None,
    max_depth: Optional[int] = None,
    abbreviate_field_names: Optional[bool] = None,
    max_field_component_length: Optional[int] = None,
    preserve_root_component: Optional[bool] = None,
    preserve_leaf_component: Optional[bool] = None,
    custom_abbreviations: Optional[dict[str, str]] = None,
    current_depth: int = 0,
    in_place: bool = False,
    mode: FlattenMode = "standard",
    recovery_strategy: Optional[Any] = None,
) -> dict[str, Any]:
    """Flatten a nested JSON structure.

    This is the main entry point for flattening a nested JSON structure
    into a single-level dictionary with path-based keys.

    Args:
        data: JSON data to flatten
        separator: Separator for path components (default: "_")
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        skip_arrays: Whether to skip arrays
        visit_arrays: Whether to visit arrays
        parent_path: Current path from root
        path_parts: Components of the path for optimization
        path_parts_optimization: Whether to use path parts optimization
        max_depth: Maximum nesting depth
        abbreviate_field_names: Whether to abbreviate field names
        max_field_component_length: Maximum length for field name components
        preserve_root_component: Whether to preserve root component
        preserve_leaf_component: Whether to preserve leaf component
        custom_abbreviations: Custom abbreviation dictionary
        current_depth: Current nesting depth
        in_place: Whether to modify data in place
        mode: Processing mode (standard or streaming)
        recovery_strategy: Strategy for error recovery

    Returns:
        Flattened JSON data
    """
    # Load defaults from settings
    if separator is None:
        separator = settings.get("separator", "_")

    if cast_to_string is None:
        cast_to_string = settings.get("cast_to_string", True)

    if include_empty is None:
        include_empty = settings.get("include_empty", False)

    if skip_null is None:
        skip_null = settings.get("skip_null", True)

    if visit_arrays is None:
        visit_arrays = settings.get("visit_arrays", False)

    if abbreviate_field_names is None:
        abbreviate_field_names = settings.get("abbreviate_field_names", True)

    if max_field_component_length is None:
        max_field_component_length = settings.get("max_field_component_length", None)

    if preserve_root_component is None:
        preserve_root_component = settings.get("preserve_root_component", True)

    if preserve_leaf_component is None:
        preserve_leaf_component = settings.get("preserve_leaf_component", True)

    if path_parts_optimization is None:
        path_parts_optimization = settings.get("path_parts_optimization", True)

    if max_depth is None:
        max_depth = settings.get("max_depth", 100)

    # Optimize path parts handling
    if path_parts is None and path_parts_optimization:
        path_parts = parent_path.split(separator) if parent_path else []

    # Execute core flattening logic
    return _flatten_json_core(
        data=data,
        separator=separator,
        cast_to_string=cast_to_string,
        include_empty=include_empty,
        skip_null=skip_null,
        skip_arrays=skip_arrays,
        visit_arrays=visit_arrays,
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
        context=mode,
        flatten_func=flatten_json,
        recovery_strategy=recovery_strategy,
    )
