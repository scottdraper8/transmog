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
from ..naming.conventions import handle_deeply_nested_path
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
    deeply_nested_threshold: Optional[int],
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
        skip_arrays: Arrays are not skipped by default
        visit_arrays: Whether to visit arrays
        parent_path: Current path from root
        path_parts: Components of the path for optimization
        path_parts_optimization: Whether to use path parts optimization
        max_depth: Maximum nesting depth
        deeply_nested_threshold: Threshold for when to consider a path deeply nested
        current_depth: Current nesting depth
        in_place: Whether to modify data in place
        context: Processing context
        flatten_func: Function to use for recursion
        recovery_strategy: Strategy for error recovery

    Returns:
        Flattened JSON data
    """
    # Return early if no data to process
    if data is None:
        return {}

    # Use data in place or create a new dictionary
    result = data if in_place else {}

    # Prevent excessive recursion
    if max_depth is not None and current_depth >= max_depth:
        logger.warning(
            f"Maximum nesting depth ({max_depth}) reached at path: {parent_path}"
        )
        return result

    # Initialize path parts if optimization is enabled
    if path_parts_optimization and path_parts is None:
        path_parts = parent_path.split(separator) if parent_path else []

    # Process each key in the data
    for key, value in list(data.items()):
        # Skip internal metadata fields from processing
        if key.startswith("__"):
            # Copy them to the result if not modifying in place
            if not in_place:
                result[key] = value
            continue

        # Build the current path for this field
        if path_parts_optimization and path_parts is not None:
            # Efficient path building without string concatenation
            current_parts = path_parts + [key]
            current_path = separator.join(current_parts)
        else:
            # Direct string concatenation for path building
            current_path = f"{parent_path}{separator}{key}" if parent_path else key

        # Apply deep nesting simplification if threshold is provided
        if (
            deeply_nested_threshold is not None
            and current_path.count(separator) >= deeply_nested_threshold
        ):
            current_path = handle_deeply_nested_path(
                current_path, separator, deeply_nested_threshold
            )

        # Skip empty dictionaries and arrays
        if (isinstance(value, dict) and not value) or (
            isinstance(value, list) and not value
        ):
            # Skip empty objects and arrays
            continue

        # Process value based on its type
        if isinstance(value, dict):
            # Recursively flatten nested dictionary
            try:
                # Use path parts for optimization in recursive calls if enabled
                nested_path_parts = current_parts if path_parts_optimization else None

                # Process the nested dictionary
                flattened = flatten_func(
                    value,
                    separator=separator,
                    cast_to_string=cast_to_string,
                    include_empty=include_empty,
                    skip_null=skip_null,
                    skip_arrays=skip_arrays,
                    visit_arrays=visit_arrays,
                    parent_path=current_path,
                    path_parts=nested_path_parts,
                    path_parts_optimization=path_parts_optimization,
                    max_depth=max_depth,
                    deeply_nested_threshold=deeply_nested_threshold,
                    current_depth=current_depth + 1,
                    in_place=False,  # Always create new dict for nested objects
                    context=context,
                    recovery_strategy=recovery_strategy,
                )

                # Add flattened dictionary to result
                for flattened_key, flattened_value in flattened.items():
                    if flattened_key.startswith("__"):
                        # Don't prefix internal fields
                        result[flattened_key] = flattened_value
                    else:
                        # Regular field, apply path prefix
                        result[flattened_key] = flattened_value

                # Remove the original key to avoid duplication, but only if not in_place
                if key in result and not in_place:
                    result.pop(key, None)

            except Exception as e:
                if recovery_strategy == "skip":
                    # Skip the problematic nested object
                    continue
                elif recovery_strategy == "partial":
                    # Keep partial results and add error indicator
                    result[f"{current_path}{separator}__error"] = str(e)
                    continue
                else:
                    # Re-raise the exception
                    raise
        elif isinstance(value, list) and visit_arrays:
            # Process array based on configuration
            if skip_arrays:
                # Skip arrays when configured to do so
                continue

            # For array values, either stringify them or process them
            if all(not isinstance(item, (dict, list)) for item in value):
                # Handle primitive arrays (no objects/arrays inside)
                processed_value = _process_value_wrapper(
                    value,
                    cast_to_string,
                    include_empty,
                    skip_null,
                    context=context,
                )
                if processed_value is not None:
                    result[current_path] = processed_value
                    # Remove original array field, but only if not in_place
                    if key in result and not in_place:
                        result.pop(key, None)
            else:
                # Array contains complex objects, stringify it
                processed_value = _process_value_wrapper(
                    value,
                    cast_to_string=True,  # Force string for complex arrays
                    include_empty=include_empty,
                    skip_null=skip_null,
                    context=context,
                )
                if processed_value is not None:
                    result[current_path] = processed_value
                    # Remove original array field, but only if not in_place
                    if key in result and not in_place:
                        result.pop(key, None)
        else:
            # For scalar values, process and add to the result
            processed_value = _process_value_wrapper(
                value, cast_to_string, include_empty, skip_null, context=context
            )
            if processed_value is not None:
                result[current_path] = processed_value

    # Return the flattened result
    return result


@error_context("Failed to flatten JSON", wrap_as=lambda e: ProcessingError(str(e)))  # type: ignore
def flatten_json(
    data: dict[str, Any],
    separator: Optional[str] = None,
    cast_to_string: Optional[bool] = None,
    include_empty: Optional[bool] = None,
    skip_null: Optional[bool] = None,
    skip_arrays: bool = False,
    visit_arrays: Optional[bool] = None,
    parent_path: str = "",
    path_parts: Optional[list[str]] = None,
    path_parts_optimization: Optional[bool] = None,
    max_depth: Optional[int] = None,
    deeply_nested_threshold: Optional[int] = None,
    current_depth: int = 0,
    in_place: bool = False,
    mode: FlattenMode = "standard",
    context: Literal["standard", "streaming"] = "standard",
    recovery_strategy: Optional[Any] = None,
) -> dict[str, Any]:
    """Flatten a nested JSON structure into a flat dictionary.

    Args:
        data: JSON data to flatten
        separator: Path separator character
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty string values
        skip_null: Whether to skip null values
        skip_arrays: Whether to skip arrays
        visit_arrays: Whether to visit array elements
        parent_path: Current path from root
        path_parts: Components of the path for optimization
        path_parts_optimization: Whether to use path parts optimization
        max_depth: Maximum nesting depth
        deeply_nested_threshold: Threshold for when to consider a path deeply nested
        current_depth: Current nesting depth
        in_place: Whether to modify data in place
        mode: Processing mode ("standard" or "streaming")
        context: Context type for internal use
        recovery_strategy: Strategy for error recovery

    Returns:
        Flattened JSON data

    Notes:
        - Empty objects ({}) and empty arrays ([]) are skipped
        - Original nested structures are removed after being flattened
        - Deeply nested paths are simplified when they exceed the threshold
    """
    # Use default settings if parameters are not provided
    if separator is None:
        separator = getattr(settings, "separator", "_")

    if cast_to_string is None:
        cast_to_string = getattr(settings, "cast_to_string", True)

    if include_empty is None:
        include_empty = getattr(settings, "include_empty", False)

    if skip_null is None:
        skip_null = getattr(settings, "skip_null", True)

    if visit_arrays is None:
        visit_arrays = getattr(settings, "visit_arrays", True)

    if path_parts_optimization is None:
        path_parts_optimization = getattr(settings, "path_parts_optimization", True)

    if max_depth is None:
        max_depth = getattr(settings, "max_depth", 100)

    if deeply_nested_threshold is None:
        deeply_nested_threshold = getattr(settings, "deeply_nested_threshold", 4)

    # Empty data case
    if data is None:
        return {}

    # Initialize path_parts for optimization
    if path_parts_optimization and path_parts is None:
        path_parts = parent_path.split(separator) if parent_path else []

    # Choose recursive function based on mode
    if mode == "streaming":
        # For streaming mode, always use in-place = False
        return _flatten_json_core(
            data,
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
            deeply_nested_threshold=deeply_nested_threshold,
            current_depth=current_depth,
            in_place=False,  # Never modify in place for streaming
            context=context,
            flatten_func=flatten_json,
            recovery_strategy=recovery_strategy,
        )

    # Standard mode
    return _flatten_json_core(
        data,
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
        deeply_nested_threshold=deeply_nested_threshold,
        current_depth=current_depth,
        in_place=in_place,
        context=context,
        flatten_func=flatten_json,
        recovery_strategy=recovery_strategy,
    )
