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
from ..error import (
    build_error_context,
    error_context,
    format_error_message,
    get_recovery_strategy,
    logger,
)
from ..error.exceptions import ProcessingError
from ..naming.conventions import handle_deeply_nested_path
from ..types import FlattenMode

# Define a return type variable for the decorator's generic type
R = TypeVar("R")


class PathBuilder:
    """Efficient path building with minimal string operations."""

    def __init__(self, separator: str = "_"):
        """Initialize the path builder with a separator."""
        self.separator = separator
        self.parts: list[str] = []

    def append(self, part: str) -> None:
        """Add a part to the path."""
        self.parts.append(part)

    def build(self) -> str:
        """Build the complete path string."""
        return self.separator.join(self.parts)

    def pop(self) -> None:
        """Remove the last part from the path."""
        if self.parts:
            self.parts.pop()

    def copy(self) -> "PathBuilder":
        """Create a copy of this path builder."""
        new_builder = PathBuilder(self.separator)
        new_builder.parts = self.parts.copy()
        return new_builder

    def extend(self, parts: list[str]) -> "PathBuilder":
        """Create a new path builder with additional parts."""
        new_builder = self.copy()
        new_builder.parts.extend(parts)
        return new_builder


class DepthTracker:
    """Efficient depth tracking for nested structures."""

    def __init__(self, threshold: int):
        """Initialize depth tracker with a nesting threshold."""
        self.threshold = threshold
        self.current_depth = 0

    def descend(self) -> bool:
        """Move deeper into nesting and check if threshold exceeded."""
        self.current_depth += 1
        return self.current_depth >= self.threshold

    def ascend(self) -> None:
        """Move up one level in nesting."""
        self.current_depth = max(0, self.current_depth - 1)

    def at_threshold(self) -> bool:
        """Check if currently at the threshold depth."""
        return self.current_depth >= self.threshold


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


class MemoryAwareCache:
    """Memory-aware caching system with adaptive sizing."""

    def __init__(self, max_memory_mb: int = 50, fallback_size: int = 1000):
        """Initialize cache with memory limits and fallback size."""
        self.max_memory_mb = max_memory_mb
        self.fallback_size = fallback_size
        self._cache: dict[Any, Any] = {}
        self._access_order: list[Any] = []

    def get_memory_usage_mb(self) -> float:
        """Estimate current cache memory usage in MB."""
        try:
            import sys

            total_size = 0
            for key, value in self._cache.items():
                total_size += sys.getsizeof(key) + sys.getsizeof(value)
            return total_size / (1024 * 1024)
        except ImportError:
            # Fallback estimation
            return len(self._cache) * 0.005  # Rough estimate of 5KB per entry

    def should_evict(self) -> bool:
        """Check if cache should evict entries due to memory pressure."""
        return self.get_memory_usage_mb() > self.max_memory_mb

    def evict_oldest(self, percentage: float = 0.25) -> None:
        """Evict oldest entries by percentage."""
        if not self._access_order:
            return

        num_to_evict = max(1, int(len(self._access_order) * percentage))
        for _ in range(num_to_evict):
            if self._access_order:
                oldest_key = self._access_order.pop(0)
                self._cache.pop(oldest_key, None)

    def get(self, key: Any, default: Any = None) -> Any:
        """Get value from cache with LRU tracking."""
        if key in self._cache:
            # Move to end (most recently used)
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)
            return self._cache[key]
        return default

    def put(self, key: Any, value: Any) -> None:
        """Put value in cache with memory management."""
        # Check memory pressure before adding
        if self.should_evict():
            self.evict_oldest()

        # Add or update
        if key not in self._cache:
            self._access_order.append(key)
        else:
            # Move to end
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)

        self._cache[key] = value

        # Limit size as fallback
        while len(self._cache) > self.fallback_size * 2:
            self.evict_oldest(0.1)

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
        self._access_order.clear()


# Global memory-aware cache
_memory_cache = MemoryAwareCache()


def get_adaptive_cache_size() -> int:
    """Get adaptive cache size based on memory settings."""
    cache_enabled = getattr(settings, "cache_enabled", True)
    if not cache_enabled:
        return 0

    cache_maxsize = getattr(settings, "cache_maxsize", 10000)

    # Try to get memory information for adaptive sizing
    try:
        import psutil

        memory = psutil.virtual_memory()
        available_mb = memory.available / (1024 * 1024)

        # Use 1% of available memory for caching, max 50MB
        target_memory_mb = min(50, available_mb * 0.01)
        # Estimate ~5KB per cache entry
        estimated_size = int(target_memory_mb * 1024 / 5)

        return min(cache_maxsize, max(100, estimated_size))
    except ImportError:
        # Fallback to configured size
        return cache_maxsize


def _get_lru_cache_decorator(maxsize: Optional[int] = None) -> Callable:
    """Create an LRU cache decorator with adaptive maxsize.

    Args:
        maxsize: Maximum size for the LRU cache (adaptive if None)

    Returns:
        Configured functools.lru_cache decorator
    """
    if maxsize is None:
        maxsize = get_adaptive_cache_size()

    return functools.lru_cache(maxsize=maxsize)


def _get_cached_process_value() -> Callable[
    [int, bool, bool, bool, Any], Optional[Any]
]:
    """Get a cached version of the process_value function.

    Returns:
        Cached function for processing values
    """
    cache_size = get_adaptive_cache_size()
    lru_decorator = _get_lru_cache_decorator(cache_size)

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
    """Refresh the cache configuration based on settings."""
    global _process_value_cached, _memory_cache
    clear_caches()
    _process_value_cached = _get_cached_process_value()
    _memory_cache = MemoryAwareCache()


def clear_caches() -> None:
    """Clear all processing caches."""
    global _memory_cache
    if hasattr(_process_value_cached, "cache_clear"):
        _process_value_cached.cache_clear()
    _memory_cache.clear()


def _process_value_wrapper(
    value: Any,
    cast_to_string: bool,
    include_empty: bool,
    skip_null: bool,
    context: Literal["standard", "streaming"] = "standard",
    recovery_strategy: Optional[Any] = None,
) -> Optional[Any]:
    """Simplified wrapper that handles edge cases and uses LRU cache.

    Args:
        value: The value to process
        cast_to_string: Whether to cast to string
        include_empty: Whether to include empty strings
        skip_null: Whether to skip null values
        context: Cache context
        recovery_strategy: Strategy for error recovery

    Returns:
        Processed value or None if it should be skipped
    """
    # Special values handled directly without caching
    if value is None or value == "":
        return _process_value(value, cast_to_string, include_empty, skip_null)

    # Check if caching is enabled in settings
    cache_enabled = getattr(settings, "cache_enabled", True)

    try:
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
    except Exception as e:
        # Handle errors based on recovery strategy
        strategy = get_recovery_strategy(recovery_strategy)
        error_context = build_error_context(
            operation="value processing", value=repr(value)
        )

        try:
            return strategy.recover(e, **error_context)
        except Exception:
            # If recovery fails, raise with formatted message
            error_msg = format_error_message(
                "type_conversion", e, **error_context, target_type="processed"
            )
            raise ProcessingError(error_msg) from e


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
    nested_threshold: Optional[int],
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
        nested_threshold: Threshold for when to consider a path deeply nested
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

    # Use data in place or create a dictionary
    result = data if in_place else {}

    # Prevent excessive recursion
    if max_depth is not None and current_depth >= max_depth:
        logger.warning(
            f"Maximum nesting depth ({max_depth}) reached at path: {parent_path}"
        )
        return result

    # Initialize efficient path building
    path_builder = None
    depth_tracker = None

    if path_parts_optimization:
        if path_parts is not None:
            path_builder = PathBuilder(separator)
            path_builder.parts = path_parts.copy()
        elif parent_path:
            path_builder = PathBuilder(separator)
            path_builder.parts = parent_path.split(separator)
        else:
            path_builder = PathBuilder(separator)

    if nested_threshold is not None:
        depth_tracker = DepthTracker(nested_threshold)
        depth_tracker.current_depth = current_depth

    # Track keys to remove when not in-place
    keys_to_remove = []

    # Process each key in the data (create list to avoid iteration issues
    # during modification)
    for key, value in list(data.items()):
        # Skip internal metadata fields from processing
        if key.startswith("__"):
            # Copy them to the result if not modifying in place
            if not in_place:
                result[key] = value
            continue

        # Build the path for this field efficiently
        if path_builder is not None:
            # Use PathBuilder for efficient path construction
            current_path_builder = path_builder.copy()
            current_path_builder.append(key)
            current_path = current_path_builder.build()
            current_parts = current_path_builder.parts
        else:
            # Fallback to direct string concatenation
            current_path = f"{parent_path}{separator}{key}" if parent_path else key
            current_parts = None

        # Apply deep nesting simplification using depth tracker
        if depth_tracker is not None and depth_tracker.at_threshold():
            # Provide default threshold if None
            threshold = nested_threshold if nested_threshold is not None else 4
            current_path = handle_deeply_nested_path(current_path, separator, threshold)

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
                # Use current parts for optimization in recursive calls
                nested_path_parts = current_parts if current_parts is not None else None

                # Process the nested dictionary with safe in-place optimization
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
                    nested_threshold=nested_threshold,
                    current_depth=current_depth + 1,
                    in_place=True,  # Safe to use in-place for nested objects
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

                # Mark key for removal if not in-place
                if not in_place:
                    keys_to_remove.append(key)

            except Exception as e:
                # Handle errors using standardized recovery strategy
                strategy = get_recovery_strategy(recovery_strategy)
                error_context = build_error_context(
                    entity_name=key,
                    entity_type="nested object",
                    operation="flattening",
                    source=current_path,
                )

                try:
                    recovery_result = strategy.recover(e, **error_context)
                    if recovery_result is not None:
                        # Add recovery result to output
                        result[f"{current_path}{separator}__error"] = recovery_result
                    # Continue processing other fields
                    continue
                except Exception:
                    # Re-raise with formatted message
                    error_msg = format_error_message("processing", e, **error_context)
                    raise ProcessingError(error_msg) from e
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
                    recovery_strategy=recovery_strategy,
                )
                if processed_value is not None:
                    result[current_path] = processed_value
                    # Mark key for removal if not in-place
                    if not in_place:
                        keys_to_remove.append(key)
            else:
                # Array contains complex objects, stringify it
                processed_value = _process_value_wrapper(
                    value,
                    cast_to_string=True,  # Force string for complex arrays
                    include_empty=include_empty,
                    skip_null=skip_null,
                    context=context,
                    recovery_strategy=recovery_strategy,
                )
                if processed_value is not None:
                    result[current_path] = processed_value
                    # Mark key for removal if not in-place
                    if not in_place:
                        keys_to_remove.append(key)
        else:
            # For scalar values, process and add to the result
            processed_value = _process_value_wrapper(
                value,
                cast_to_string,
                include_empty,
                skip_null,
                context=context,
                recovery_strategy=recovery_strategy,
            )
            if processed_value is not None:
                # For top-level scalar values, use original key name if no parent path
                if parent_path:
                    result[current_path] = processed_value
                else:
                    result[key] = processed_value

    # Remove original keys after processing to avoid modification during iteration
    for key in keys_to_remove:
        result.pop(key, None)

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
    nested_threshold: Optional[int] = None,
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
        nested_threshold: Threshold for when to consider a path deeply nested
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

    if nested_threshold is None:
        nested_threshold = getattr(settings, "nested_threshold", 4)

    # Empty data case
    if data is None:
        return {}

    # Check for non-serializable objects at the top level
    try:
        # Attempt to serialize the data to JSON to check if it's serializable
        import json

        for key, value in data.items():
            try:
                json.dumps(value)
            except (TypeError, ValueError, OverflowError) as serialization_error:
                # Handle non-serializable values using standardized recovery
                strategy = get_recovery_strategy(recovery_strategy)
                error_context = build_error_context(
                    entity_name=key,
                    entity_type="field",
                    operation="serialization check",
                    value=repr(value),
                )

                try:
                    recovery_result = strategy.recover(
                        serialization_error, **error_context
                    )
                    if recovery_result is not None:
                        # Replace with recovery result
                        data[key] = recovery_result
                    # Continue with other fields
                    continue
                except Exception:
                    # Re-raise with formatted message
                    error_msg = format_error_message(
                        "type_conversion",
                        serialization_error,
                        **error_context,
                        target_type="serializable",
                    )
                    raise ProcessingError(error_msg) from serialization_error
    except Exception as e:
        if not isinstance(e, ProcessingError):
            raise ProcessingError(f"Failed to process data: {e}") from e
        raise

    # Initialize path_parts for optimization
    if path_parts_optimization and path_parts is None:
        path_parts = parent_path.split(separator) if parent_path else []

    # Choose recursive function based on mode
    if mode == "streaming":
        # For streaming mode, always use in_place = False
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
            nested_threshold=nested_threshold,
            current_depth=current_depth,
            in_place=False,  # Never modify in place for streaming
            context=context,
            flatten_func=flatten_json,
            recovery_strategy=recovery_strategy,
        )

    # Standard mode - only use in_place for nested calls, not top level
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
        nested_threshold=nested_threshold,
        current_depth=current_depth,
        in_place=False,  # Never modify original data at top level
        context=context,
        flatten_func=flatten_json,
        recovery_strategy=recovery_strategy,
    )
