"""Flatten nested JSON data into a tabular structure.

This module provides functions for flattening JSON data with customizable naming.
"""

import functools
from typing import Any, Callable, Optional

from transmog.config import TransmogConfig
from transmog.error import (
    logger,
)
from transmog.types import ArrayMode, ProcessingContext
from transmog.types.processing_types import FlattenMode


def _is_simple_array(array: list) -> bool:
    """Check if array contains only primitive values.

    Args:
        array: The array to check

    Returns:
        True if array contains only primitives (str, int, float, bool, None)
    """
    if not array:
        return True

    for item in array:
        if isinstance(item, (dict, list, tuple)):
            return False
    return True


def _process_value(
    value: Any,
    config: TransmogConfig,
) -> Optional[Any]:
    """Process value according to configuration.

    Args:
        value: The value to process
        config: Configuration settings

    Returns:
        Processed value or None if it should be skipped
    """
    if value is None:
        if config.skip_null:
            return None
        return ""

    if value == "" and not config.include_empty:
        return None

    if isinstance(value, float) and (
        value != value or value == float("inf") or value == float("-inf")
    ):
        return "_error_invalid_float" if config.cast_to_string else value

    if config.cast_to_string:
        if isinstance(value, bool):
            return "true" if value else "false"
        elif not isinstance(value, str):
            return str(value)

    return value


_value_cache: dict[tuple[int, bool, bool, bool], Any] = {}


def _get_cached_process_value() -> Callable:
    """Get cached version of process_value function."""

    @functools.lru_cache(maxsize=10000)
    def _cached_func(
        value_hash: int,
        cast_to_string: bool,
        include_empty: bool,
        skip_null: bool,
        original_value: Any,
    ) -> Optional[Any]:
        from transmog.config import TransmogConfig

        temp_config = TransmogConfig(
            cast_to_string=cast_to_string,
            include_empty=include_empty,
            skip_null=skip_null,
        )
        return _process_value(original_value, temp_config)

    return _cached_func


_process_value_cached = _get_cached_process_value()


def clear_caches() -> None:
    """Clear all processing caches."""
    if hasattr(_process_value_cached, "cache_clear"):
        _process_value_cached.cache_clear()


def refresh_cache_config() -> None:
    """Refresh cache configuration."""
    global _process_value_cached
    clear_caches()
    _process_value_cached = _get_cached_process_value()


def _process_value_wrapper(
    value: Any,
    config: TransmogConfig,
) -> Optional[Any]:
    """Wrapper that handles caching for value processing.

    Args:
        value: The value to process
        config: Configuration settings

    Returns:
        Processed value or None if it should be skipped
    """
    if value is None or value == "":
        return _process_value(value, config)

    if config.cache_size > 0 and isinstance(value, (int, float, bool, str)):
        try:
            value_hash = hash(value)
            return _process_value_cached(
                value_hash,
                config.cast_to_string,
                config.include_empty,
                config.skip_null,
                value,
            )
        except (TypeError, ValueError):
            pass

    return _process_value(value, config)


def _flatten_json_core(
    data: dict[str, Any],
    config: TransmogConfig,
    context: ProcessingContext,
    in_place: bool,
    mode: FlattenMode,
) -> dict[str, Any]:
    """Core implementation of flatten_json.

    Args:
        data: JSON data to flatten
        config: Configuration settings
        context: Processing context with runtime state
        in_place: Whether to modify data in place
        mode: Processing mode (standard or streaming)

    Returns:
        Flattened JSON data
    """
    if data is None:
        return {}

    result = data if in_place else {}

    if context.current_depth >= config.max_depth:
        path = context.build_path(config.separator)
        logger.warning(f"Maximum depth ({config.max_depth}) reached at path: {path}")
        return result

    keys_to_remove = []
    items = tuple(data.items()) if in_place else data.items()

    for key, value in items:
        if len(key) >= 2 and key[0] == "_" and key[1] == "_":
            if not in_place:
                result[key] = value
            continue

        nested_context = context.descend(key, config.nested_threshold)
        current_path = nested_context.build_path(config.separator)

        is_dict = isinstance(value, dict)
        is_list = isinstance(value, list)

        if (is_dict or is_list) and not value:
            continue

        if is_dict:
            flattened = _flatten_json_core(
                value,
                config,
                nested_context,
                in_place=False,
                mode=mode,
            )

            for flattened_key, flattened_value in flattened.items():
                result[flattened_key] = flattened_value

            if not in_place:
                keys_to_remove.append(key)

        elif is_list:
            if config.array_mode == ArrayMode.SKIP:
                continue
            elif config.array_mode == ArrayMode.SMART:
                if _is_simple_array(value):
                    result[current_path] = value
                else:
                    if not in_place:
                        keys_to_remove.append(key)
            elif config.array_mode == ArrayMode.INLINE:
                processed_value = _process_value_wrapper(value, config)
                if processed_value is not None:
                    result[current_path] = processed_value
            elif config.array_mode == ArrayMode.SEPARATE:
                if not in_place:
                    keys_to_remove.append(key)

        else:
            processed_value = _process_value_wrapper(value, config)
            if processed_value is not None:
                if context.path_components:
                    result[current_path] = processed_value
                else:
                    result[key] = processed_value

    for key in keys_to_remove:
        result.pop(key, None)

    return result


def flatten_json(
    data: dict[str, Any],
    config: TransmogConfig,
    context: Optional[ProcessingContext] = None,
) -> dict[str, Any]:
    """Flatten nested JSON structure into flat dictionary.

    Args:
        data: JSON data to flatten
        config: Configuration settings
        context: Optional processing context (created if not provided)

    Returns:
        Flattened JSON data
    """
    if data is None:
        return {}

    if context is None:
        context = ProcessingContext()

    return _flatten_json_core(
        data,
        config,
        context,
        in_place=False,
        mode="standard",
    )
