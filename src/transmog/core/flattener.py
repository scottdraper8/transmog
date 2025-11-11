"""Flatten nested JSON data into a tabular structure."""

from typing import Any, Optional

from transmog.config import TransmogConfig
from transmog.error import (
    logger,
)
from transmog.types import ArrayMode, NullHandling, ProcessingContext


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
    if value is None or value == "":
        if config.null_handling == NullHandling.SKIP:
            return None
        return ""

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


def _flatten_json_core(
    data: dict[str, Any],
    config: TransmogConfig,
    context: ProcessingContext,
) -> dict[str, Any]:
    """Core implementation of flatten_json.

    Args:
        data: JSON data to flatten
        config: Configuration settings
        context: Processing context with runtime state

    Returns:
        Flattened JSON data
    """
    if data is None:
        return {}

    result: dict[str, Any] = {}

    if context.current_depth >= config.max_depth:
        path = context.build_path(config.separator)
        logger.warning(f"Maximum depth ({config.max_depth}) reached at path: {path}")
        return result

    for key, value in data.items():
        if len(key) >= 2 and key[0] == "_" and key[1] == "_":
            result[key] = value
            continue

        nested_context = context.descend(key)
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
            )

            for flattened_key, flattened_value in flattened.items():
                result[flattened_key] = flattened_value

        elif is_list:
            if config.array_mode == ArrayMode.SKIP:
                continue
            elif config.array_mode == ArrayMode.SMART:
                if _is_simple_array(value):
                    result[current_path] = value
            elif config.array_mode == ArrayMode.INLINE:
                processed_value = _process_value(value, config)
                if processed_value is not None:
                    result[current_path] = processed_value

        else:
            processed_value = _process_value(value, config)
            if processed_value is not None:
                if context.path_components:
                    result[current_path] = processed_value
                else:
                    result[key] = processed_value

    return result


def flatten_json(
    data: dict[str, Any],
    config: TransmogConfig,
    context: Optional[ProcessingContext] = None,
) -> dict[str, Any]:
    """Flatten nested JSON structure into a flat dictionary."""
    if data is None:
        return {}

    if context is None:
        context = ProcessingContext()

    return _flatten_json_core(
        data,
        config,
        context,
    )
