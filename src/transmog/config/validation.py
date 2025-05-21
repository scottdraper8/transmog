"""Configuration validation.

This module provides validation utilities for TransmogConfig to catch
configuration errors early.
"""

import re
from typing import Any, Optional, Union

from transmog.error import ConfigurationError


def validate_separator(separator: str) -> None:
    """Validate the separator character.

    Args:
        separator: Separator character to validate

    Raises:
        ConfigurationError: If the separator is invalid
    """
    if not separator:
        raise ConfigurationError("Separator cannot be empty")

    if len(separator) > 2:
        raise ConfigurationError(
            f"Separator '{separator}' is too long. Must be 1-2 characters."
        )

    # Check for invalid characters in separator
    invalid_chars = r"[\\\/\"\'\[\]\{\}\(\)\s]"
    if re.search(invalid_chars, separator):
        raise ConfigurationError(
            f"Separator '{separator}' contains invalid characters. "
            f"Cannot use spaces or special characters: \\ / \" ' [ ] {{ }} ( )"
        )


def validate_field_name(field_name: str) -> None:
    """Validate a field name.

    Args:
        field_name: Field name to validate

    Raises:
        ConfigurationError: If the field name is invalid
    """
    if not field_name:
        raise ConfigurationError("Field name cannot be empty")

    if field_name.startswith("__") and not (
        field_name.startswith("__extract_") or field_name.startswith("__custom_")
    ):
        raise ConfigurationError(
            f"Field name '{field_name}' cannot start with '__' unless it's a "
            f"recognized metadata field (e.g., __extract_* or __custom_*)"
        )


def validate_recovery_strategy(strategy: str) -> None:
    """Validate the recovery strategy.

    Args:
        strategy: Recovery strategy to validate

    Raises:
        ConfigurationError: If the strategy is invalid
    """
    valid_strategies = ["strict", "skip", "partial"]
    if strategy not in valid_strategies:
        raise ConfigurationError(
            f"Invalid recovery strategy: '{strategy}'. "
            f"Must be one of: {', '.join(valid_strategies)}"
        )


def validate_batch_size(batch_size: int) -> None:
    """Validate the batch size.

    Args:
        batch_size: Batch size to validate

    Raises:
        ConfigurationError: If the batch size is invalid
    """
    if batch_size <= 0:
        raise ConfigurationError(f"Batch size must be positive, got {batch_size}")

    if batch_size > 100000:
        # This is a warning rather than an error
        import warnings

        warnings.warn(
            f"Very large batch size ({batch_size}) may cause memory issues. "
            f"Consider using a smaller value (1000-10000) for better performance.",
            stacklevel=2,
        )


def validate_max_depth(max_depth: int) -> None:
    """Validate the maximum recursion depth.

    Args:
        max_depth: Maximum depth to validate

    Raises:
        ConfigurationError: If the max depth is invalid
    """
    if max_depth <= 0:
        raise ConfigurationError(
            f"Maximum recursion depth must be positive, got {max_depth}"
        )

    if max_depth > 1000:
        # This is a warning rather than an error
        import warnings

        warnings.warn(
            f"Very high maximum recursion depth ({max_depth}) may cause "
            f"stack overflow or performance issues.",
            stacklevel=2,
        )


def validate_cache_size(maxsize: int) -> None:
    """Validate the cache size.

    Args:
        maxsize: Cache size to validate

    Raises:
        ConfigurationError: If the cache size is invalid
    """
    if maxsize < 0:
        raise ConfigurationError(f"Cache size must be non-negative, got {maxsize}")


def validate_id_field_mapping(mapping: Union[str, dict[str, str]]) -> None:
    """Validate an ID field mapping.

    Args:
        mapping: ID field mapping to validate

    Raises:
        ConfigurationError: If the mapping is invalid
    """
    if isinstance(mapping, str):
        validate_field_name(mapping)
    elif isinstance(mapping, dict):
        for path, field in mapping.items():
            if not path:
                raise ConfigurationError("Path key cannot be empty in ID field mapping")
            validate_field_name(field)
    else:
        raise ConfigurationError(
            f"ID field mapping must be a string or dictionary, "
            f"got {type(mapping).__name__}"
        )


def validate_component_length(length: Optional[int]) -> None:
    """Validate a component length.

    Args:
        length: Component length to validate

    Raises:
        ConfigurationError: If the length is invalid
    """
    if length is not None and length <= 0:
        raise ConfigurationError(f"Component length must be positive, got {length}")


def validate_config(config_dict: dict[str, Any]) -> None:
    """Validate a complete configuration dictionary.

    This function validates the entire configuration at once,
    checking for inconsistencies or invalid combinations.

    Args:
        config_dict: Configuration dictionary to validate

    Raises:
        ConfigurationError: If the configuration is invalid
    """
    # Extract key configuration items
    separator = config_dict.get("separator", "_")
    batch_size = config_dict.get("batch_size", 1000)
    max_depth = config_dict.get("max_depth", 100)
    id_field = config_dict.get("id_field", "__extract_id")
    parent_field = config_dict.get("parent_field", "__parent_extract_id")
    time_field = config_dict.get("time_field", "__extract_datetime")
    recovery_strategy = config_dict.get("recovery_strategy", "strict")
    max_table_component_length = config_dict.get("max_table_component_length")
    max_field_component_length = config_dict.get("max_field_component_length")
    cache_maxsize = config_dict.get("cache_maxsize", 10000)
    default_id_field = config_dict.get("default_id_field")

    # Validate individual parameters
    validate_separator(separator)
    validate_batch_size(batch_size)
    validate_max_depth(max_depth)
    validate_field_name(id_field)
    validate_field_name(parent_field)
    validate_field_name(time_field)
    validate_recovery_strategy(recovery_strategy)
    validate_component_length(max_table_component_length)
    validate_component_length(max_field_component_length)
    validate_cache_size(cache_maxsize)

    # Validate ID field mapping if present
    if default_id_field is not None:
        validate_id_field_mapping(default_id_field)

    # Check for duplicate field names
    if id_field == parent_field or id_field == time_field or parent_field == time_field:
        raise ConfigurationError(
            f"Metadata field names must be unique. Got: "
            f"id={id_field}, parent={parent_field}, time={time_field}"
        )
