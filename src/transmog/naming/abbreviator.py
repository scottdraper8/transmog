"""Abbreviation system for table and field names.

Provides functionality to abbreviate long table and field names
with configurable abbreviation strategies and dictionaries.
"""

import functools
from typing import Optional

from ..config import settings


def abbreviate_component(
    component: str,
    max_length: Optional[int] = None,
    abbreviation_dict: Optional[dict[str, str]] = None,
) -> str:
    """Abbreviate a single path component.

    Args:
        component: Path component to abbreviate
        max_length: Maximum length for the abbreviated component
        abbreviation_dict: Dictionary of custom abbreviations

    Returns:
        Abbreviated component
    """
    # Use default from settings if max_length is None
    if max_length is None:
        max_length = settings.DEFAULT_MAX_FIELD_COMPONENT_LENGTH

    # Convert to string to be safe
    component = str(component)

    # Check for custom abbreviation first if provided
    if abbreviation_dict and isinstance(abbreviation_dict, dict):
        if component in abbreviation_dict:
            return abbreviation_dict[component]

    # If already short enough, return as is
    if len(component) <= max_length:
        return component

    # Simple truncation for unknown terms
    return component[:max_length]


# Cache for table name abbreviation to avoid frequent recomputation
@functools.lru_cache(maxsize=1024)
def _abbreviate_table_name_cached(
    path: str,
    parent_entity: str,
    separator: str = "_",
    abbreviate_enabled: bool = True,
    max_component_length: Optional[int] = None,
    preserve_root: bool = True,
    preserve_leaf: bool = True,
) -> str:
    """Cached version of abbreviate_table_name without the dictionary parameter."""
    # Use default from settings if max_component_length is None
    if max_component_length is None:
        max_component_length = settings.DEFAULT_MAX_TABLE_COMPONENT_LENGTH

    # Convert to string to be safe
    path = str(path)
    parent_entity = str(parent_entity)

    # If abbreviation is disabled, just return full path
    if not abbreviate_enabled:
        return f"{parent_entity}{separator}{path}" if path != parent_entity else path

    # Split the path into components
    parts = path.split(separator)

    # For a single-part path, it's a direct child of the entity
    if len(parts) <= 1:
        return f"{parent_entity}{separator}{path}" if path != parent_entity else path

    # Process each component
    abbreviated_parts = []
    for i, part in enumerate(parts):
        # Check if this is the root or leaf component
        is_first = i == 0
        is_last = i == len(parts) - 1

        # Apply abbreviation rules
        if (is_first and preserve_root) or (is_last and preserve_leaf):
            # Keep root/leaf component intact
            abbreviated_parts.append(part)
        else:
            # Abbreviate middle components by truncation
            abbreviated_parts.append(part[:max_component_length])

    # Join components into the final name
    if parent_entity in abbreviated_parts:
        # If parent entity is already in the path, don't duplicate it
        result = separator.join(abbreviated_parts)
    else:
        # Prepend parent entity if not already in the path
        result = f"{parent_entity}{separator}{separator.join(abbreviated_parts)}"

    return result


def abbreviate_table_name(
    path: str,
    parent_entity: str,
    separator: str = "_",
    abbreviate_enabled: bool = True,
    max_component_length: Optional[int] = None,
    preserve_root: bool = True,
    preserve_leaf: bool = True,
    abbreviation_dict: Optional[dict[str, str]] = None,
) -> str:
    """Generate abbreviated table name from path.

    Args:
        path: Array path (e.g., "orders_items_details")
        parent_entity: Root entity name
        separator: Separator character for path components
        abbreviate_enabled: Whether abbreviation is enabled
        max_component_length: Maximum length for each path component
        preserve_root: Whether to preserve the root component
        preserve_leaf: Whether to preserve the leaf component
        abbreviation_dict: Dictionary of custom abbreviations

    Returns:
        Abbreviated table name
    """
    # Use default from settings if max_component_length is None
    if max_component_length is None:
        max_component_length = settings.DEFAULT_MAX_TABLE_COMPONENT_LENGTH

    # Convert to string to be safe
    path = str(path)
    parent_entity = str(parent_entity)

    # If no dictionary, use the cached version
    if abbreviation_dict is None:
        return _abbreviate_table_name_cached(
            path,
            parent_entity,
            separator,
            abbreviate_enabled,
            max_component_length,
            preserve_root,
            preserve_leaf,
        )

    # If abbreviation is disabled, just return full path
    if not abbreviate_enabled:
        return f"{parent_entity}{separator}{path}" if path != parent_entity else path

    # Split the path into components
    parts = path.split(separator)

    # For a single-part path, it's a direct child of the entity
    if len(parts) <= 1:
        return f"{parent_entity}{separator}{path}" if path != parent_entity else path

    # Process each component
    abbreviated_parts = []
    for i, part in enumerate(parts):
        # Check if this is the root or leaf component
        is_first = i == 0
        is_last = i == len(parts) - 1

        # Apply abbreviation rules
        if (is_first and preserve_root) or (is_last and preserve_leaf):
            # Keep root/leaf component intact
            abbreviated_parts.append(part)
        else:
            # Abbreviate middle components
            abbreviated_parts.append(
                abbreviate_component(
                    part,
                    max_length=max_component_length,
                    abbreviation_dict=abbreviation_dict,
                )
            )

    # Join components into the final name
    if parent_entity in abbreviated_parts:
        # If parent entity is already in the path, don't duplicate it
        result = separator.join(abbreviated_parts)
    else:
        # Prepend parent entity if not already in the path
        result = f"{parent_entity}{separator}{separator.join(abbreviated_parts)}"

    return result


@functools.lru_cache(maxsize=1024)
def _abbreviate_field_name_cached(
    field_path: str,
    separator: str = "_",
    abbreviate_enabled: bool = True,
    max_component_length: Optional[int] = None,
    preserve_root: bool = True,
    preserve_leaf: bool = True,
) -> str:
    """Cached version of abbreviate_field_name without the dictionary parameter."""
    # Use default from settings if max_component_length is None
    if max_component_length is None:
        max_component_length = settings.DEFAULT_MAX_FIELD_COMPONENT_LENGTH

    # If abbreviation is disabled, return the original path
    if not abbreviate_enabled:
        return field_path

    # Split the path into components
    parts = field_path.split(separator)

    # If single component, return as is
    if len(parts) <= 1:
        return field_path

    # Process each component
    abbreviated_parts = []
    for i, part in enumerate(parts):
        # Check if this is the root or leaf component
        is_first = i == 0
        is_last = i == len(parts) - 1

        # Apply abbreviation rules
        if (is_first and preserve_root) or (is_last and preserve_leaf):
            # Keep root/leaf component intact
            abbreviated_parts.append(part)
        else:
            # Simple truncation for cached version (no dictionary to use)
            if len(part) <= max_component_length:
                abbreviated_parts.append(part)
            else:
                abbreviated_parts.append(part[:max_component_length])

    # Join components into the final name
    return separator.join(abbreviated_parts)


def abbreviate_field_name(
    field_path: str,
    separator: str = "_",
    abbreviate_enabled: bool = True,
    max_component_length: Optional[int] = None,
    preserve_root: bool = True,
    preserve_leaf: bool = True,
    abbreviation_dict: Optional[dict[str, str]] = None,
) -> str:
    """Generate abbreviated field name from path.

    Args:
        field_path: Field path (e.g., "order_items_price")
        separator: Separator character for path components
        abbreviate_enabled: Whether abbreviation is enabled
        max_component_length: Maximum length for each path component
        preserve_root: Whether to preserve the root component
        preserve_leaf: Whether to preserve the leaf component
        abbreviation_dict: Dictionary of custom abbreviations

    Returns:
        Abbreviated field name
    """
    # Use default from settings if max_component_length is None
    if max_component_length is None:
        max_component_length = settings.DEFAULT_MAX_FIELD_COMPONENT_LENGTH

    # If abbreviation is disabled, return the original path
    if not abbreviate_enabled:
        return field_path

    # Split the path into components using the separator
    parts = field_path.split(separator)

    # If single component, return as is
    if len(parts) <= 1:
        return field_path

    # Process each component
    abbreviated_parts = []
    for i, part in enumerate(parts):
        # Check if this is the root or leaf component
        is_first = i == 0
        is_last = i == len(parts) - 1

        # Apply abbreviation rules
        if (is_first and preserve_root) or (is_last and preserve_leaf):
            # Keep root/leaf component intact
            abbreviated_parts.append(part)
        else:
            # Try custom abbreviation first if available
            if abbreviation_dict and part in abbreviation_dict:
                abbreviated_parts.append(abbreviation_dict[part])
            # If not, truncate to max length if needed
            elif len(part) <= max_component_length:
                abbreviated_parts.append(part)
            else:
                abbreviated_parts.append(part[:max_component_length])

    # Join components into the final name with the separator
    return separator.join(abbreviated_parts)


def merge_abbreviation_dicts(*dicts: dict[str, str]) -> dict[str, str]:
    """Merge multiple abbreviation dictionaries.

    Args:
        *dicts: Dictionaries to merge

    Returns:
        Merged dictionary
    """
    result = {}
    for d in dicts:
        result.update(d)
    return result
