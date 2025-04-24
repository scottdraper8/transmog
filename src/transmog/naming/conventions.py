"""
Table naming conventions module.

Provides functions to generate standardized table names
for nested arrays based on their hierarchy.
"""

import functools
from typing import List, Optional, Tuple


@functools.lru_cache(maxsize=256)
def get_table_name(
    path: str,
    parent_entity: str,
    separator: str = "_",
    abbreviate_middle: bool = True,
    abbreviation_length: int = 2,
) -> str:
    """
    Generate standardized table name for nested arrays.

    Naming conventions:
    - First level: parent_arrayname
    - Deeper: parent_abbr1_abbr2_arrayname

    This function is cached to avoid recalculating table names repeatedly.

    Args:
        path: Array path with potential nesting
        parent_entity: Top-level entity name
        separator: Separator character for path components
        abbreviate_middle: Whether to abbreviate middle segments
        abbreviation_length: Length of abbreviations

    Returns:
        Formatted table name
    """
    # Split the path into components
    parts = path.split(separator)

    # For a single-part path, it's a direct child of the entity
    if len(parts) <= 1:
        # For direct children of the entity, use full parent name
        return f"{parent_entity}{separator}{path}"

    # For deeper nesting, first get the final array name
    array_name = parts[-1]

    # If we're not abbreviating middle segments, return simplified name
    if not abbreviate_middle:
        return f"{parent_entity}{separator}{array_name}"

    # Extract all path segments except the final one and abbreviate them
    middle_segments = parts[:-1]
    abbreviated_path = []

    for segment in middle_segments:
        # Create abbreviation from segment
        if len(segment) <= abbreviation_length:
            abbrev = segment
        else:
            # Use first n chars as an abbreviation
            abbrev = segment[:abbreviation_length]

        abbreviated_path.append(abbrev)

    # Join everything with separators
    result = f"{parent_entity}{separator}{separator.join(abbreviated_path)}{separator}{array_name}"

    return result


@functools.lru_cache(maxsize=1024)
def sanitize_name(
    name: str,
    separator: str = "_",
    replace_with: str = "",
) -> str:
    """
    Sanitize names to prevent issues with path parsing.

    Args:
        name: Name to sanitize
        separator: Character to replace
        replace_with: Replacement string

    Returns:
        Sanitized name
    """
    return name.replace(separator, replace_with)


def sanitize_column_names(
    columns: List[str],
    separator: str = "_",
    replace_with: str = "_",
    sql_safe: bool = True,
) -> List[str]:
    """
    Sanitize a list of column names.

    Args:
        columns: List of column names to sanitize
        separator: Character to replace
        replace_with: Replacement string
        sql_safe: Make column names SQL-safe (remove spaces, special chars)

    Returns:
        List of sanitized column names
    """
    result = []
    for column in columns:
        # Replace separator characters
        sanitized = sanitize_name(column, separator, replace_with)

        # Make SQL-safe if requested
        if sql_safe:
            # Replace spaces with underscores
            sanitized = sanitized.replace(" ", "_")

            # Replace dashes with underscores to preserve readability
            sanitized = sanitized.replace("-", "_")

            # Replace other special characters
            sanitized = "".join(
                c if c.isalnum() or c == "_" else "_" for c in sanitized
            )

            # Ensure it doesn't start with a number
            if sanitized and sanitized[0].isdigit():
                sanitized = f"col_{sanitized}"

            # Handle empty column names
            if not sanitized:
                sanitized = "unnamed_column"

        result.append(sanitized)

    return result


@functools.lru_cache(maxsize=256)
def get_standard_field_name(
    field_name: str,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    separator: str = "_",
) -> str:
    """
    Generate standardized field name.

    Args:
        field_name: Original field name
        prefix: Optional prefix to add
        suffix: Optional suffix to add
        separator: Separator character

    Returns:
        Standardized field name
    """
    result = field_name

    if prefix:
        result = f"{prefix}{separator}{result}"

    if suffix:
        result = f"{result}{separator}{suffix}"

    return result


# Cached path splitting for efficiency in repeated operations
@functools.lru_cache(maxsize=1024)
def split_path(path: str, separator: str = "_") -> Tuple[str, ...]:
    """
    Split a path into components with caching.

    Args:
        path: Path to split
        separator: Separator character

    Returns:
        Tuple of path components
    """
    return tuple(path.split(separator))


# Cached path joining for efficiency in repeated operations
@functools.lru_cache(maxsize=1024)
def join_path(parts: Tuple[str, ...], separator: str = "_") -> str:
    """
    Join path components with caching.

    Args:
        parts: Path components as tuple (must be hashable)
        separator: Separator character

    Returns:
        Joined path string
    """
    return separator.join(parts)
