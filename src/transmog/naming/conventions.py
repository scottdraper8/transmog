"""Table naming conventions module.

Provides functions to generate standardized table names
for nested arrays based on their hierarchy.
"""

import functools
from typing import Optional


@functools.lru_cache(maxsize=256)
def get_table_name(
    path: str,
    parent_entity: str,
    separator: str = "_",
    parent_path: str = "",
    abbreviate_middle: bool = True,
    abbreviation_length: Optional[int] = None,
) -> str:
    """Generate standardized table name for nested arrays.

    Naming conventions:
    - First level: <entity>_<arrayname>
    - Nested arrays: <entity>_<path>_<arrayname>

    This function is cached to avoid recalculating table names repeatedly.

    Args:
        path: Array path or name
        parent_entity: Top-level entity name
        separator: Separator character for path components
        parent_path: Path to the parent object containing this array
        abbreviate_middle: Whether to abbreviate middle segments
            (deprecated, handled by extractor)
        abbreviation_length: Length of abbreviations
            (deprecated, handled by extractor)

    Returns:
        Formatted table name
    """
    # For first-level arrays (direct children of the entity)
    if not parent_path:
        return f"{parent_entity}{separator}{path}"

    # For nested arrays
    full_path = f"{parent_path}{separator}{path}"
    return f"{parent_entity}{separator}{full_path.replace('/', separator)}"


@functools.lru_cache(maxsize=1024)
def sanitize_name(
    name: str,
    separator: str = "_",
    replace_with: str = "_",
    sql_safe: bool = True,
    preserve_separator: bool = False,
) -> str:
    """Sanitize names to prevent issues with path parsing and SQL compatibility.

    Args:
        name: Name to sanitize
        separator: Character to replace
        replace_with: Replacement string
        sql_safe: Make names SQL-safe (remove spaces, special chars)
        preserve_separator: Whether to preserve the separator character when it's used
            in field names

    Returns:
        Sanitized name
    """
    # Handle separator replacement based on configuration
    if separator != replace_with and not preserve_separator:
        sanitized = name.replace(separator, replace_with)
    else:
        sanitized = name

    # SQL safety transformations
    if sql_safe:
        sanitized = sanitized.replace(" ", "_")
        sanitized = sanitized.replace("-", "_")

        # Process special characters
        result = ""
        last_was_underscore = False

        for c in sanitized:
            if c.isalnum() or c == "_":
                result += c
                last_was_underscore = c == "_"
            else:
                # Avoid consecutive underscores
                if not last_was_underscore:
                    result += "_"
                    last_was_underscore = True

        sanitized = result

        # Prefix numeric names with "col_"
        if sanitized and sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"

        # Fallback for empty names
        if not sanitized:
            sanitized = "unnamed_field"

    return sanitized


def sanitize_column_names(
    columns: list[str],
    separator: str = "_",
    replace_with: str = "_",
    sql_safe: bool = True,
) -> list[str]:
    """Sanitize a list of column names.

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
        sanitized = sanitize_name(
            column, separator=separator, replace_with=replace_with, sql_safe=sql_safe
        )
        result.append(sanitized)

    return result


@functools.lru_cache(maxsize=256)
def get_standard_field_name(
    field_name: str,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    separator: str = "_",
) -> str:
    """Generate standardized field name.

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


# Path operations with caching for performance
@functools.lru_cache(maxsize=1024)
def split_path(path: str, separator: str = "_") -> tuple[str, ...]:
    """Split a path into components with caching.

    Args:
        path: Path to split
        separator: Separator character

    Returns:
        Tuple of path components
    """
    return tuple(path.split(separator))


@functools.lru_cache(maxsize=1024)
def join_path(parts: tuple[str, ...], separator: str = "_") -> str:
    """Join path components with caching.

    Args:
        parts: Path components as tuple (must be hashable)
        separator: Separator character

    Returns:
        Joined path string
    """
    return separator.join(parts)
