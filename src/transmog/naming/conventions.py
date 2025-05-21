"""Table naming conventions module.

Provides functions to generate standardized table names
based on field combinations with separators.
"""

import functools
from typing import Optional


def get_table_name(
    path: str,
    parent_entity: str,
    separator: str = "_",
    parent_path: str = "",
    deeply_nested_threshold: int = 4,
) -> str:
    """Generate standardized table name for nested arrays.

    Naming convention is simplified to directly combine field names with separators.
    Special handling is only provided for deeply nested structures (>4 layers).

    Args:
        path: Array path or name
        parent_entity: Top-level entity name
        separator: Separator character for path components
        parent_path: Path to the parent object containing this array
        deeply_nested_threshold: Threshold for when to consider a path deeply nested

    Returns:
        Formatted table name
    """
    # For first-level arrays (direct children of the entity)
    if not parent_path:
        return f"{parent_entity}{separator}{path}"

    # For nested arrays
    full_path = f"{parent_path}{separator}{path}"
    path_parts = full_path.split(separator)

    # Check if we have a deeply nested structure
    # Including the entity name, a path with deeply_nested_threshold components
    # already counts as deeply nested
    if len(path_parts) >= deeply_nested_threshold:
        # For deeply nested structures, use first component, nested indicator, and the
        # last component
        simplified_path = separator.join(
            [
                path_parts[0],  # First component
                "nested",  # Indicator of nesting
                path_parts[-1],  # Last component
            ]
        )
        return f"{parent_entity}{separator}{simplified_path}"

    # For regular nesting, simply combine all fields
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


def handle_deeply_nested_path(
    path: str, separator: str = "_", deeply_nested_threshold: int = 4
) -> str:
    """Handle deeply nested paths by simplifying them.

    For paths with more than the threshold number of components,
    only keep the first and last components.

    Args:
        path: The path to potentially simplify
        separator: Separator character for path components
        deeply_nested_threshold: Threshold for when to consider a path deeply nested

    Returns:
        Simplified path for deeply nested structures, original path otherwise
    """
    components = path.split(separator)

    # Including the entity name, a path with deeply_nested_threshold components
    # already counts as deeply nested
    if len(components) >= deeply_nested_threshold:
        # For deeply nested structures, include first and last components
        # with a "nested" indicator in between
        return separator.join([components[0], "nested", components[-1]])

    return path
