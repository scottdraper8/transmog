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
    nested_threshold: int = 4,
) -> str:
    """Generate standardized table name for nested arrays.

    Naming convention is simplified to directly combine field names with separators.
    Special handling is only provided for deeply nested structures (>4 layers).

    Args:
        path: Array path or name
        parent_entity: Top-level entity name
        separator: Separator character for path components
        parent_path: Path to the parent object containing this array
        nested_threshold: Threshold for when to consider a path deeply nested

    Returns:
        Formatted table name
    """
    if not parent_path:
        return f"{parent_entity}{separator}{path}"

    full_path = f"{parent_path}{separator}{path}"
    path_parts = full_path.split(separator)

    if len(path_parts) >= nested_threshold:
        simplified_path = separator.join(
            [
                path_parts[0],
                "nested",
                path_parts[-1],
            ]
        )
        return f"{parent_entity}{separator}{simplified_path}"

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
    if separator != replace_with and not preserve_separator:
        sanitized = name.replace(separator, replace_with)
    else:
        sanitized = name

    if sql_safe:
        sanitized = sanitized.replace(" ", "_")
        sanitized = sanitized.replace("-", "_")

        result = ""
        last_was_underscore = False

        for c in sanitized:
            if c.isalnum() or c == "_":
                result += c
                last_was_underscore = c == "_"
            else:
                if not last_was_underscore:
                    result += "_"
                    last_was_underscore = True

        sanitized = result
        sanitized = sanitized.strip("_")

        if sanitized and sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"

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
