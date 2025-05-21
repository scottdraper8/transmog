"""Naming utilities for field and table names.

This module provides common functions for field and table name handling
to reduce duplication across modules.
"""


def get_table_name_for_array(
    entity_name: str,
    array_name: str,
    parent_path: str,
    separator: str,
    deeply_nested_threshold: int = 4,
) -> str:
    """Generate consistent table names for arrays.

    This function centralizes the logic for generating table names from array paths
    to ensure consistency across modules.

    Args:
        entity_name: Entity name
        array_name: Array field name
        parent_path: Parent path
        separator: Separator for path components
        deeply_nested_threshold: Threshold for deeply nested paths (default 4)

    Returns:
        Generated table name
    """
    if not parent_path:
        # First level array: <entity>_<arrayname>
        return f"{entity_name}{separator}{array_name}"

    # For nested arrays, simply combine all path components
    full_path = f"{parent_path}{separator}{array_name}"
    path_parts = full_path.split(separator)

    # Handle deeply nested structures specially
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
        return f"{entity_name}{separator}{simplified_path}"

    # For regular nesting, simply combine entity with the full path
    return f"{entity_name}{separator}{full_path}"
