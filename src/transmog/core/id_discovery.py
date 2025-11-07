"""Natural ID field discovery utilities.

Provides functions to discover existing ID fields in data
to avoid generating synthetic IDs when natural keys exist.
"""

from typing import Any, Optional

# Common ID field names to check, in order of preference
DEFAULT_ID_FIELD_PATTERNS = [
    "id",
    "ID",
    "Id",
    "_id",
    "uuid",
    "UUID",
    "guid",
    "GUID",
    "pk",
    "PK",
    "primary_key",
    "key",
    "identifier",
    "code",
    "number",
    "no",
]


def discover_id_field(
    record: dict[str, Any],
    id_patterns: Optional[list[str]] = None,
    path: Optional[str] = None,
) -> Optional[str]:
    """Discover a natural ID field in a record.

    Args:
        record: The record to search for ID fields
        id_patterns: List of field names to check (uses defaults if None)
        path: The path/table name (unused, kept for compatibility)

    Returns:
        The name of the discovered ID field, or None if not found
    """
    if not isinstance(record, dict):
        return None

    patterns = id_patterns if id_patterns is not None else DEFAULT_ID_FIELD_PATTERNS

    for pattern in patterns:
        if pattern in record and record[pattern] is not None:
            value = record[pattern]
            if _is_valid_id_value(value):
                return pattern

    return None


def _is_valid_id_value(value: Any) -> bool:
    """Check if a value is suitable to use as an ID.

    Args:
        value: The value to check

    Returns:
        True if the value can be used as an ID
    """
    if value is None:
        return False

    # Allow scalar types that can be used as IDs
    if isinstance(value, (str, int, float)):
        # Check for empty strings
        if isinstance(value, str) and not value.strip():
            return False
        return True

    # Disallow complex types
    return False


def get_record_id(
    record: dict[str, Any],
    id_patterns: Optional[list[str]] = None,
    fallback_field: Optional[str] = None,
) -> tuple[Optional[str], Optional[Any]]:
    """Get the ID value from a record, discovering the field if needed.

    Args:
        record: The record to get ID from
        id_patterns: List of field names to check
        fallback_field: Field to check if natural ID not found

    Returns:
        Tuple of (field_name, id_value) or (None, None) if not found
    """
    id_field = discover_id_field(record, id_patterns)

    if id_field and id_field in record:
        return id_field, record[id_field]

    if fallback_field and fallback_field in record:
        return fallback_field, record[fallback_field]

    return None, None


def should_add_transmog_id(
    record: dict[str, Any],
    id_patterns: Optional[list[str]] = None,
    path: Optional[str] = None,
) -> bool:
    """Determine if a transmog ID should be added to a record.

    Args:
        record: The record to check
        id_patterns: List of field names to check
        path: The path/table name (unused, kept for compatibility)

    Returns:
        True if a transmog ID should be added
    """
    id_field = discover_id_field(record, id_patterns, path)
    return id_field is None
