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
    id_field_patterns: Optional[list[str]] = None,
    path: Optional[str] = None,
    id_field_mapping: Optional[dict[str, str]] = None,
) -> Optional[str]:
    """Discover a natural ID field in a record.

    Args:
        record: The record to search for ID fields
        id_field_patterns: List of field names to check (uses defaults if None)
        path: The path/table name for path-specific mappings
        id_field_mapping: Optional mapping of paths to specific ID fields

    Returns:
        The name of the discovered ID field, or None if not found
    """
    if not isinstance(record, dict):
        return None

    # Check path-specific mapping first
    if id_field_mapping and path:
        # Direct path match
        if path in id_field_mapping:
            field_name = id_field_mapping[path]
            if field_name in record and record[field_name] is not None:
                return field_name

        # Wildcard match
        if "*" in id_field_mapping:
            field_name = id_field_mapping["*"]
            if field_name in record and record[field_name] is not None:
                return field_name

    # Use provided patterns or defaults
    patterns = (
        id_field_patterns
        if id_field_patterns is not None
        else DEFAULT_ID_FIELD_PATTERNS
    )

    # Check each pattern
    for pattern in patterns:
        if pattern in record and record[pattern] is not None:
            # Verify the value is suitable as an ID (not empty, not a complex object)
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
    id_field_patterns: Optional[list[str]] = None,
    path: Optional[str] = None,
    id_field_mapping: Optional[dict[str, str]] = None,
    fallback_field: Optional[str] = None,
) -> tuple[Optional[str], Optional[Any]]:
    """Get the ID value from a record, discovering the field if needed.

    Args:
        record: The record to get ID from
        id_field_patterns: List of field names to check
        path: The path/table name
        id_field_mapping: Optional mapping of paths to specific ID fields
        fallback_field: Field to check if natural ID not found

    Returns:
        Tuple of (field_name, id_value) or (None, None) if not found
    """
    # Try to discover natural ID field
    id_field = discover_id_field(record, id_field_patterns, path, id_field_mapping)

    if id_field and id_field in record:
        return id_field, record[id_field]

    # Check fallback field (like __transmog_id)
    if fallback_field and fallback_field in record:
        return fallback_field, record[fallback_field]

    return None, None


def should_add_transmog_id(
    record: dict[str, Any],
    id_field_patterns: Optional[list[str]] = None,
    path: Optional[str] = None,
    id_field_mapping: Optional[dict[str, str]] = None,
    force_transmog_id: bool = False,
) -> bool:
    """Determine if a transmog ID should be added to a record.

    Args:
        record: The record to check
        id_field_patterns: List of field names to check
        path: The path/table name
        id_field_mapping: Optional mapping of paths to specific ID fields
        force_transmog_id: If True, always add transmog ID

    Returns:
        True if a transmog ID should be added
    """
    if force_transmog_id:
        return True

    # Check if natural ID exists
    id_field = discover_id_field(record, id_field_patterns, path, id_field_mapping)

    # Add transmog ID only if no natural ID found
    return id_field is None


def build_id_field_mapping(
    config: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, str]]:
    """Build ID field mapping from configuration.

    Args:
        config: Configuration dictionary

    Returns:
        ID field mapping or None
    """
    if not config:
        return None

    # Check for direct mapping
    if "id_field_mapping" in config:
        # Ensure correct return type
        mapping = config["id_field_mapping"]
        if isinstance(mapping, dict):
            return {str(k): str(v) for k, v in mapping.items()}
        return None

    # Check for simple default ID field
    if "natural_id_field" in config:
        # Convert single field to wildcard mapping
        field = config["natural_id_field"]
        if isinstance(field, str):
            return {"*": field}
        return None

    return None
