"""Natural ID field discovery utilities."""

from typing import Any, Optional


def get_record_id(
    record: dict[str, Any],
    id_patterns: Optional[list[str]] = None,
    fallback_field: Optional[str] = None,
) -> tuple[Optional[str], Optional[Any]]:
    """Get the ID value from a record.

    Args:
        record: Record to extract ID from
        id_patterns: Field names to check for natural IDs
            (defaults to common patterns if not provided)
        fallback_field: Fallback field if natural ID not found

    Returns:
        Tuple of (field_name, id_value) or (None, None)
    """
    if not isinstance(record, dict):
        return None, None

    if id_patterns is None:
        id_patterns = [
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
        ]

    for pattern in id_patterns:
        if pattern in record:
            value = record[pattern]
            if value is not None and (
                (isinstance(value, str) and value.strip())
                or isinstance(value, (int, float))
            ):
                return pattern, value

    if fallback_field and fallback_field in record:
        return fallback_field, record[fallback_field]

    return None, None
