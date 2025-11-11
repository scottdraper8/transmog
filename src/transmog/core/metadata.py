"""Metadata handling module for data annotation.

Provides functions to generate transmog IDs, timestamps,
and other metadata for record tracking and lineage.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from .id_discovery import get_record_id

# Namespace UUID for deterministic ID generation
TRANSMOG_NAMESPACE = uuid.UUID("a9b8c7d6-e5f4-1234-abcd-0123456789ab")


def _generate_deterministic_id_from_value(value: Any) -> str:
    """Generate a deterministic UUID5 from a value.

    Args:
        value: Value to generate deterministic ID from

    Returns:
        UUID5 string
    """
    if isinstance(value, (dict, list)):
        value_str = json.dumps(value, sort_keys=True, ensure_ascii=False)
    else:
        value_str = str(value)

    normalized_value = value_str.strip().lower()
    deterministic_uuid = uuid.uuid5(TRANSMOG_NAMESPACE, normalized_value)
    return str(deterministic_uuid)


def _generate_composite_id(record: dict[str, Any], id_fields: list[str]) -> str:
    """Generate deterministic ID from multiple fields.

    Args:
        record: Record dictionary
        id_fields: List of field names to use for composite ID

    Returns:
        Deterministic UUID5 string
    """
    composite_values = []
    for field in id_fields:
        if field in record:
            composite_values.append((field, record[field]))
        else:
            composite_values.append((field, None))

    composite_dict = dict(composite_values)
    return _generate_deterministic_id_from_value(composite_dict)


def generate_transmog_id(
    record: Optional[dict[str, Any]] = None,
    deterministic: bool = False,
    id_fields: Optional[list[str]] = None,
) -> str:
    """Generate a unique ID for record tracking.

    Args:
        record: The record for which to generate an ID
        deterministic: Whether to generate deterministic IDs
        id_fields: List of field names for composite deterministic IDs

    Returns:
        UUID string
    """
    if deterministic and record is not None:
        if id_fields:
            return _generate_composite_id(record, id_fields)
        return _generate_deterministic_id_from_value(record)

    return str(uuid.uuid4())


def get_current_timestamp() -> str:
    """Get current timestamp in UTC as ISO format string.

    Returns:
        Formatted timestamp string
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def annotate_with_metadata(
    record: dict[str, Any],
    config: Any,
    parent_id: Optional[str] = None,
    transmog_time: Optional[str] = None,
) -> dict[str, Any]:
    """Annotate a record with metadata fields.

    Modifies the record in place and returns it.

    Args:
        record: Record dictionary to annotate
        config: Configuration object
        parent_id: Optional parent record ID
        transmog_time: Transmog timestamp (current time if None)

    Returns:
        Annotated record
    """
    field, _ = get_record_id(record, config.id_patterns)
    if field is None:
        transmog_id = generate_transmog_id(
            record=record,
            deterministic=config.deterministic_ids,
            id_fields=config.id_fields,
        )
        record[config.id_field] = transmog_id

    if parent_id is not None:
        record[config.parent_field] = parent_id

    if config.time_field:
        if transmog_time is None:
            transmog_time = get_current_timestamp()
        record[config.time_field] = transmog_time

    return record
