"""Metadata handling module for data annotation.

Provides functions to generate transmog IDs, timestamps,
and other metadata for record tracking and lineage.
"""

import functools
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from .id_discovery import should_add_transmog_id

if TYPE_CHECKING:
    from transmog.config import TransmogConfig

logger = logging.getLogger(__name__)

# Namespace UUID for deterministic ID generation
TRANSMOG_NAMESPACE = uuid.UUID("a9b8c7d6-e5f4-1234-abcd-0123456789ab")


def generate_transmog_id(
    record: Optional[dict[str, Any]] = None,
    id_generator: Optional[Callable[[dict[str, Any]], str]] = None,
) -> str:
    """Generate a unique ID for record tracking.

    Args:
        record: The record for which to generate an ID
        id_generator: Custom function to generate ID

    Returns:
        UUID string
    """
    if id_generator is not None and record is not None:
        try:
            return str(id_generator(record))
        except Exception:
            return str(uuid.uuid4())

    return str(uuid.uuid4())


def generate_deterministic_id(value: Any) -> str:
    """Generate a deterministic UUID5 from a value.

    Args:
        value: Value to generate deterministic ID from

    Returns:
        UUID5 string
    """
    value_str = str(value)

    # Normalize for consistent hashing
    normalized_value = value_str.strip().lower()

    # Generate UUID5 with namespace
    deterministic_uuid = uuid.uuid5(TRANSMOG_NAMESPACE, normalized_value)

    return str(deterministic_uuid)


def generate_composite_id(values: dict[str, Any], fields: list) -> str:
    """Generate a deterministic ID from multiple fields.

    Args:
        values: Dictionary containing the field values
        fields: List of field names to include

    Returns:
        Deterministic UUID string
    """
    # Sort fields for consistent ordering
    sorted_fields = sorted(fields)
    combined = "|".join(str(values.get(field, "")) for field in sorted_fields)

    return generate_deterministic_id(combined)


@functools.lru_cache(maxsize=8)
def get_current_timestamp(
    format_string: Optional[str] = None, as_string: bool = True
) -> Any:
    """Get timestamp in UTC with caching for improved performance.

    This function is cached to avoid excessive timestamp generation in batch processing.
    The cache has a small size since timestamp values change frequently.

    Args:
        format_string: Optional format string for datetime
        as_string: Whether to return as string or datetime object

    Returns:
        Formatted timestamp string or datetime object
    """
    timestamp = datetime.now(timezone.utc)

    if as_string and format_string:
        return timestamp.strftime(format_string)
    elif as_string:
        # ISO format with microseconds
        return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")

    return timestamp


def annotate_with_metadata(
    record: dict[str, Any],
    config: "TransmogConfig",
    parent_id: Optional[str] = None,
    transmog_time: Optional[Any] = None,
    path: Optional[str] = None,
    in_place: bool = True,
) -> dict[str, Any]:
    """Annotate a record with metadata fields.

    Args:
        record: Record dictionary to annotate
        config: Configuration object
        parent_id: Optional parent record ID
        transmog_time: Transmog timestamp (current time if None)
        path: Current path/table name for ID discovery
        in_place: Whether to modify the record in place

    Returns:
        Annotated record
    """
    annotated = record if in_place else record.copy()

    if should_add_transmog_id(annotated, config.id_patterns, path):
        transmog_id = generate_transmog_id(
            record=annotated,
            id_generator=config.id_generator,
        )
        annotated[config.id_field] = transmog_id

    if parent_id is not None:
        annotated[config.parent_field] = parent_id

    if config.time_field:
        if transmog_time is None:
            transmog_time = get_current_timestamp()
        annotated[config.time_field] = transmog_time

    return annotated


def create_batch_metadata(
    batch_size: int,
    transmog_time: Optional[Any] = None,
    parent_id: Optional[str] = None,
) -> dict[str, Union[str, Any]]:
    """Create metadata for a batch of records.

    Args:
        batch_size: Number of records in the batch
        transmog_time: Transmog timestamp
        parent_id: Optional parent record ID

    Returns:
        Dictionary of batch metadata
    """
    batch_id = str(uuid.uuid4())

    if transmog_time is None:
        transmog_time = get_current_timestamp()

    metadata = {
        "batch_id": batch_id,
        "batch_size": batch_size,
        "transmog_time": transmog_time,
    }

    if parent_id:
        metadata["parent_id"] = parent_id

    return metadata
