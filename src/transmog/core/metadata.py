"""Metadata handling module for data annotation.

Provides functions to generate transmog IDs, timestamps,
and other metadata for record tracking and lineage.
"""

import functools
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Union

from .id_discovery import should_add_transmog_id

# Logger initialization
logger = logging.getLogger(__name__)

# Namespace UUID for deterministic ID generation
TRANSMOG_NAMESPACE = uuid.UUID("a9b8c7d6-e5f4-1234-abcd-0123456789ab")


def generate_transmog_id(
    record: Optional[dict[str, Any]] = None,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
) -> str:
    """Generate a unique ID for record tracking.

    This function can generate IDs in three ways:
    1. Random UUID (default)
    2. Deterministic UUID based on record field(s)
    3. Custom function-based ID generation

    Args:
        record: The record for which to generate an ID
        source_field: Field name to use for deterministic ID generation
        id_generation_strategy: Custom function to generate ID

    Returns:
        UUID string
    """
    # Option 3: Custom function-based generation
    if id_generation_strategy is not None and record is not None:
        try:
            return str(id_generation_strategy(record))
        except Exception:
            return str(uuid.uuid4())

    # Option 2: Deterministic field-based generation
    if record is not None and source_field is not None:
        try:
            field_value = record.get(source_field)
            if field_value:
                return generate_deterministic_id(field_value)
        except Exception as e:
            logger.debug(f"Error generating deterministic ID: {e}")
            pass

    # Option 1: Random UUID (default)
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
    parent_id: Optional[str] = None,
    transmog_id: Optional[str] = None,
    transmog_time: Optional[Any] = None,
    id_field: str = "__transmog_id",
    parent_field: str = "__parent_transmog_id",
    time_field: str = "__transmog_datetime",
    extra_fields: Optional[dict[str, Any]] = None,
    in_place: bool = True,  # Default to in-place for better performance
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    id_field_patterns: Optional[list[str]] = None,
    path: Optional[str] = None,
    id_field_mapping: Optional[dict[str, str]] = None,
    force_transmog_id: bool = False,
) -> dict[str, Any]:
    """Annotate a record with metadata fields.

    Args:
        record: Record dictionary to annotate
        parent_id: Optional parent record ID
        transmog_id: Transmog ID (generated if None)
        transmog_time: Transmog timestamp (current time if None)
        id_field: Field name for transmog ID
        parent_field: Field name for parent ID
        time_field: Field name for timestamp
        extra_fields: Additional metadata fields to add
        in_place: Whether to modify the record in place (default True for performance)
        source_field: Field name to use for deterministic ID generation
        id_generation_strategy: Custom function to generate ID
        id_field_patterns: List of field names to check for natural IDs
        path: Current path/table name for ID mapping
        id_field_mapping: Optional mapping of paths to specific ID fields
        force_transmog_id: If True, always add transmog ID

    Returns:
        Annotated record
    """
    # Use in-place modification by default for better memory efficiency
    annotated = record if in_place else record.copy()

    # Check if transmog ID should be added
    if should_add_transmog_id(
        annotated, id_field_patterns, path, id_field_mapping, force_transmog_id
    ):
        # Add transmog ID
        if transmog_id is None:
            transmog_id = generate_transmog_id(
                record=annotated,
                source_field=source_field,
                id_generation_strategy=id_generation_strategy,
            )
        annotated[id_field] = transmog_id

    # Add parent ID if provided
    if parent_id is not None:
        annotated[parent_field] = parent_id

    # Add transmog timestamp only if time_field is provided
    if time_field:
        if transmog_time is None:
            transmog_time = get_current_timestamp()
        annotated[time_field] = transmog_time

    # Add extra fields efficiently
    if extra_fields:
        # Use update for in-place efficiency
        annotated.update(extra_fields)

    return annotated


def create_batch_metadata(
    batch_size: int,
    transmog_time: Optional[Any] = None,
    parent_id: Optional[str] = None,
    records: Optional[list[dict[str, Any]]] = None,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
) -> dict[str, Union[str, Any]]:
    """Create metadata for a batch of records.

    Args:
        batch_size: Number of records in the batch
        transmog_time: Transmog timestamp
        parent_id: Optional parent record ID
        records: Optional record data for ID generation
        source_field: Field name to use for deterministic ID generation
        id_generation_strategy: Custom function to generate ID

    Returns:
        Dictionary of batch metadata
    """
    # Generate batch ID
    if records and (source_field or id_generation_strategy):
        # Use first record for deterministic ID
        first_record = records[0]
        batch_id = generate_transmog_id(
            record=first_record,
            source_field=source_field,
            id_generation_strategy=id_generation_strategy,
        )
    else:
        # Random UUID fallback
        batch_id = str(uuid.uuid4())

    # Use timestamp if not provided
    if transmog_time is None:
        transmog_time = get_current_timestamp()

    # Construct metadata dictionary
    metadata = {
        "batch_id": batch_id,
        "batch_size": batch_size,
        "transmog_time": transmog_time,
    }

    # Include parent ID if available
    if parent_id:
        metadata["parent_id"] = parent_id

    return metadata
