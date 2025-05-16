"""Metadata handling module for data annotation.

Provides functions to generate extract IDs, timestamps,
and other metadata for record tracking and lineage.
"""

import functools
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Union

# Logger initialization
logger = logging.getLogger(__name__)

# Namespace UUID for deterministic ID generation
TRANSMOG_NAMESPACE = uuid.UUID("a9b8c7d6-e5f4-1234-abcd-0123456789ab")


def generate_extract_id(
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
    """Get current timestamp in UTC with caching for improved performance.

    This function is cached to avoid excessive timestamp generation in batch processing.
    The cache has a small size since timestamp values change frequently.

    Args:
        format_string: Optional format string for datetime
        as_string: Whether to return as string or datetime object

    Returns:
        Formatted timestamp string or datetime object
    """
    now = datetime.now(timezone.utc)

    if as_string and format_string:
        return now.strftime(format_string)
    elif as_string:
        # ISO format with microseconds
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")

    return now


def annotate_with_metadata(
    record: dict[str, Any],
    parent_id: Optional[str] = None,
    extract_id: Optional[str] = None,
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    extra_fields: Optional[dict[str, Any]] = None,
    in_place: bool = False,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
) -> dict[str, Any]:
    """Annotate a record with metadata fields.

    Args:
        record: Record dictionary to annotate
        parent_id: Optional parent record ID
        extract_id: Extract ID (generated if None)
        extract_time: Extraction timestamp (current time if None)
        id_field: Field name for extract ID
        parent_field: Field name for parent ID
        time_field: Field name for timestamp
        extra_fields: Additional metadata fields to add
        in_place: Whether to modify the record in place
        source_field: Field name to use for deterministic ID generation
        id_generation_strategy: Custom function to generate ID

    Returns:
        Annotated record
    """
    # Create copy if not modifying in-place
    annotated = record if in_place else record.copy()

    # Add extract ID
    if extract_id is None:
        extract_id = generate_extract_id(
            record=annotated,
            source_field=source_field,
            id_generation_strategy=id_generation_strategy,
        )
    annotated[id_field] = extract_id

    # Add parent ID if provided
    if parent_id is not None:
        annotated[parent_field] = parent_id

    # Add extract timestamp
    if extract_time is None:
        extract_time = get_current_timestamp()
    annotated[time_field] = extract_time

    # Add extra fields
    if extra_fields:
        annotated.update(extra_fields)

    return annotated


def create_batch_metadata(
    batch_size: int,
    extract_time: Optional[Any] = None,
    parent_id: Optional[str] = None,
    records: Optional[list[dict[str, Any]]] = None,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
) -> dict[str, Union[str, Any]]:
    """Create metadata for a batch of records.

    Args:
        batch_size: Number of records in the batch
        extract_time: Extraction timestamp
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
        batch_id = generate_extract_id(
            record=first_record,
            source_field=source_field,
            id_generation_strategy=id_generation_strategy,
        )
    else:
        # Random UUID fallback
        batch_id = str(uuid.uuid4())

    # Use current timestamp if not provided
    if extract_time is None:
        extract_time = get_current_timestamp()

    # Construct metadata dictionary
    metadata = {
        "batch_id": batch_id,
        "batch_size": batch_size,
        "extract_time": extract_time,
    }

    # Include parent ID if available
    if parent_id:
        metadata["parent_id"] = parent_id

    return metadata
