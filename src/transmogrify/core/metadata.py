"""
Metadata handling module for data annotation.

Provides functions to generate extract IDs, timestamps,
and other metadata for record tracking and lineage.
"""

import functools
import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union, Callable


# Define a consistent namespace for deterministic IDs
TRANSMOGRIFY_NAMESPACE = uuid.UUID("a9b8c7d6-e5f4-1234-abcd-0123456789ab")


def generate_extract_id(
    record: Optional[Dict[str, Any]] = None,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> str:
    """
    Generate a unique ID for record tracking.

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
        except Exception as e:
            # Fall back to random UUID on error
            return str(uuid.uuid4())

    # Option 2: Deterministic field-based generation
    if record is not None and source_field is not None:
        try:
            field_value = record.get(source_field)
            if field_value:
                return generate_deterministic_id(field_value)
        except Exception:
            # Fall back to random UUID on error
            pass

    # Option 1: Random UUID (default)
    return str(uuid.uuid4())


def generate_deterministic_id(value: Any) -> str:
    """
    Generate a deterministic UUID5 from a value.

    Args:
        value: Value to generate deterministic ID from

    Returns:
        UUID5 string
    """
    # Convert value to string if it isn't already
    value_str = str(value)

    # Create UUID5 using the namespace and the value
    deterministic_uuid = uuid.uuid5(TRANSMOGRIFY_NAMESPACE, value_str)

    return str(deterministic_uuid)


def generate_composite_id(values: Dict[str, Any], fields: list) -> str:
    """
    Generate a deterministic ID from multiple fields.

    Args:
        values: Dictionary containing the field values
        fields: List of field names to include

    Returns:
        Deterministic UUID string
    """
    # Combine values into a single string
    combined = "|".join(str(values.get(field, "")) for field in fields)

    return generate_deterministic_id(combined)


@functools.lru_cache(maxsize=8)
def get_current_timestamp(
    format_string: Optional[str] = None, as_string: bool = True
) -> Any:
    """
    Get current timestamp in UTC with caching for improved performance.

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
        # Default to ISO format with microseconds
        return now.strftime("%Y-%m-%d %H:%M:%S.%f")

    return now


def annotate_with_metadata(
    record: Dict[str, Any],
    parent_id: Optional[str] = None,
    extract_id: Optional[str] = None,
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    extra_fields: Optional[Dict[str, Any]] = None,
    in_place: bool = False,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> Dict[str, Any]:
    """
    Annotate a record with metadata fields.

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
    # Make a copy only if needed
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

    # Add any extra fields
    if extra_fields:
        annotated.update(extra_fields)

    return annotated


def create_batch_metadata(
    batch_size: int,
    extract_time: Optional[Any] = None,
    parent_id: Optional[str] = None,
    records: Optional[List[Dict[str, Any]]] = None,
    source_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> Dict[str, Union[str, Any]]:
    """
    Create metadata for an entire batch of records efficiently.

    This is more efficient than calling annotate_with_metadata repeatedly
    for large batches as it pre-generates IDs and reuses the timestamp.

    Args:
        batch_size: Number of records in the batch
        extract_time: Extraction timestamp (current time if None)
        parent_id: Optional parent record ID
        records: Optional list of records for deterministic ID generation
        source_field: Field name to use for deterministic ID generation
        id_generation_strategy: Custom function to generate ID

    Returns:
        Dictionary with metadata arrays keyed by field name
    """
    # Set default timestamp if needed
    if extract_time is None:
        extract_time = get_current_timestamp()

    # Generate all extract IDs in one go
    extract_ids = []
    if records and (source_field or id_generation_strategy):
        for record in records:
            extract_ids.append(
                generate_extract_id(
                    record=record,
                    source_field=source_field,
                    id_generation_strategy=id_generation_strategy,
                )
            )
    else:
        extract_ids = [generate_extract_id() for _ in range(batch_size)]

    # Create metadata dictionary
    metadata = {
        "__extract_id": extract_ids,
        "__extract_datetime": [extract_time] * batch_size,
    }

    # Add parent ID if provided
    if parent_id is not None:
        metadata["__parent_extract_id"] = [parent_id] * batch_size

    return metadata
