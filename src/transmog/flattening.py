"""Core data flattening functionality.

This module handles all aspects of flattening nested JSON structures into
tabular format, including metadata annotation, array extraction, and
hierarchical relationship preservation.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from transmog.config import TransmogConfig
from transmog.exceptions import ValidationError
from transmog.types import ArrayMode, JsonDict, ProcessingContext

# Namespace UUID for deterministic ID generation
TRANSMOG_NAMESPACE = uuid.UUID("a9b8c7d6-e5f4-1234-abcd-0123456789ab")


# ============================================================================
# Metadata and ID Generation
# ============================================================================


def _hash_value(value: Any) -> str:
    """Generate a deterministic UUID5 from a value.

    Args:
        value: Value to hash

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


def _hash_fields(record: dict[str, Any], field_names: list[str]) -> str:
    """Generate deterministic ID from specific fields.

    Args:
        record: Record dictionary
        field_names: List of field names to use for composite ID

    Returns:
        UUID5 string
    """
    composite_values = []
    for field in field_names:
        if field in record:
            composite_values.append((field, record[field]))
        else:
            composite_values.append((field, None))

    composite_dict = dict(composite_values)
    return _hash_value(composite_dict)


def generate_transmog_id(
    record: dict[str, Any],
    strategy: str | list[str],
    id_field_name: str,
) -> str | None:
    """Generate or discover ID for a record based on strategy.

    Args:
        record: The record to process
        strategy: ID generation strategy
        id_field_name: Name of the ID field to check/use

    Returns:
        ID string, or None if using natural ID that exists

    Raises:
        ProcessingError: If strategy is "natural" but field doesn't exist
    """
    if isinstance(strategy, list):
        return _hash_fields(record, strategy)

    if strategy == "random":
        return str(uuid.uuid4())
    elif strategy == "hash":
        return _hash_value(record)
    elif strategy == "natural":
        if id_field_name not in record:
            raise ValidationError(
                f"Strategy 'natural' requires field '{id_field_name}' in record, "
                f"but it was not found. Available fields: {list(record.keys())}"
            )
        if record[id_field_name] is None or record[id_field_name] == "":
            raise ValidationError(
                f"Strategy 'natural' requires non-empty '{id_field_name}', "
                f"but found: {record[id_field_name]!r}"
            )
        return None
    else:
        raise ValidationError(f"Invalid id_generation strategy: {strategy}")


def get_current_timestamp() -> str:
    """Get current timestamp in UTC as ISO format string.

    Returns:
        Formatted timestamp string
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def annotate_with_metadata(
    record: dict[str, Any],
    config: Any,
    parent_id: str | None = None,
    transmog_time: str | None = None,
    record_id: str | None = None,
) -> dict[str, Any]:
    """Annotate a record with metadata fields.

    Modifies the record in place and returns it.

    Args:
        record: Record dictionary to annotate
        config: Configuration object
        parent_id: Optional parent record ID
        transmog_time: Transmog timestamp (current time if None)
        record_id: Pre-generated record ID (if None, generates new one)

    Returns:
        Annotated record
    """
    if record_id is not None:
        record[config.id_field] = record_id
    else:
        generated_id = generate_transmog_id(
            record=record,
            strategy=config.id_generation,
            id_field_name=config.id_field,
        )
        if generated_id is not None:
            record[config.id_field] = generated_id

    if parent_id is not None:
        record[config.parent_field] = parent_id

    if config.time_field:
        if transmog_time is None:
            transmog_time = get_current_timestamp()
        record[config.time_field] = transmog_time

    return record


# ============================================================================
# Core Flattening
# ============================================================================


def _process_array_items(
    array: list,
    key: str,
    config: TransmogConfig,
    _context: ProcessingContext,
    _collect_arrays: bool,
    _parent_id: str | None,
    _entity_name: str,
) -> tuple[bool, dict[str, list[dict[str, Any]]]]:
    """Process array items in a single pass, determining simplicity while extracting.

    Args:
        array: The array to process
        key: The field name/key for this array
        config: Configuration settings
        _context: Processing context
        _collect_arrays: Whether to collect arrays into separate tables
        _parent_id: Parent record ID for child records
        _entity_name: Entity name for table naming

    Returns:
        Tuple of (is_simple: bool, child_arrays: dict)
    """
    if not array:
        return True, {}

    arrays: dict[str, list[dict[str, Any]]] = {}
    is_simple = True

    for item in array:  # Single iteration through the array
        if item is None and not config.include_nulls:
            continue

        if isinstance(item, dict):
            is_simple = False  # Array contains complex objects
            if not item:
                continue

            # Process complex item
            item_context = ProcessingContext(
                current_depth=_context.current_depth + 1,
                path_components=[],
                extract_time=_context.extract_time,
            )
            flattened, item_arrays = flatten_json(
                item,
                config,
                item_context,
                _collect_arrays=True,
                _parent_id=_parent_id,
                _entity_name=_entity_name,
            )
            metadata_dict = flattened
        else:
            # Simple primitive value
            metadata_dict = {"value": item}
            item_arrays = {}

        if _collect_arrays:
            # Create child record
            if (
                config.id_generation == "natural"
                and config.id_field not in metadata_dict
            ):
                metadata_dict[config.id_field] = str(uuid.uuid4())
            annotated = annotate_with_metadata(
                metadata_dict,
                config=config,
                parent_id=_parent_id,
                transmog_time=_context.extract_time,
            )

            # Determine table name using the actual field key
            sanitized_key = _sanitize_name(key)
            sanitized_entity = _sanitize_name(_entity_name) if _entity_name else ""
            parent_path = "_".join(_context.path_components)
            table_name = _get_table_name(sanitized_entity, sanitized_key, parent_path)

            arrays.setdefault(table_name, []).append(annotated)

            if isinstance(item, dict):
                for nested_table_name, nested_records in item_arrays.items():
                    arrays.setdefault(nested_table_name, []).extend(nested_records)

    return is_simple, arrays


def flatten_json(
    data: dict[str, Any],
    config: TransmogConfig,
    _context: ProcessingContext | None = None,
    _collect_arrays: bool = False,
    _parent_id: str | None = None,
    _entity_name: str = "",
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Flatten nested JSON structure and optionally extract arrays in a single pass.

    Args:
        data: JSON data to flatten
        config: Configuration settings
        _context: Processing context
        _collect_arrays: Whether to collect arrays into separate tables
        _parent_id: Parent record ID for array extraction
        _entity_name: Entity name for table naming

    Returns:
        Tuple of (flattened_data, child_arrays)
    """
    if data is None:
        return {}, {}

    if _context is None:
        _context = ProcessingContext()

    result: dict[str, Any] = {}
    arrays: dict[str, list[dict[str, Any]]] = {}

    if _context.current_depth >= config.max_depth:
        return result, arrays

    for key, value in data.items():
        is_dict = isinstance(value, dict)
        is_list = isinstance(value, list)

        if (is_dict or is_list) and not value:
            continue

        nested_context = ProcessingContext(
            current_depth=_context.current_depth + 1,
            path_components=_context.path_components + [key],
            extract_time=_context.extract_time,
        )
        current_path = "_".join(nested_context.path_components)

        if is_dict:
            flattened, nested_arrays = flatten_json(
                value,
                config,
                nested_context,
                _collect_arrays,
                _parent_id,
                _entity_name,
            )

            result.update(flattened)

            if _collect_arrays:
                for nested_table_name, nested_records in nested_arrays.items():
                    arrays.setdefault(nested_table_name, []).extend(nested_records)

        elif is_list:
            if config.array_mode == ArrayMode.SKIP:
                continue
            elif config.array_mode == ArrayMode.INLINE:
                result[current_path] = json.dumps(value, ensure_ascii=False)
            elif config.array_mode == ArrayMode.SMART:
                is_simple, array_items = _process_array_items(
                    value,
                    key,
                    config,
                    _context,
                    _collect_arrays,
                    _parent_id,
                    _entity_name,
                )

                if is_simple:
                    result[current_path] = value
                elif _collect_arrays:
                    for table_name, table_records in array_items.items():
                        arrays.setdefault(table_name, []).extend(table_records)
            elif config.array_mode == ArrayMode.SEPARATE:
                if _collect_arrays:
                    _, array_items = _process_array_items(
                        value,
                        key,
                        config,
                        _context,
                        _collect_arrays,
                        _parent_id,
                        _entity_name,
                    )

                    for table_name, table_records in array_items.items():
                        arrays.setdefault(table_name, []).extend(table_records)
            else:
                raise ValueError(
                    f"Unhandled ArrayMode: {config.array_mode}. "
                    f"Valid modes: {[mode.value for mode in ArrayMode]}"
                )

        else:
            if value is not None and value != "":
                if _context.path_components:
                    result[current_path] = value
                else:
                    result[key] = value
            elif config.include_nulls:
                if _context.path_components:
                    result[current_path] = ""
                else:
                    result[key] = ""

    return result, arrays


# ============================================================================
# Array Extraction Helpers
# ============================================================================


def _sanitize_name(name: str) -> str:
    """Sanitize names for SQL compatibility.

    Args:
        name: Name to sanitize

    Returns:
        Sanitized name
    """
    sanitized = name.replace(" ", "_").replace("-", "_")

    result = []
    last_underscore = False

    for char in sanitized:
        if char.isalnum() or char == "_":
            result.append(char)
            last_underscore = char == "_"
        elif not last_underscore:
            result.append("_")
            last_underscore = True

    sanitized = "".join(result).strip("_")

    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"

    return sanitized or "unnamed_field"


def _get_table_name(entity_name: str, array_name: str, parent_path: str) -> str:
    """Generate table names for arrays.

    Args:
        entity_name: Entity name
        array_name: Array field name
        parent_path: Parent path

    Returns:
        Generated table name
    """
    if not parent_path:
        return f"{entity_name}_{array_name}"
    return f"{entity_name}_{parent_path}_{array_name}"


# ============================================================================
# Hierarchy Processing
# ============================================================================


def _process_structure(
    data: JsonDict,
    entity_name: str,
    config: TransmogConfig,
    _context: ProcessingContext,
    parent_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Process JSON structure with parent-child relationship preservation.

    Args:
        data: JSON data to process
        entity_name: Name of the entity
        config: Configuration settings
        _context: Processing context
        parent_id: Parent record ID

    Returns:
        Tuple of (flattened_data, child_arrays)
    """
    if not data:
        return {}, {}

    if config.array_mode == ArrayMode.SEPARATE:
        collect_arrays = True
    elif config.array_mode == ArrayMode.SMART:
        collect_arrays = True
    elif config.array_mode == ArrayMode.INLINE:
        collect_arrays = False
    elif config.array_mode == ArrayMode.SKIP:
        collect_arrays = False
    else:
        raise ValueError(
            f"Unhandled ArrayMode: {config.array_mode}. "
            f"Valid modes: {[mode.value for mode in ArrayMode]}"
        )

    generated_id = generate_transmog_id(
        record=data,
        strategy=config.id_generation,
        id_field_name=config.id_field,
    )
    if generated_id is None and config.id_generation == "natural":
        current_record_id = data.get(config.id_field)
    else:
        current_record_id = generated_id

    # Single pass through data with parent ID already known
    flattened, arrays_result = flatten_json(
        data,
        config,
        _context,
        _collect_arrays=collect_arrays,
        _parent_id=current_record_id,
        _entity_name=entity_name,
    )

    # Apply metadata with pre-generated ID
    annotated = annotate_with_metadata(
        flattened,
        config=config,
        parent_id=parent_id,
        transmog_time=_context.extract_time,
        record_id=current_record_id,
    )

    return annotated, arrays_result


def process_record_batch(
    records: list[JsonDict],
    entity_name: str,
    config: TransmogConfig,
    _context: ProcessingContext,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Process a batch of records.

    Args:
        records: List of records to process
        entity_name: Entity name
        config: Configuration settings
        _context: Processing context

    Returns:
        Tuple of (flattened_records, child_arrays)
    """
    flattened_records = []
    all_child_arrays: dict[str, list[dict[str, Any]]] = {}

    for record in records:
        flattened, child_arrays = _process_structure(
            record,
            entity_name,
            config,
            _context,
        )

        if flattened:
            flattened_records.append(flattened)

        for table_name, table_records in child_arrays.items():
            if table_name not in all_child_arrays:
                all_child_arrays[table_name] = []
            all_child_arrays[table_name].extend(table_records)

    return flattened_records, all_child_arrays


__all__ = [
    "get_current_timestamp",
    "annotate_with_metadata",
    "process_record_batch",
]
