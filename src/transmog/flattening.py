"""Core data flattening functionality.

This module handles all aspects of flattening nested JSON structures into
tabular format, including metadata annotation, array extraction, and
hierarchical relationship preservation.
"""

import functools
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from transmog.config import TransmogConfig
from transmog.exceptions import ProcessingError
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
            raise ProcessingError(
                f"Strategy 'natural' requires field '{id_field_name}' in record, "
                f"but it was not found. Available fields: {list(record.keys())}"
            )
        if record[id_field_name] is None or record[id_field_name] == "":
            raise ProcessingError(
                f"Strategy 'natural' requires non-empty '{id_field_name}', "
                f"but found: {record[id_field_name]!r}"
            )
        return None
    else:
        raise ProcessingError(f"Invalid id_generation strategy: {strategy}")


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


def _is_simple_array(array: list) -> bool:
    """Check if array contains only primitive values.

    Args:
        array: The array to check

    Returns:
        True if array contains only primitives (str, int, float, bool, None)
    """
    if not array:
        return True

    for item in array:
        if isinstance(item, (dict, list, tuple)):
            return False
    return True


def flatten_json(
    data: dict[str, Any],
    config: TransmogConfig,
    _context: ProcessingContext | None = None,
) -> dict[str, Any]:
    """Flatten nested JSON structure into a flat dictionary.

    Args:
        data: JSON data to flatten
        config: Configuration settings

    Returns:
        Flattened JSON data
    """
    if data is None:
        return {}

    if _context is None:
        _context = ProcessingContext()

    result: dict[str, Any] = {}

    if _context.current_depth >= config.max_depth:
        return result

    for key, value in data.items():
        if len(key) >= 2 and key[0] == "_" and key[1] == "_":
            result[key] = value
            continue

        is_dict = isinstance(value, dict)
        is_list = isinstance(value, list)

        if (is_dict or is_list) and not value:
            continue

        nested_context = _context.descend(key)
        current_path = nested_context.build_path("_")

        if is_dict:
            flattened = flatten_json(
                value,
                config,
                nested_context,
            )

            for flattened_key, flattened_value in flattened.items():
                result[flattened_key] = flattened_value

        elif is_list:
            if config.array_mode == ArrayMode.SKIP:
                continue
            elif config.array_mode == ArrayMode.SMART:
                if _is_simple_array(value):
                    result[current_path] = value
            elif config.array_mode == ArrayMode.INLINE:
                result[current_path] = value

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

    return result


# ============================================================================
# Array Extraction
# ============================================================================


@functools.lru_cache(maxsize=1024)
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


def extract_arrays(
    data: JsonDict,
    config: TransmogConfig,
    _context: ProcessingContext,
    parent_id: str | None = None,
    entity_name: str = "",
) -> dict[str, list[dict[str, Any]]]:
    """Extract nested arrays into flattened tables.

    Processes nested JSON data and extracts arrays into separate tables.
    Parent-child relationships are maintained with ID references.

    Args:
        data: JSON data to process
        config: Configuration settings
        parent_id: UUID of parent record
        entity_name: Entity name

    Returns:
        Dictionary mapping table names to lists of records
    """
    result: dict[str, list[dict[str, Any]]] = {}

    if data is None or _context.current_depth >= config.max_depth:
        return result

    for key, value in data.items():
        if len(key) >= 2 and key[0] == "_" and key[1] == "_":
            continue

        is_list = isinstance(value, list)
        is_dict = isinstance(value, dict)

        if not value and (is_list or is_dict):
            continue

        if is_list:
            if config.array_mode == ArrayMode.SMART and _is_simple_array(value):
                continue

            nested_context = _context.descend(key)

            sanitized_key = _sanitize_name(key)
            sanitized_entity = _sanitize_name(entity_name) if entity_name else ""

            table_name = _get_table_name(
                entity_name=sanitized_entity,
                array_name=sanitized_key,
                parent_path=_context.build_path("_"),
            )

            for item in value:
                if item is None and not config.include_nulls:
                    continue

                if isinstance(item, dict) and not item:
                    continue

                if isinstance(item, dict):
                    item_context = ProcessingContext(
                        current_depth=0,
                        path_components=[],
                        extract_time=_context.extract_time,
                    )
                    flattened = flatten_json(item, config, item_context)
                    metadata_dict = flattened if flattened is not None else {}
                else:
                    metadata_dict = {"value": item}

                annotated = annotate_with_metadata(
                    metadata_dict,
                    config=config,
                    parent_id=parent_id,
                    transmog_time=_context.extract_time,
                )

                if table_name not in result:
                    result[table_name] = []
                result[table_name].append(annotated)

                if isinstance(item, dict):
                    parent_id_value = annotated.get(config.id_field)
                    nested_arrays = extract_arrays(
                        item,
                        config,
                        nested_context,
                        parent_id_value,
                        entity_name,
                    )
                    for nested_table_name, nested_records in nested_arrays.items():
                        if nested_table_name not in result:
                            result[nested_table_name] = []
                        result[nested_table_name].extend(nested_records)

        elif is_dict:
            nested_arrays = extract_arrays(
                value,
                config,
                _context.descend(key),
                parent_id,
                entity_name,
            )
            for nested_table_name, nested_records in nested_arrays.items():
                if nested_table_name not in result:
                    result[nested_table_name] = []
                result[nested_table_name].extend(nested_records)

    return result


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
        parent_id: Parent record ID

    Returns:
        Tuple of (flattened_data, child_arrays)
    """
    if not data:
        return {}, {}

    flattened = flatten_json(data, config, _context)

    annotated = annotate_with_metadata(
        flattened,
        config=config,
        parent_id=parent_id,
        transmog_time=_context.extract_time,
    )

    record_id = annotated.get(config.id_field)

    if config.array_mode not in (ArrayMode.SEPARATE, ArrayMode.SMART):
        return annotated, {}

    arrays_result = extract_arrays(
        data,
        config,
        _context,
        parent_id=record_id,
        entity_name=entity_name,
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
    "flatten_json",
    "extract_arrays",
    "process_record_batch",
]
