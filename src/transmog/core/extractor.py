"""Array extraction module for nested JSON structures.

This module extracts nested arrays from JSON structures and creates
child tables with appropriate parent-child relationships.
"""

import functools
from collections.abc import Generator
from typing import Any, Optional

from transmog.config import TransmogConfig
from transmog.core.flattener import _is_simple_array, flatten_json
from transmog.core.id_discovery import get_record_id
from transmog.core.metadata import annotate_with_metadata
from transmog.error import (
    logger,
)
from transmog.types import ArrayMode, JsonDict, NullHandling, ProcessingContext


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


def _get_table_name(
    entity_name: str, array_name: str, parent_path: str, separator: str
) -> str:
    """Generate table names for arrays.

    Args:
        entity_name: Entity name
        array_name: Array field name
        parent_path: Parent path
        separator: Separator for path components

    Returns:
        Generated table name
    """
    if not parent_path:
        return f"{entity_name}{separator}{array_name}"
    return f"{entity_name}{separator}{parent_path}{separator}{array_name}"


def _extract_arrays_impl(
    data: JsonDict,
    config: TransmogConfig,
    context: ProcessingContext,
    parent_id: Optional[str],
    entity_name: str,
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    """Generate array extraction records.

    Yields records one at a time for both batch and streaming modes.
    The caller determines how to collect the yielded records.

    Args:
        data: JSON data to process
        config: Configuration settings
        context: Processing context with runtime state
        parent_id: UUID of parent record
        entity_name: Entity name

    Yields:
        Tuples of (table_name, record) for each extracted record
    """
    if data is None:
        return

    if context.current_depth >= config.max_depth:
        path = context.build_path(config.separator)
        logger.warning(
            f"Maximum recursion depth ({config.max_depth}) reached at path: {path}"
        )
        return

    for key, value in data.items():
        if len(key) >= 2 and key[0] == "_" and key[1] == "_":
            continue

        is_list = isinstance(value, list)
        is_dict = isinstance(value, dict)

        if (is_list or is_dict) and not value:
            continue

        if is_list:
            if config.array_mode == ArrayMode.SMART and _is_simple_array(value):
                continue

            nested_context = context.descend(key)

            sanitized_key = _sanitize_name(key)
            sanitized_entity = _sanitize_name(entity_name) if entity_name else ""

            table_name = _get_table_name(
                entity_name=sanitized_entity,
                array_name=sanitized_key,
                parent_path=context.build_path(config.separator),
                separator=config.separator,
            )

            for _i, item in enumerate(value):
                if item is None and config.null_handling == NullHandling.SKIP:
                    continue

                if isinstance(item, dict) and not item:
                    continue

                if isinstance(item, dict):
                    item_context = ProcessingContext(
                        current_depth=0,
                        path_components=[],
                        extract_time=context.extract_time,
                    )
                    flattened = flatten_json(item, config, item_context)
                    metadata_dict = flattened if flattened is not None else {}
                else:
                    metadata_dict = {"value": item}

                annotated = annotate_with_metadata(
                    metadata_dict,
                    config=config,
                    parent_id=parent_id,
                    transmog_time=context.extract_time,
                )

                yield table_name, annotated

                if isinstance(item, dict):
                    _, parent_id_value = get_record_id(
                        annotated,
                        id_patterns=config.id_patterns,
                        fallback_field=config.id_field,
                    )

                    nested_generator = _extract_arrays_impl(
                        item,
                        config,
                        nested_context,
                        parent_id_value,
                        entity_name,
                    )

                    yield from nested_generator

        elif is_dict:
            nested_generator = _extract_arrays_impl(
                value,
                config,
                context.descend(key),
                parent_id,
                entity_name,
            )

            yield from nested_generator


def extract_arrays(
    data: JsonDict,
    config: TransmogConfig,
    context: ProcessingContext,
    parent_id: Optional[str] = None,
    entity_name: str = "",
) -> dict[str, list[dict[str, Any]]]:
    """Extract nested arrays into flattened tables.

    Processes nested JSON data and extracts arrays into separate tables.
    Parent-child relationships are maintained with ID references.

    Args:
        data: JSON data to process
        config: Configuration settings
        context: Processing context with runtime state
        parent_id: UUID of parent record
        entity_name: Entity name

    Returns:
        Dictionary mapping table names to lists of records
    """
    result: dict[str, list[dict[str, Any]]] = {}

    for table_name, record in _extract_arrays_impl(
        data,
        config,
        context,
        parent_id,
        entity_name,
    ):
        if table_name not in result:
            result[table_name] = []
        result[table_name].append(record)

    return result


def stream_extract_arrays(
    data: JsonDict,
    config: TransmogConfig,
    context: ProcessingContext,
    parent_id: Optional[str] = None,
    entity_name: str = "",
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    """Extract nested arrays as a stream.

    Memory-efficient streaming version that yields records one at a time
    instead of collecting them into tables.

    Args:
        data: JSON data to process
        config: Configuration settings
        context: Processing context with runtime state
        parent_id: UUID of parent record
        entity_name: Entity name

    Yields:
        Tuples of (table_name, record) for each extracted record
    """
    yield from _extract_arrays_impl(
        data,
        config,
        context,
        parent_id,
        entity_name,
    )
