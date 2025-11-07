"""Array extraction module for nested JSON structures.

This module extracts nested arrays from JSON structures and creates
child tables with appropriate parent-child relationships.
"""

from collections.abc import Generator
from typing import Any, Optional

from transmog.config import TransmogConfig
from transmog.core.flattener import _is_simple_array, flatten_json
from transmog.core.id_discovery import get_record_id
from transmog.core.metadata import annotate_with_metadata
from transmog.error import (
    logger,
)
from transmog.naming.conventions import sanitize_name
from transmog.naming.utils import get_table_name_for_array
from transmog.types import ArrayMode, JsonDict, ProcessingContext

ExtractResult = dict[str, list[dict[str, Any]]]
StreamExtractResult = Generator[tuple[str, dict[str, Any]], None, None]


def _extract_arrays_impl(
    data: JsonDict,
    config: TransmogConfig,
    context: ProcessingContext,
    parent_id: Optional[str],
    entity_name: str,
    streaming_mode: bool,
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    """Implementation helper for array extraction.

    This shared implementation is used by both extract_arrays and stream_extract_arrays.
    In streaming mode, it yields records individually. In batch mode, the caller
    collects records into tables.

    Args:
        data: JSON data to process
        config: Configuration settings
        context: Processing context with runtime state
        parent_id: UUID of parent record
        entity_name: Entity name
        streaming_mode: Whether to operate in streaming mode

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

            nested_context = context.descend(key, config.nested_threshold)

            sanitized_key = sanitize_name(key, config.separator)
            sanitized_entity = sanitize_name(entity_name) if entity_name else ""

            table_name = get_table_name_for_array(
                entity_name=sanitized_entity,
                array_name=sanitized_key,
                parent_path=context.build_path(config.separator),
                separator=config.separator,
                nested_threshold=config.nested_threshold,
            )

            for _i, item in enumerate(value):
                if item is None and config.skip_null:
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
                    path=table_name,
                    in_place=True,
                )

                yield table_name, annotated

                if isinstance(item, dict):
                    parent_id_field, parent_id_value = get_record_id(
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
                        streaming_mode,
                    )

                    yield from nested_generator

        elif is_dict:
            nested_generator = _extract_arrays_impl(
                value,
                config,
                context.descend(key, config.nested_threshold),
                parent_id,
                entity_name,
                streaming_mode,
            )

            yield from nested_generator


def extract_arrays(
    data: JsonDict,
    config: TransmogConfig,
    context: ProcessingContext,
    parent_id: Optional[str] = None,
    entity_name: str = "",
) -> ExtractResult:
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
    result: ExtractResult = {}

    for table_name, record in _extract_arrays_impl(
        data,
        config,
        context,
        parent_id,
        entity_name,
        streaming_mode=False,
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
) -> StreamExtractResult:
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
        streaming_mode=True,
    )
