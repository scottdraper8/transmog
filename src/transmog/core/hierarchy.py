"""Hierarchy processing module for nested JSON structures.

Provides functions to process complete JSON structures with
parent-child relationship preservation.
"""

import logging
from collections.abc import Generator
from typing import Any, Optional, Union

from transmog.config import TransmogConfig
from transmog.core.extractor import extract_arrays, stream_extract_arrays
from transmog.core.flattener import flatten_json
from transmog.core.id_discovery import get_record_id
from transmog.core.metadata import annotate_with_metadata
from transmog.types import ArrayMode, JsonDict, ProcessingContext

StreamingChildTables = Generator[tuple[str, dict[str, Any]], None, None]

logger = logging.getLogger(__name__)


def _process_structure(
    data: JsonDict,
    entity_name: str,
    config: TransmogConfig,
    context: ProcessingContext,
    parent_id: Optional[str] = None,
    streaming: bool = False,
) -> tuple[
    dict[str, Any], Union[dict[str, list[dict[str, Any]]], StreamingChildTables]
]:
    """Process JSON structure with parent-child relationship preservation.

    Args:
        data: JSON data to process
        entity_name: Name of the entity
        config: Configuration settings
        context: Processing context
        parent_id: Parent record ID
        streaming: Whether to use streaming mode

    Returns:
        Tuple of (flattened_data, child_arrays)
    """
    if not data:
        return {}, stream_extract_arrays({}, config, context) if streaming else {}

    flattened = flatten_json(data, config, context)

    annotated = annotate_with_metadata(
        flattened,
        config=config,
        parent_id=parent_id,
        transmog_time=context.extract_time,
    )

    _, record_id = get_record_id(
        annotated,
        id_patterns=config.id_patterns,
        fallback_field=config.id_field,
    )

    if config.array_mode not in (ArrayMode.SEPARATE, ArrayMode.SMART):
        return annotated, stream_extract_arrays(
            {}, config, context
        ) if streaming else {}

    if streaming:
        arrays_result: Union[dict[str, list[dict[str, Any]]], StreamingChildTables] = (
            stream_extract_arrays(
                data,
                config,
                context,
                parent_id=record_id,
                entity_name=entity_name,
            )
        )
    else:
        arrays_result = extract_arrays(
            data,
            config,
            context,
            parent_id=record_id,
            entity_name=entity_name,
        )

    return annotated, arrays_result


def process_record_batch(
    records: list[JsonDict],
    entity_name: str,
    config: TransmogConfig,
    context: ProcessingContext,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Process a batch of records.

    Args:
        records: List of records to process
        entity_name: Entity name
        config: Configuration settings
        context: Processing context

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
            context,
            streaming=False,
        )

        if flattened:
            flattened_records.append(flattened)

        if isinstance(child_arrays, dict):
            for table_name, table_records in child_arrays.items():
                if table_name not in all_child_arrays:
                    all_child_arrays[table_name] = []
                all_child_arrays[table_name].extend(table_records)

    return flattened_records, all_child_arrays


def stream_process_records(
    records: list[JsonDict],
    entity_name: str,
    config: TransmogConfig,
    context: ProcessingContext,
) -> tuple[list[dict[str, Any]], StreamingChildTables]:
    """Process records in streaming mode.

    Args:
        records: List of records to process
        entity_name: Entity name for all records
        config: Configuration settings
        context: Processing context

    Returns:
        Tuple of (flattened_records, child_tables_generator)
    """
    flattened_records = []
    child_arrays_list = []

    for record in records:
        flattened, child_arrays = _process_structure(
            record,
            entity_name,
            config,
            context,
            streaming=True,
        )
        flattened_records.append(flattened)
        child_arrays_list.append(child_arrays)

    def child_tables_generator() -> StreamingChildTables:
        for child_arrays_gen in child_arrays_list:
            if isinstance(child_arrays_gen, dict):
                for table_name, records in child_arrays_gen.items():
                    for record in records:
                        yield table_name, record
            else:
                yield from child_arrays_gen

    return flattened_records, child_tables_generator()
