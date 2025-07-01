"""Array extraction module for nested JSON structures.

This module extracts nested arrays from JSON structures and
creates child tables with appropriate parent-child relationships.
"""

from collections.abc import Generator
from datetime import datetime
from typing import Any, Callable, Optional, Union

from transmog.core.flattener import flatten_json
from transmog.core.id_discovery import get_record_id
from transmog.core.metadata import annotate_with_metadata
from transmog.error import (
    ProcessingError,
    build_error_context,
    format_error_message,
    get_recovery_strategy,
    logger,
)
from transmog.naming.conventions import sanitize_name
from transmog.naming.utils import get_table_name_for_array
from transmog.types.base import JsonDict

# Type aliases for improved code readability
JsonList = list[dict[str, Any]]
ExtractResult = dict[str, list[dict[str, Any]]]
StreamExtractResult = Generator[tuple[str, dict[str, Any]], None, None]


def _extract_arrays_impl(
    data: JsonDict,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "",
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    transmog_time: Optional[Any] = None,
    id_field: str = "__transmog_id",
    parent_field: str = "__parent_transmog_id",
    time_field: str = "__transmog_datetime",
    nested_threshold: int = 4,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
    streaming_mode: bool = False,
    id_field_patterns: Optional[list[str]] = None,
    id_field_mapping: Optional[dict[str, str]] = None,
    force_transmog_id: bool = False,
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    """Implementation helper for array extraction.

    This is the shared implementation used by both extract_arrays and
    stream_extract_arrays.
    In streaming mode, it yields records individually. In batch mode,
    the caller collects the records into tables.

    Args:
        data: JSON data to process
        parent_id: UUID of parent record
        parent_path: Path from root
        entity_name: Entity name
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        transmog_time: Transmog timestamp
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        nested_threshold: Threshold for deep nesting (default 4)
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Depth in the recursion
        streaming_mode: Whether to operate in streaming mode
        id_field_patterns: List of field names to check for natural IDs
        id_field_mapping: Optional mapping of paths to specific ID fields
        force_transmog_id: If True, always add transmog ID

    Yields:
        Tuples of (table_name, record) for each extracted record
    """
    # Return early if no data to process
    if data is None:
        return

    # Prevent excessive recursion
    if max_depth is not None and current_depth >= max_depth:
        logger.warning(
            f"Maximum recursion depth ({max_depth}) reached at path: {parent_path}"
        )
        return

    # Process each key in the data
    for key, value in data.items():
        # Skip internal metadata fields
        if key.startswith("__"):
            continue

        # Build the path for this field
        current_path = f"{parent_path}{separator}{key}" if parent_path else key

        # Skip empty arrays and dictionaries
        if (isinstance(value, list) and not value) or (
            isinstance(value, dict) and not value
        ):
            continue

        # Array processing
        if isinstance(value, list) and value:
            # Generate table name with proper sanitization
            sanitized_key = sanitize_name(key, separator)
            sanitized_entity = sanitize_name(entity_name) if entity_name else ""

            # Generate table name using utility function
            table_name = get_table_name_for_array(
                entity_name=sanitized_entity,
                array_name=sanitized_key,
                parent_path=parent_path,
                separator=separator,
                nested_threshold=nested_threshold,
            )

            # Process each item in the array
            for i, item in enumerate(value):
                # Skip null array items if configured to skip nulls
                if item is None and skip_null:
                    continue

                # Skip empty dictionary items
                if isinstance(item, dict) and not item:
                    continue

                try:
                    # Create path for this specific array item without index
                    item_path = current_path

                    # Determine ID source field based on configuration
                    source_field = None
                    if default_id_field:
                        if isinstance(default_id_field, str):
                            # Single field for all paths
                            source_field = default_id_field
                        else:
                            # Path-specific field mapping
                            # Try with current_path first since it doesn't have
                            # the index
                            if current_path in default_id_field:
                                source_field = default_id_field[current_path]
                            # Try wildcard match
                            elif "*" in default_id_field:
                                source_field = default_id_field["*"]
                            # Try table name
                            elif table_name in default_id_field:
                                source_field = default_id_field[table_name]
                            # Try root path (empty string) as fallback
                            elif "" in default_id_field:
                                source_field = default_id_field[""]

                    if isinstance(item, dict):
                        # Process dictionary item
                        # Flatten the JSON structure
                        flattened = flatten_json(
                            item,
                            separator=separator,
                            cast_to_string=cast_to_string,
                            include_empty=include_empty,
                            skip_null=skip_null,
                            parent_path="",
                            in_place=False,
                            nested_threshold=nested_threshold,
                            mode="streaming" if streaming_mode else "standard",
                            recovery_strategy=recovery_strategy,
                        )

                        # Add metadata fields to the record
                        metadata_dict = flattened if flattened is not None else {}
                    else:
                        # Primitive array value storage
                        metadata_dict = {"value": item}

                    # Add metadata to the dictionary
                    annotated = annotate_with_metadata(
                        metadata_dict,
                        parent_id=parent_id,
                        transmog_time=transmog_time,
                        id_field=id_field,
                        parent_field=parent_field,
                        time_field=time_field,
                        in_place=True,
                        source_field=source_field,
                        id_generation_strategy=id_generation_strategy,
                        id_field_patterns=id_field_patterns,
                        path=table_name,
                        id_field_mapping=id_field_mapping,
                        force_transmog_id=force_transmog_id,
                    )

                    # Track array source information
                    annotated["__array_field"] = sanitized_key
                    annotated["__array_index"] = i

                    # Output the processed record
                    yield table_name, annotated

                    # Process any nested arrays recursively, but only for dict items
                    if isinstance(item, dict):
                        # Get the parent ID - use natural ID if available
                        parent_id_field, parent_id_value = get_record_id(
                            annotated,
                            id_field_patterns=id_field_patterns,
                            path=table_name,
                            id_field_mapping=id_field_mapping,
                            fallback_field=id_field,
                        )
                        nested_generator = _extract_arrays_impl(
                            item,
                            parent_id=parent_id_value,
                            parent_path=item_path,
                            entity_name=entity_name,
                            separator=separator,
                            cast_to_string=cast_to_string,
                            include_empty=include_empty,
                            skip_null=skip_null,
                            transmog_time=transmog_time,
                            id_field=id_field,
                            parent_field=parent_field,
                            time_field=time_field,
                            nested_threshold=nested_threshold,
                            default_id_field=default_id_field,
                            id_generation_strategy=id_generation_strategy,
                            recovery_strategy=recovery_strategy,
                            max_depth=max_depth,
                            current_depth=current_depth + 1,
                            streaming_mode=streaming_mode,
                            id_field_patterns=id_field_patterns,
                            id_field_mapping=id_field_mapping,
                            force_transmog_id=force_transmog_id,
                        )
                        # Process nested arrays one by one
                        yield from nested_generator
                except Exception as e:
                    # Handle errors using standardized recovery strategy
                    strategy = get_recovery_strategy(recovery_strategy)
                    context = build_error_context(
                        entity_name=f"{sanitized_key}[{i}]",
                        entity_type="array item",
                        operation="array extraction",
                        source=table_name,
                        array_field=sanitized_key,
                        array_index=i,
                    )

                    try:
                        recovery_result = strategy.recover(e, **context)
                        if recovery_result is not None:
                            # Create error record from recovery result
                            if isinstance(recovery_result, dict):
                                error_record = recovery_result.copy()
                            else:
                                error_record = {"__error": str(recovery_result)}

                            # Add array context
                            error_record.update(
                                {
                                    "__array_field": sanitized_key,
                                    "__array_index": i,
                                }
                            )

                            # Add metadata
                            annotated = annotate_with_metadata(
                                error_record,
                                parent_id=parent_id,
                                transmog_time=transmog_time,
                                id_field=id_field,
                                parent_field=parent_field,
                                time_field=time_field,
                                in_place=True,
                                id_field_patterns=id_field_patterns,
                                path=f"{table_name}{separator}errors",
                                id_field_mapping=id_field_mapping,
                                force_transmog_id=force_transmog_id,
                            )
                            # Output the error record
                            yield f"{table_name}{separator}errors", annotated
                        # Continue processing other items
                        continue
                    except Exception:
                        # Re-raise with formatted message
                        error_msg = format_error_message(
                            "array_processing", e, **context
                        )
                        raise ProcessingError(error_msg) from e

        # Nested object processing (non-array)
        elif isinstance(value, dict):
            # Skip empty dictionaries
            if not value:
                continue

            # Recursively process nested objects for arrays inside them
            nested_generator = _extract_arrays_impl(
                value,
                parent_id=parent_id,
                parent_path=current_path,
                entity_name=entity_name,
                separator=separator,
                cast_to_string=cast_to_string,
                include_empty=include_empty,
                skip_null=skip_null,
                transmog_time=transmog_time,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                nested_threshold=nested_threshold,
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=recovery_strategy,
                max_depth=max_depth,
                current_depth=current_depth + 1,
                streaming_mode=streaming_mode,
                id_field_patterns=id_field_patterns,
                id_field_mapping=id_field_mapping,
                force_transmog_id=force_transmog_id,
            )
            # Process nested arrays one by one
            yield from nested_generator


def extract_arrays(
    data: JsonDict,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "",
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    transmog_time: Optional[Any] = None,
    id_field: str = "__transmog_id",
    parent_field: str = "__parent_transmog_id",
    time_field: str = "__transmog_datetime",
    nested_threshold: int = 4,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    streaming: bool = False,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
    id_field_patterns: Optional[list[str]] = None,
    id_field_mapping: Optional[dict[str, str]] = None,
    force_transmog_id: bool = False,
) -> ExtractResult:
    """Extract nested arrays into flattened tables.

    Processes nested JSON data and extracts arrays into separate tables.
    Parent-child relationships are maintained with ID references.

    Args:
        data: JSON data to process
        parent_id: UUID of parent record
        parent_path: Path from root
        entity_name: Entity name
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        transmog_time: Transmog timestamp
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        nested_threshold: Threshold for deep nesting (default 4)
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        streaming: Whether to use streaming mode (returns generator)
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Depth in the recursion
        id_field_patterns: List of field names to check for natural IDs
        id_field_mapping: Optional mapping of paths to specific ID fields
        force_transmog_id: If True, always add transmog ID

    Returns:
        Dictionary mapping table names to lists of records

    Notes:
        - Empty objects ({}) and empty arrays ([]) are skipped
        - Dictionary items that are empty are also skipped
        - Null values in arrays are skipped when skip_null is True
    """
    # Initialize transmog timestamp if not provided
    if transmog_time is None:
        transmog_time = datetime.now()

    # Collect records in streaming mode
    result: ExtractResult = {}
    for table_name, record in _extract_arrays_impl(
        data,
        parent_id=parent_id,
        parent_path=parent_path,
        entity_name=entity_name,
        separator=separator,
        cast_to_string=cast_to_string,
        include_empty=include_empty,
        skip_null=skip_null,
        transmog_time=transmog_time,
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
        nested_threshold=nested_threshold,
        default_id_field=default_id_field,
        id_generation_strategy=id_generation_strategy,
        recovery_strategy=recovery_strategy,
        max_depth=max_depth,
        current_depth=current_depth,
        streaming_mode=False,
        id_field_patterns=id_field_patterns,
        id_field_mapping=id_field_mapping,
        force_transmog_id=force_transmog_id,
    ):
        # Accumulate records by table
        if table_name not in result:
            result[table_name] = []
        result[table_name].append(record)

    return result


def stream_extract_arrays(
    data: JsonDict,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "",
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    transmog_time: Optional[Any] = None,
    id_field: str = "__transmog_id",
    parent_field: str = "__parent_transmog_id",
    time_field: str = "__transmog_datetime",
    nested_threshold: int = 4,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
    id_field_patterns: Optional[list[str]] = None,
    id_field_mapping: Optional[dict[str, str]] = None,
    force_transmog_id: bool = False,
) -> StreamExtractResult:
    """Extract nested arrays into a stream of records for memory-efficient processing.

    Similar to extract_arrays, but yields records individually instead of
    collecting them into tables. This is useful for streaming large datasets
    directly to output.

    Args:
        data: JSON data to process
        parent_id: UUID of parent record
        parent_path: Path from root
        entity_name: Entity name
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        transmog_time: Transmog timestamp
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        nested_threshold: Threshold for deep nesting (default 4)
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Depth in the recursion
        id_field_patterns: List of field names to check for natural IDs
        id_field_mapping: Optional mapping of paths to specific ID fields
        force_transmog_id: If True, always add transmog ID

    Yields:
        Tuples of (table_name, record) for each extracted record

    Notes:
        - Empty objects ({}) and empty arrays ([]) are skipped
        - Dictionary items that are empty are also skipped
        - Null values in arrays are skipped when skip_null is True
    """
    # Initialize transmog timestamp if not provided
    if transmog_time is None:
        transmog_time = datetime.now()

    # Process all records from the implementation and yield each record individually
    yield from _extract_arrays_impl(
        data,
        parent_id=parent_id,
        parent_path=parent_path,
        entity_name=entity_name,
        separator=separator,
        cast_to_string=cast_to_string,
        include_empty=include_empty,
        skip_null=skip_null,
        transmog_time=transmog_time,
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
        nested_threshold=nested_threshold,
        default_id_field=default_id_field,
        id_generation_strategy=id_generation_strategy,
        recovery_strategy=recovery_strategy,
        max_depth=max_depth,
        current_depth=current_depth,
        streaming_mode=True,
        id_field_patterns=id_field_patterns,
        id_field_mapping=id_field_mapping,
        force_transmog_id=force_transmog_id,
    )
