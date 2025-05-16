"""Array extraction module for nested JSON structures.

This module extracts nested arrays from JSON structures and
creates child tables with appropriate parent-child relationships.
"""

from collections.abc import Generator
from datetime import datetime
from typing import Any, Callable, Optional, Union

from transmog.core.flattener import flatten_json
from transmog.core.metadata import annotate_with_metadata, generate_deterministic_id
from transmog.error import logger
from transmog.naming.abbreviator import abbreviate_table_name
from transmog.naming.conventions import get_table_name, sanitize_name

# Type aliases
JsonDict = dict[str, Any]
JsonList = list[dict[str, Any]]
ArrayDict = dict[str, JsonList]
ExtractResult = dict[str, list[dict[str, Any]]]


def _extract_arrays_impl(
    data: JsonDict,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "",
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    abbreviate_enabled: bool = True,
    max_component_length: Optional[int] = None,
    preserve_leaf: bool = True,
    custom_abbreviations: Optional[dict[str, str]] = None,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
    streaming_mode: bool = False,
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
        extract_time: Extraction timestamp
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        abbreviate_enabled: Whether to abbreviate table names
        max_component_length: Maximum length for table name components
        preserve_leaf: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Current depth in the recursion
        streaming_mode: Whether to operate in streaming mode

    Yields:
        Tuples of (table_name, record) for each extracted record
    """
    # Handle null/missing data - nothing to extract
    if data is None:
        return

    # Check if we've reached the maximum depth
    if max_depth is not None and current_depth >= max_depth:
        logger.warning(
            f"Maximum recursion depth ({max_depth}) reached at path: {parent_path}"
        )
        return

    # Process each key in the data
    for key, value in data.items():
        # Skip metadata fields - these are used internally
        if key.startswith("__"):
            continue

        # Create path for this field
        current_path = f"{parent_path}{separator}{key}" if parent_path else key

        # Handle arrays
        if isinstance(value, list) and value:
            # Determine the array's table name
            sanitized_key = sanitize_name(key, separator)
            sanitized_entity = sanitize_name(entity_name) if entity_name else ""
            raw_table_name = get_table_name(sanitized_key, sanitized_entity, separator)

            # Abbreviate table name if enabled
            if abbreviate_enabled:
                table_name = abbreviate_table_name(
                    raw_table_name,
                    parent_entity=sanitized_entity,
                    separator=separator,
                    max_component_length=max_component_length,
                    preserve_leaf=preserve_leaf,
                    abbreviation_dict=custom_abbreviations,
                )
            else:
                table_name = raw_table_name

            # Skip empty arrays - nothing to extract
            if not value:
                continue

            # Process each array item
            for i, item in enumerate(value):
                # Skip null items
                if item is None:
                    continue

                # Skip non-dicts if child items are not dicts
                # unless we want scalars in their own table
                if not isinstance(item, dict):
                    # For now, we skip non-dict items
                    continue

                # For dicts, we need to process recursively
                try:
                    # Create unique path for this array item
                    item_path = f"{current_path}{separator}{i}"

                    # Determine source field for ID generation
                    source_field = None
                    if default_id_field:
                        if isinstance(default_id_field, str):
                            # Simple case - same field for all paths
                            source_field = default_id_field
                        else:
                            # Dict case - need to check for specific path matches
                            # Exact path match
                            if current_path in default_id_field:
                                source_field = default_id_field[current_path]
                            # Wildcard match
                            elif "*" in default_id_field:
                                source_field = default_id_field["*"]
                            # Table name match
                            elif table_name in default_id_field:
                                source_field = default_id_field[table_name]

                    # Flatten the item
                    flattened = flatten_json(
                        item,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        parent_path="",
                        in_place=False,
                        mode="streaming" if streaming_mode else "standard",
                        recovery_strategy=recovery_strategy,
                    )

                    # Add metadata (extract_id, parent_id, etc.)
                    metadata_dict = flattened if flattened is not None else {}
                    annotated = annotate_with_metadata(
                        metadata_dict,
                        parent_id=parent_id,
                        extract_time=extract_time,
                        id_field=id_field,
                        parent_field=parent_field,
                        time_field=time_field,
                        in_place=True,
                        source_field=source_field,
                        id_generation_strategy=id_generation_strategy,
                    )

                    # Add additional fields to identify source and relation
                    annotated["__array_field"] = sanitized_key
                    annotated["__array_index"] = i

                    # Yield the record
                    yield table_name, annotated

                    # Process nested arrays recursively with the same streaming mode
                    nested_generator = _extract_arrays_impl(
                        item,
                        parent_id=annotated.get(id_field),
                        parent_path=item_path,
                        entity_name=entity_name,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        extract_time=extract_time,
                        id_field=id_field,
                        parent_field=parent_field,
                        time_field=time_field,
                        abbreviate_enabled=abbreviate_enabled,
                        max_component_length=max_component_length,
                        preserve_leaf=preserve_leaf,
                        custom_abbreviations=custom_abbreviations,
                        default_id_field=default_id_field,
                        id_generation_strategy=id_generation_strategy,
                        recovery_strategy=recovery_strategy,
                        max_depth=max_depth,
                        current_depth=current_depth + 1,
                        streaming_mode=streaming_mode,
                    )

                    # Process all nested records
                    yield from nested_generator

                except Exception as e:
                    # Handle errors with recovery strategy if provided
                    if recovery_strategy and hasattr(recovery_strategy, "recover"):
                        try:
                            recovery_result = recovery_strategy.recover(
                                e,
                                entity_name=entity_name,
                                path=current_path.split(separator),
                            )
                            # If we got a valid result, add it as a placeholder
                            if recovery_result is not None and isinstance(
                                recovery_result, dict
                            ):
                                # Create minimal record to maintain structure
                                simple_record = {
                                    id_field: generate_deterministic_id(
                                        str(current_path) + str(i)
                                    ),
                                    parent_field: parent_id,
                                    time_field: extract_time or datetime.now(),
                                    "__array_field": sanitized_key,
                                    "__array_index": i,
                                    "__extract_status": "error_recovered",
                                    "__error_message": str(e),
                                }
                                yield table_name, simple_record
                                continue
                        except Exception as re:
                            # If recovery itself fails, log and
                            # continue with original error
                            logger.warning(f"Recovery failed: {re}")
                    # Rethrow the exception if no recovery or recovery failed
                    raise

        # Handle nested objects
        elif isinstance(value, dict):
            # Process nested arrays recursively
            try:
                # Recursively extract arrays from nested objects
                nested_generator = _extract_arrays_impl(
                    value,
                    parent_id=parent_id,
                    parent_path=current_path,
                    entity_name=entity_name,
                    separator=separator,
                    cast_to_string=cast_to_string,
                    include_empty=include_empty,
                    skip_null=skip_null,
                    extract_time=extract_time,
                    id_field=id_field,
                    parent_field=parent_field,
                    time_field=time_field,
                    abbreviate_enabled=abbreviate_enabled,
                    max_component_length=max_component_length,
                    preserve_leaf=preserve_leaf,
                    custom_abbreviations=custom_abbreviations,
                    default_id_field=default_id_field,
                    id_generation_strategy=id_generation_strategy,
                    recovery_strategy=recovery_strategy,
                    max_depth=max_depth,
                    current_depth=current_depth + 1,
                    streaming_mode=streaming_mode,
                )

                # Process all nested records
                yield from nested_generator

            except Exception as e:
                # Handle errors with recovery strategy if provided
                if recovery_strategy and hasattr(recovery_strategy, "recover"):
                    try:
                        recovery_result = recovery_strategy.recover(
                            e,
                            entity_name=entity_name,
                            path=current_path.split(separator),
                        )
                        # If recovery succeeded, continue with next field
                        if recovery_result is not None:
                            continue
                    except Exception as re:
                        # If recovery fails, log and continue with original error
                        logger.warning(f"Recovery failed: {re}")
                # Rethrow the exception if no recovery or recovery failed
                raise


def extract_arrays(
    data: JsonDict,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    entity_name: str = "",
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    abbreviate_enabled: bool = True,
    max_component_length: Optional[int] = None,
    preserve_leaf: bool = True,
    custom_abbreviations: Optional[dict[str, str]] = None,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    streaming: bool = False,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
) -> ExtractResult:
    """Extract all nested arrays from a data structure.

    This function identifies arrays within the given JSON structure and
    extracts them into separate tables, preserving parent-child relationships.

    Args:
        data: JSON data to process
        parent_id: UUID of parent record
        parent_path: Path from root
        entity_name: Entity name
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        extract_time: Extraction timestamp
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        abbreviate_enabled: Whether to abbreviate table names
        max_component_length: Maximum length for table name components
        preserve_leaf: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        streaming: Whether to stream results
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Current depth in the recursion

    Returns:
        Dictionary of array tables with their records
    """
    # Use the common implementation but collect the results into a dictionary
    result: ExtractResult = {}

    # Use the implementation helper with the appropriate streaming mode
    for table_name, record in _extract_arrays_impl(
        data,
        parent_id=parent_id,
        parent_path=parent_path,
        entity_name=entity_name,
        separator=separator,
        cast_to_string=cast_to_string,
        include_empty=include_empty,
        skip_null=skip_null,
        extract_time=extract_time,
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
        abbreviate_enabled=abbreviate_enabled,
        max_component_length=max_component_length,
        preserve_leaf=preserve_leaf,
        custom_abbreviations=custom_abbreviations,
        default_id_field=default_id_field,
        id_generation_strategy=id_generation_strategy,
        recovery_strategy=recovery_strategy,
        max_depth=max_depth,
        current_depth=current_depth,
        streaming_mode=streaming,
    ):
        # Initialize the table if it doesn't exist
        if table_name not in result:
            result[table_name] = []

        # Add the record to the appropriate table
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
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    abbreviate_enabled: bool = True,
    max_component_length: Optional[int] = None,
    preserve_leaf: bool = True,
    custom_abbreviations: Optional[dict[str, str]] = None,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
) -> Generator[tuple[str, JsonDict], None, None]:
    """Stream extract arrays for memory-efficient processing.

    Similar to extract_arrays but yields records one at a time
    rather than building the entire result in memory.

    Args:
        data: JSON data to process
        parent_id: UUID of parent record
        parent_path: Path from root
        entity_name: Entity name
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        extract_time: Extraction timestamp
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        abbreviate_enabled: Whether to abbreviate table names
        max_component_length: Maximum length for table name components
        preserve_leaf: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Current depth in the recursion

    Returns:
        Generator yielding tuples of (table_name, record)
    """
    # Simply use the implementation helper with streaming mode set to True
    yield from _extract_arrays_impl(
        data,
        parent_id=parent_id,
        parent_path=parent_path,
        entity_name=entity_name,
        separator=separator,
        cast_to_string=cast_to_string,
        include_empty=include_empty,
        skip_null=skip_null,
        extract_time=extract_time,
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
        abbreviate_enabled=abbreviate_enabled,
        max_component_length=max_component_length,
        preserve_leaf=preserve_leaf,
        custom_abbreviations=custom_abbreviations,
        default_id_field=default_id_field,
        id_generation_strategy=id_generation_strategy,
        recovery_strategy=recovery_strategy,
        max_depth=max_depth,
        current_depth=current_depth,
        streaming_mode=True,
    )
