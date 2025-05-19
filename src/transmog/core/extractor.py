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
from transmog.naming.conventions import sanitize_name

# Type aliases for improved code readability
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

        # Build the current path for this field
        current_path = f"{parent_path}{separator}{key}" if parent_path else key

        # Array processing
        if isinstance(value, list) and value:
            # Generate table name with proper sanitization
            sanitized_key = sanitize_name(key, separator)
            sanitized_entity = sanitize_name(entity_name) if entity_name else ""

            # Generate table name directly based on path depth
            if not parent_path:
                # First level array: <entity>_<arrayname>
                table_name = f"{sanitized_entity}{separator}{sanitized_key}"
            else:
                # Determine the array path for nesting
                path_parts = current_path.split(separator)

                # For deeply nested arrays, only consider the relevant parts of the path
                # excluding the final array name (key)
                if len(path_parts) > 1:
                    # Extract intermediate nodes for abbreviation
                    intermediate_parts = path_parts[
                        :-1
                    ]  # exclude the array name itself

                    # Apply abbreviation to intermediate nodes
                    if abbreviate_enabled:
                        abbreviated_intermediates = []
                        for part in intermediate_parts:
                            # Skip entity name if it appears in the path
                            if part == sanitized_entity:
                                continue

                            # Abbreviate intermediate nodes
                            if (
                                max_component_length
                                and len(part) > max_component_length
                            ):
                                abbreviated_part = part[:max_component_length]
                            else:
                                abbreviated_part = part

                            abbreviated_intermediates.append(abbreviated_part)

                        # Build the table name with abbreviated intermediates
                        if abbreviated_intermediates:
                            path_str = separator.join(abbreviated_intermediates)
                            table_name = (
                                f"{sanitized_entity}{separator}{path_str}"
                                f"{separator}{sanitized_key}"
                            )
                        else:
                            # No intermediates - use first level format
                            table_name = f"{sanitized_entity}{separator}{sanitized_key}"
                    else:
                        # No abbreviation, just join without entity duplication
                        path_str = separator.join(
                            [p for p in intermediate_parts if p != sanitized_entity]
                        )
                        if path_str:
                            table_name = (
                                f"{sanitized_entity}{separator}{path_str}"
                                f"{separator}{sanitized_key}"
                            )
                        else:
                            table_name = f"{sanitized_entity}{separator}{sanitized_key}"
                else:
                    # Only key level (shouldn't happen in nested case but as fallback)
                    table_name = f"{sanitized_entity}{separator}{sanitized_key}"

            # Skip if array is empty
            if not value:
                continue

            # Process each item in the array
            for i, item in enumerate(value):
                # Skip null array items
                if item is None:
                    continue

                # Only process dictionary items
                if not isinstance(item, dict):
                    continue

                # Process dictionary item
                try:
                    # Create path for this specific array item
                    item_path = f"{current_path}{separator}{i}"

                    # Determine ID source field based on configuration
                    source_field = None
                    if default_id_field:
                        if isinstance(default_id_field, str):
                            # Single field for all paths
                            source_field = default_id_field
                        else:
                            # Path-specific field mapping
                            if current_path in default_id_field:
                                source_field = default_id_field[current_path]
                            elif "*" in default_id_field:
                                source_field = default_id_field["*"]
                            elif table_name in default_id_field:
                                source_field = default_id_field[table_name]

                    # Flatten the JSON structure
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

                    # Add metadata fields to the record
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

                    # Track array source information
                    annotated["__array_field"] = sanitized_key
                    annotated["__array_index"] = i

                    # Output the processed record
                    yield table_name, annotated

                    # Process any nested arrays recursively
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

                    yield from nested_generator

                except Exception as e:
                    # Apply recovery strategy if available
                    if recovery_strategy and hasattr(recovery_strategy, "recover"):
                        try:
                            recovery_result = recovery_strategy.recover(
                                e,
                                entity_name=entity_name,
                                path=current_path.split(separator),
                            )
                            # Create placeholder record if recovery succeeded
                            if recovery_result is not None and isinstance(
                                recovery_result, dict
                            ):
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
                            # Log recovery failures
                            logger.warning(f"Recovery failed: {re}")
                    # Re-raise original exception if recovery failed or unavailable
                    raise

        # Process nested objects
        elif isinstance(value, dict):
            try:
                # Recursively process nested dictionary
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

                yield from nested_generator

            except Exception as e:
                # Apply recovery strategy if available
                if recovery_strategy and hasattr(recovery_strategy, "recover"):
                    try:
                        recovery_result = recovery_strategy.recover(
                            e,
                            entity_name=entity_name,
                            path=current_path.split(separator),
                        )
                        # Continue to next field if recovery succeeded
                        if recovery_result is not None:
                            continue
                    except Exception as re:
                        logger.warning(f"Recovery failed: {re}")
                # Re-raise original exception if recovery failed or unavailable
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
    # Initialize result dictionary
    result: ExtractResult = {}

    # Process data using implementation helper
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
        # Create table if not exists
        if table_name not in result:
            result[table_name] = []

        # Add record to appropriate table
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
    # Delegate to implementation with streaming mode enabled
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
