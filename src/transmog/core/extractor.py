"""Array extraction module for nested JSON structures.

This module extracts nested arrays from JSON structures and
creates child tables with appropriate parent-child relationships.
"""

from collections.abc import Generator
from datetime import datetime
from typing import Any, Callable, Optional, Union

from transmog.core.flattener import flatten_json
from transmog.core.metadata import annotate_with_metadata
from transmog.error import logger
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
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    deeply_nested_threshold: int = 4,
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
        deeply_nested_threshold: Threshold for deep nesting (default 4)
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
                deeply_nested_threshold=deeply_nested_threshold,
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
                            deeply_nested_threshold=deeply_nested_threshold,
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

                    # Process any nested arrays recursively, but only for dict items
                    if isinstance(item, dict):
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
                            deeply_nested_threshold=deeply_nested_threshold,
                            default_id_field=default_id_field,
                            id_generation_strategy=id_generation_strategy,
                            recovery_strategy=recovery_strategy,
                            max_depth=max_depth,
                            current_depth=current_depth + 1,
                            streaming_mode=streaming_mode,
                        )
                        # Process nested arrays one by one
                        yield from nested_generator
                except Exception as e:
                    # Apply recovery strategy if available
                    if recovery_strategy == "skip":
                        # Skip this array item
                        continue
                    elif recovery_strategy == "partial":
                        # Create error record
                        error_record = {
                            "__error": str(e),
                            "__array_field": sanitized_key,
                            "__array_index": i,
                        }
                        # Add metadata
                        annotated = annotate_with_metadata(
                            error_record,
                            parent_id=parent_id,
                            extract_time=extract_time,
                            id_field=id_field,
                            parent_field=parent_field,
                            time_field=time_field,
                            in_place=True,
                        )
                        # Output the error record
                        yield f"{table_name}{separator}errors", annotated
                    else:
                        # Raise the exception (default behavior)
                        raise

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
                extract_time=extract_time,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                deeply_nested_threshold=deeply_nested_threshold,
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=recovery_strategy,
                max_depth=max_depth,
                current_depth=current_depth + 1,
                streaming_mode=streaming_mode,
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
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    deeply_nested_threshold: int = 4,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    streaming: bool = False,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
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
        extract_time: Extraction timestamp
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        deeply_nested_threshold: Threshold for deep nesting (default 4)
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        streaming: Whether to use streaming mode (returns generator)
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Current depth in the recursion

    Returns:
        Dictionary mapping table names to lists of records

    Notes:
        - Empty objects ({}) and empty arrays ([]) are skipped
        - Dictionary items that are empty are also skipped
        - Null values in arrays are skipped when skip_null is True
    """
    # Initialize extraction timestamp if not provided
    if extract_time is None:
        extract_time = datetime.now()

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
        extract_time=extract_time,
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
        deeply_nested_threshold=deeply_nested_threshold,
        default_id_field=default_id_field,
        id_generation_strategy=id_generation_strategy,
        recovery_strategy=recovery_strategy,
        max_depth=max_depth,
        current_depth=current_depth,
        streaming_mode=False,
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
    extract_time: Optional[Any] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    deeply_nested_threshold: int = 4,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
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
        extract_time: Extraction timestamp
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        deeply_nested_threshold: Threshold for deep nesting (default 4)
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Current depth in the recursion

    Yields:
        Tuples of (table_name, record) for each extracted record

    Notes:
        - Empty objects ({}) and empty arrays ([]) are skipped
        - Dictionary items that are empty are also skipped
        - Null values in arrays are skipped when skip_null is True
    """
    # Initialize extraction timestamp if not provided
    if extract_time is None:
        extract_time = datetime.now()

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
        extract_time=extract_time,
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
        deeply_nested_threshold=deeply_nested_threshold,
        default_id_field=default_id_field,
        id_generation_strategy=id_generation_strategy,
        recovery_strategy=recovery_strategy,
        max_depth=max_depth,
        current_depth=current_depth,
        streaming_mode=True,
    )
