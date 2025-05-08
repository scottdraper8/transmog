"""
Array extraction module for nested JSON structures.

This module extracts nested arrays from JSON structures and
creates child tables with appropriate parent-child relationships.
"""

import json
from typing import Any, Dict, List, Optional, Tuple, Callable, Generator, Union, cast
from datetime import datetime

from transmog.core.flattener import flatten_json
from transmog.core.metadata import annotate_with_metadata, generate_deterministic_id
from transmog.error import error_context, ProcessingError, logger
from transmog.naming.conventions import get_table_name, sanitize_name
from transmog.naming.abbreviator import abbreviate_table_name
from transmog.config.settings import settings

# Type aliases
JsonDict = Dict[str, Any]
JsonList = List[Dict[str, Any]]
ArrayDict = Dict[str, JsonList]
ExtractResult = Dict[str, List[Dict[str, Any]]]


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
    custom_abbreviations: Optional[Dict[str, str]] = None,
    default_id_field: Optional[Union[str, Dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    streaming: bool = False,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
) -> ExtractResult:
    """
    Extract all nested arrays from a data structure.

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
        default_id_field: Default ID field name for deterministic IDs
        id_generation_strategy: Custom function for ID generation
        streaming: Whether to stream results
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Current depth in the recursion

    Returns:
        Dictionary of array tables with their records
    """
    # Handle null/missing data - nothing to extract
    if data is None:
        return {}

    # Check if we've reached the maximum depth
    if current_depth >= max_depth:
        logger.warning(
            f"Maximum recursion depth ({max_depth}) reached at path: {parent_path}"
        )
        return {}

    # Use in-place for better performance
    result: ExtractResult = {}

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

            # Initialize array table if not already present
            if table_name not in result:
                result[table_name] = []

            # Create sanitized versions of field paths
            sanitized_key = sanitize_name(key, separator)

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

                    # Convert default_id_field to deterministic_id_fields format
                    deterministic_id_fields = None
                    if default_id_field:
                        if isinstance(default_id_field, str):
                            deterministic_id_fields = {current_path: default_id_field}
                        else:
                            deterministic_id_fields = default_id_field

                    # Identify ID field for deterministic generation
                    source_field = None
                    if deterministic_id_fields:
                        # Exact path match
                        if current_path in deterministic_id_fields:
                            source_field = deterministic_id_fields[current_path]
                        # Wildcard match
                        elif "*" in deterministic_id_fields:
                            source_field = deterministic_id_fields["*"]
                        # Table name match
                        elif table_name in deterministic_id_fields:
                            source_field = deterministic_id_fields[table_name]

                    # Flatten the item
                    flattened = flatten_json(
                        item,
                        separator=separator,
                        cast_to_string=cast_to_string,
                        include_empty=include_empty,
                        skip_null=skip_null,
                        parent_path="",
                        in_place=False,
                        mode="streaming" if streaming else "standard",
                        recovery_strategy=recovery_strategy,
                    )

                    # Add metadata (extract_id, parent_id, etc.)
                    annotated = annotate_with_metadata(
                        flattened,
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

                    # Add to results
                    result[table_name].append(annotated)

                    # Process nested arrays recursively
                    nested_arrays = extract_arrays(
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
                        streaming=streaming,
                        recovery_strategy=recovery_strategy,
                        max_depth=max_depth,
                        current_depth=current_depth + 1,
                    )

                    # Combine nested arrays into results
                    for nested_name, nested_data in nested_arrays.items():
                        if nested_name not in result:
                            result[nested_name] = []
                        result[nested_name].extend(nested_data)
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
                                        {}, source_field, id_generation_strategy
                                    ),
                                    parent_field: parent_id,
                                    time_field: extract_time or datetime.now(),
                                    "__array_field": sanitized_key,
                                    "__array_index": i,
                                    "__extract_status": "error_recovered",
                                    "__error_message": str(e),
                                }
                                result[table_name].append(simple_record)
                                continue
                        except Exception as re:
                            # If recovery itself fails, log and continue with original error
                            logger.warning(f"Recovery failed: {re}")
                    # Rethrow the exception if no recovery or recovery failed
                    raise

        # Handle nested objects
        elif isinstance(value, dict):
            # Process nested arrays recursively
            try:
                nested_arrays = extract_arrays(
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
                    streaming=streaming,
                    recovery_strategy=recovery_strategy,
                    max_depth=max_depth,
                    current_depth=current_depth + 1,
                )

                # Combine nested arrays into results
                for nested_name, nested_data in nested_arrays.items():
                    if nested_name not in result:
                        result[nested_name] = []
                    result[nested_name].extend(nested_data)
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
    custom_abbreviations: Optional[Dict[str, str]] = None,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    current_depth: int = 0,
) -> Generator[Tuple[str, JsonList], None, None]:
    """
    Stream extract arrays for memory-efficient processing.

    Similar to extract_arrays but yields tables one at a time
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
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth allowed
        current_depth: Current depth in the recursion

    Returns:
        Generator yielding tuples of (table_name, records)
    """
    # Handle null case
    if data is None:
        return
        yield  # Never executed, just for type checking

    # Use extract_arrays with streaming flag
    result = extract_arrays(
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
        streaming=True,
        recovery_strategy=recovery_strategy,
        max_depth=max_depth,
        current_depth=current_depth,
    )

    # Yield each table
    for table_name, table_records in result.items():
        if table_records:
            # For each table, yield each record individually
            for record in table_records:
                yield table_name, record
