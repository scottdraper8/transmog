"""Hierarchy processing module for nested JSON structures.

Provides functions to process complete JSON structures with
parent-child relationship preservation.
"""

import logging
from collections.abc import Generator
from datetime import datetime
from typing import (
    Any,
    Callable,
    Optional,
    Union,
)

from transmog.core.extractor import extract_arrays, stream_extract_arrays
from transmog.core.flattener import flatten_json
from transmog.core.metadata import (
    annotate_with_metadata,
    generate_deterministic_id,
)

# Type aliases
JsonDict = dict[str, Any]
FlatDict = dict[str, Any]
ArrayDict = dict[str, list[dict[str, Any]]]
ProcessResult = tuple[FlatDict, ArrayDict]
StreamingChildTables = Generator[tuple[str, list[FlatDict]], None, None]
ArrayResult = Union[ArrayDict, StreamingChildTables]

# Logger setup
logger = logging.getLogger(__name__)


def process_structure(
    data: JsonDict,
    entity_name: str,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    extract_time: Optional[Any] = None,
    root_entity: Optional[str] = None,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: Optional[int] = None,
    max_field_component_length: Optional[int] = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[dict[str, str]] = None,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    visit_arrays: bool = True,
    streaming: bool = False,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
) -> tuple[FlatDict, ArrayResult]:
    """Process JSON structure with parent-child relationship preservation.

    This is the main entry point for processing a complete JSON structure,
    handling both the main record and all nested arrays.

    Args:
        data: JSON data to process
        entity_name: Entity name
        parent_id: UUID of parent record
        parent_path: Path from root
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        extract_time: Extraction timestamp
        root_entity: Top-level entity name
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        visit_arrays: Whether to visit and process arrays
        streaming: Whether to return a generator for child tables
        recovery_strategy: Strategy for handling errors
        max_depth: Maximum recursion depth for nested structures

    Returns:
        Tuple of (flattened main object, array tables dictionary or generator)
    """
    # Initialize root_entity at top level
    if root_entity is None:
        root_entity = entity_name

    # Handle empty data case
    if data is None:
        empty_result = {
            id_field: generate_deterministic_id(str(entity_name) + "_empty"),
            time_field: extract_time or datetime.now(),
            "__original_entity": entity_name,
        }
        if parent_id:
            empty_result[parent_field] = parent_id

        # Return empty result based on mode
        if streaming:

            def empty_generator() -> Generator[
                tuple[str, list[dict[str, Any]]], None, None
            ]:
                if False:
                    yield "", []

            return empty_result, empty_generator()
        else:
            return empty_result, {}

    # Flatten the object
    flattened_obj = flatten_json(
        data,
        parent_path=parent_path,
        separator=separator,
        cast_to_string=cast_to_string,
        include_empty=include_empty,
        skip_arrays=True,
        skip_null=skip_null,
        abbreviate_field_names=abbreviate_field_names,
        max_field_component_length=max_field_component_length,
        preserve_leaf_component=preserve_leaf_component,
        custom_abbreviations=custom_abbreviations,
        mode="streaming",
        recovery_strategy=recovery_strategy,
        max_depth=max_depth,
    )

    # Resolve source field for deterministic ID generation
    source_field = None
    if default_id_field:
        if isinstance(default_id_field, str):
            source_field = default_id_field
        else:
            normalized_path = parent_path.strip(separator) if parent_path else ""

            # Path resolution priority: exact match, wildcard, prefix matches
            if normalized_path in default_id_field:
                source_field = default_id_field[normalized_path]
            elif "*" in default_id_field:
                source_field = default_id_field["*"]
            else:
                path_components = (
                    normalized_path.split(separator) if normalized_path else []
                )
                for i in range(len(path_components), 0, -1):
                    prefix = separator.join(path_components[:i])
                    prefix_wildcard = f"{prefix}{separator}*"
                    if prefix_wildcard in default_id_field:
                        source_field = default_id_field[prefix_wildcard]
                        break

    # Add metadata with in-place optimization
    flattened_obj_dict = flattened_obj if flattened_obj is not None else {}
    annotated_obj = annotate_with_metadata(
        flattened_obj_dict,
        parent_id=parent_id,
        extract_time=extract_time,
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
        in_place=True,
        source_field=source_field,
        id_generation_strategy=id_generation_strategy,
    )

    # Skip array processing if not needed
    if not visit_arrays:
        if streaming:

            def empty_generator() -> Generator[
                tuple[str, list[dict[str, Any]]], None, None
            ]:
                if False:
                    yield "", []

            return annotated_obj, empty_generator()
        else:
            return annotated_obj, {}

    # Handle array extraction based on streaming mode
    extract_id = annotated_obj.get(id_field)

    if streaming:

        def generate_child_records() -> Generator[
            tuple[str, list[dict[str, Any]]], None, None
        ]:
            # Collect records by table name for complete table yields
            collected_tables: dict[str, list[dict[str, Any]]] = {}

            generator = stream_extract_arrays(
                data,
                parent_id=extract_id,
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
                abbreviate_enabled=abbreviate_table_names,
                max_component_length=max_table_component_length,
                preserve_leaf=preserve_leaf_component,
                custom_abbreviations=custom_abbreviations,
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=recovery_strategy,
                max_depth=max_depth - 1,
            )

            # Group by table name
            for table, record in generator:
                if table not in collected_tables:
                    collected_tables[table] = []
                collected_tables[table].append(record)

            # Yield complete tables with records
            for table_name, records in collected_tables.items():
                if records:
                    yield table_name, records

        return annotated_obj, generate_child_records()
    else:
        # Extract all nested arrays
        arrays = extract_arrays(
            data,
            parent_id=extract_id,
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
            abbreviate_enabled=abbreviate_table_names,
            max_component_length=max_table_component_length,
            preserve_leaf=preserve_leaf_component,
            custom_abbreviations=custom_abbreviations,
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            streaming=False,
            recovery_strategy=recovery_strategy,
            max_depth=max_depth,
        )

        return annotated_obj, arrays


def process_record_batch(
    records: list[JsonDict], entity_name: str, batch_size: int = 100, **kwargs: Any
) -> tuple[list[FlatDict], ArrayDict]:
    """Process a batch of records efficiently.

    Args:
        records: List of JSON records
        entity_name: Entity name
        batch_size: Batch size for processing
        **kwargs: Additional arguments to pass to process_structure

    Returns:
        Tuple of (flattened records, array tables dictionary)
    """
    # Initialize result containers
    flat_records = []
    combined_arrays: dict[str, list[dict[str, Any]]] = {}

    # Process records in batches
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]

        # Process each record in the batch
        for record in batch:
            flat_obj, arrays = process_structure(
                record, entity_name=entity_name, **kwargs
            )
            flat_records.append(flat_obj)

            # Merge arrays from this record
            if isinstance(arrays, dict):
                for table_name, table_data in arrays.items():
                    if table_name in combined_arrays:
                        combined_arrays[table_name].extend(table_data)
                    else:
                        combined_arrays[table_name] = table_data

    return flat_records, combined_arrays


def process_records_in_single_pass(
    records: list[JsonDict],
    entity_name: str,
    extract_time: Optional[Any] = None,
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    visit_arrays: bool = True,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: Optional[int] = None,
    max_field_component_length: Optional[int] = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[dict[str, str]] = None,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    **kwargs: Any,
) -> tuple[list[FlatDict], ArrayDict]:
    """Process all records in a single pass, building main/child tables simultaneously.

    This is the standard processing approach for most datasets where memory
    is not a constraint. All tables are built fully in memory.

    Args:
        records: List of JSON records to process
        entity_name: Name of the entity being processed
        extract_time: Extraction timestamp
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        visit_arrays: Whether to visit and process arrays
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for handling errors
        max_depth: Maximum recursion depth for nested structures
        **kwargs: Additional keyword arguments passed to process_structure

    Returns:
        Tuple of (main table as list of dicts, child tables as dict of lists)
    """
    # Set default extraction timestamp if not specified
    if extract_time is None:
        extract_time = datetime.now()

    # Initialize results
    main_records = []
    child_tables: dict[str, list[dict[str, Any]]] = {}

    # Process each record
    for record in records:
        main_record, child_records = process_structure(
            data=record,
            entity_name=entity_name,
            parent_id=None,
            parent_path="",
            separator=separator,
            cast_to_string=cast_to_string,
            include_empty=include_empty,
            skip_null=skip_null,
            extract_time=extract_time,
            root_entity=entity_name,
            abbreviate_table_names=abbreviate_table_names,
            abbreviate_field_names=abbreviate_field_names,
            max_table_component_length=max_table_component_length,
            max_field_component_length=max_field_component_length,
            preserve_leaf_component=preserve_leaf_component,
            custom_abbreviations=custom_abbreviations,
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            id_field=id_field,
            parent_field=parent_field,
            time_field=time_field,
            visit_arrays=visit_arrays,
            streaming=False,
            recovery_strategy=recovery_strategy,
            max_depth=max_depth,
        )

        # Add to main table
        main_records.append(main_record)

        # Add to child tables
        if isinstance(child_records, dict):
            for table_name, table_records in child_records.items():
                if table_name not in child_tables:
                    child_tables[table_name] = []
                child_tables[table_name].extend(table_records)
        elif hasattr(child_records, "__next__"):
            for table_name, table_records in child_records:
                if table_name not in child_tables:
                    child_tables[table_name] = []
                child_tables[table_name].extend(table_records)

    return main_records, child_tables


def stream_process_records(
    records: list[JsonDict],
    entity_name: str,
    extract_time: Optional[Any] = None,
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    visit_arrays: bool = True,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: Optional[int] = None,
    max_field_component_length: Optional[int] = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[dict[str, str]] = None,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    max_depth: int = 100,
    **kwargs: Any,
) -> tuple[list[FlatDict], StreamingChildTables]:
    """Stream process records for memory-efficient processing.

    This function processes records in a streaming fashion, returning
    a generator for child tables instead of building them all in memory.

    Args:
        records: List of JSON records to process
        entity_name: Name of the entity being processed
        extract_time: Extraction timestamp
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        visit_arrays: Whether to visit and process arrays
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        max_depth: Maximum recursion depth for nested structures
        **kwargs: Additional keyword arguments passed to process_structure

    Returns:
        Tuple of (main table as list of dicts, generator for child tables)
    """
    # Set default extraction timestamp
    if extract_time is None:
        extract_time = datetime.now()

    # Initialize results
    main_records = []
    generators_list: list[Generator[tuple[str, list[dict[str, Any]]], None, None]] = []

    # Process each record
    for record in records:
        main_record, child_result = process_structure(
            data=record,
            entity_name=entity_name,
            parent_id=None,
            parent_path="",
            separator=separator,
            cast_to_string=cast_to_string,
            include_empty=include_empty,
            skip_null=skip_null,
            extract_time=extract_time,
            root_entity=entity_name,
            abbreviate_table_names=abbreviate_table_names,
            abbreviate_field_names=abbreviate_field_names,
            max_table_component_length=max_table_component_length,
            max_field_component_length=max_field_component_length,
            preserve_leaf_component=preserve_leaf_component,
            custom_abbreviations=custom_abbreviations,
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            id_field=id_field,
            parent_field=parent_field,
            time_field=time_field,
            visit_arrays=visit_arrays,
            streaming=True,
            max_depth=max_depth,
        )

        # Add to main table
        main_records.append(main_record)

        # Handle child results by type
        if isinstance(child_result, dict):
            # Convert dictionary to generator
            def dict_to_generator(
                d: dict[str, list[dict[str, Any]]],
            ) -> Generator[tuple[str, list[dict[str, Any]]], None, None]:
                items_gen: Generator[tuple[str, list[dict[str, Any]]], None, None] = (
                    (table_name, records) for table_name, records in d.items()
                )
                yield from items_gen

            generators_list.append(dict_to_generator(child_result))
        else:
            generators_list.append(child_result)

    # Generator to yield child tables
    def child_tables_generator() -> Generator[
        tuple[str, list[dict[str, Any]]], None, None
    ]:
        # Collect records for each table
        table_records: dict[str, list[dict[str, Any]]] = {}

        # Process each generator
        for generator in generators_list:
            for table_name, records in generator:
                if table_name not in table_records:
                    table_records[table_name] = []
                table_records[table_name].extend(records)

        # Yield non-empty tables
        for table_name, records in table_records.items():
            if records:
                yield table_name, records

    return main_records, child_tables_generator()
