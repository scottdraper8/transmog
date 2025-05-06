"""
Hierarchy processing module for nested JSON structures.

Provides functions to process complete JSON structures with
parent-child relationship preservation.
"""

from typing import Any, Dict, List, Optional, Set, Tuple, Callable, Generator, Iterator
from datetime import datetime

from transmog.core.extractor import extract_arrays
from transmog.core.flattener import flatten_json
from transmog.core.metadata import (
    annotate_with_metadata,
    generate_deterministic_id,
)
from transmog.config.settings import settings
from transmog.naming.conventions import sanitize_name, get_table_name
from transmog.naming.abbreviator import abbreviate_table_name

# Type aliases
JsonDict = Dict[str, Any]
FlatDict = Dict[str, Any]
ArrayDict = Dict[str, List[Dict[str, Any]]]
ProcessResult = Tuple[FlatDict, ArrayDict]
VisitedPath = Set[int]
StreamingChildTables = Generator[Tuple[str, List[FlatDict]], None, None]


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
    visited: Optional[VisitedPath] = None,
    root_entity: Optional[str] = None,
    shared_flatten_cache: Optional[Dict[int, FlatDict]] = None,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
) -> ProcessResult:
    """
    Process JSON structure with parent-child relationship preservation.

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
        visited: Object IDs for circular detection
        root_entity: Top-level entity name
        shared_flatten_cache: Cache of flattened objects
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        deterministic_id_fields: Dict mapping paths to field names for deterministic IDs
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for handling circular references

    Returns:
        Tuple of (flattened main object, array tables dictionary)
    """
    if visited is None:
        visited = set()

    if shared_flatten_cache is None:
        shared_flatten_cache = {}

    # Initialize root_entity if at top level
    if root_entity is None:
        root_entity = entity_name

    # Prevent circular references
    obj_id = id(data)
    if obj_id in visited:
        return {}, {}

    visited.add(obj_id)

    # Check if we already flattened this object
    if obj_id in shared_flatten_cache:
        flattened_obj = shared_flatten_cache[obj_id]
    else:
        # Process this object - flatten it first
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
        )
        # Cache the result
        shared_flatten_cache[obj_id] = flattened_obj

    # Determine source field for deterministic ID generation
    source_field = None
    if deterministic_id_fields:
        # Normalize path for comparison
        normalized_path = parent_path.strip(separator) if parent_path else ""

        # Exact path match first
        if normalized_path in deterministic_id_fields:
            source_field = deterministic_id_fields[normalized_path]
        # Then try wildcard matches if exact match not found
        elif "*" in deterministic_id_fields:
            source_field = deterministic_id_fields["*"]
        # Try any path prefix matches (most specific to least)
        else:
            path_components = (
                normalized_path.split(separator) if normalized_path else []
            )
            for i in range(len(path_components), 0, -1):
                prefix = separator.join(path_components[:i])
                prefix_wildcard = f"{prefix}{separator}*"
                if prefix_wildcard in deterministic_id_fields:
                    source_field = deterministic_id_fields[prefix_wildcard]
                    break

    # Add metadata - use in_place for better performance
    annotated_obj = annotate_with_metadata(
        flattened_obj,
        parent_id=parent_id,
        extract_time=extract_time,
        in_place=True,
        source_field=source_field,
        id_generation_strategy=id_generation_strategy,
    )

    # Extract all nested arrays - reuse the visited set and flatten cache
    arrays = extract_arrays(
        data,
        parent_id=annotated_obj.get("__extract_id"),
        parent_path=parent_path,
        entity_name=entity_name,
        separator=separator,
        cast_to_string=cast_to_string,
        include_empty=include_empty,
        skip_null=skip_null,
        extract_time=extract_time,
        visited=visited,
        shared_flatten_cache=shared_flatten_cache,
        abbreviate_enabled=abbreviate_table_names,
        max_component_length=max_table_component_length,
        preserve_leaf=preserve_leaf_component,
        custom_abbreviations=custom_abbreviations,
        default_id_field=deterministic_id_fields,
        id_generation_strategy=id_generation_strategy,
        recovery_strategy=recovery_strategy,
    )

    return annotated_obj, arrays


def process_record_batch(
    records: List[JsonDict], entity_name: str, batch_size: int = 100, **kwargs
) -> Tuple[List[FlatDict], ArrayDict]:
    """
    Process a batch of records, returning flattened main records and array tables.

    This is a convenience function for batch processing that can be used
    with parallel execution frameworks.

    Args:
        records: List of JSON records to process
        entity_name: Entity name
        batch_size: Size of batches to process at once
        **kwargs: Additional arguments to pass to process_structure

    Returns:
        Tuple of (list of flattened records, combined array tables dictionary)
    """
    main_records = []
    all_arrays: Dict[str, List[Dict[str, Any]]] = {}

    # Create a shared flatten cache for better performance
    shared_flatten_cache = {}

    # Filter out parameters that process_structure doesn't accept
    # Only allow parameters that are explicitly in process_structure's signature
    allowed_params = {
        "separator",
        "cast_to_string",
        "include_empty",
        "skip_null",
        "extract_time",
        "root_entity",
        "shared_flatten_cache",
        "visited",
        "abbreviate_table_names",
        "abbreviate_field_names",
        "max_table_component_length",
        "max_field_component_length",
        "preserve_leaf_component",
        "custom_abbreviations",
        "deterministic_id_fields",
        "id_generation_strategy",
    }
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in allowed_params}

    # Process in batches for better memory usage
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]

        # Shared visited set for circular reference detection
        visited = set()

        # Process each record in the batch
        for record in batch:
            # Process main record
            main_record, arrays = process_structure(
                record,
                entity_name,
                shared_flatten_cache=shared_flatten_cache,
                visited=visited,  # Pass visited set to prevent circular references
                **filtered_kwargs,
            )
            main_records.append(main_record)

            # Combine arrays efficiently
            for array_name, array_items in arrays.items():
                if array_items:
                    all_arrays.setdefault(array_name, []).extend(array_items)

        # Clear the shared cache after each batch to avoid memory buildup
        shared_flatten_cache.clear()

    return main_records, all_arrays


def process_records_in_single_pass(
    records: List[JsonDict],
    entity_name: str,
    extract_time: Optional[Any] = None,
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    visit_arrays: bool = False,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    default_id_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
    **kwargs,
) -> Tuple[List[FlatDict], ArrayDict]:
    """
    Process all records in a single pass for best performance.

    This function is optimized for performance, processing all records
    in a single pass to minimize memory consumption.

    Args:
        records: List of records to process
        entity_name: Name of the entity
        extract_time: Extraction timestamp
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        id_field: Field name for the record ID
        parent_field: Field name for the parent record ID
        time_field: Field name for the extraction timestamp
        visit_arrays: Whether to visit array elements
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Field name to use for deterministic ID generation
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for handling circular references
        **kwargs: Additional keyword arguments

    Returns:
        Tuple of (main_records, child_tables)
    """
    # Set default extract time if not provided
    if extract_time is None:
        extract_time = datetime.now()

    # Initialize result containers
    main_records = []
    all_arrays: Dict[str, List[Dict[str, Any]]] = {}
    shared_flatten_cache = {}

    # Set default xtract_time if None
    if extract_time is None:
        extract_time = datetime.now()

    # Process each record
    for record in records:
        if record is None:
            continue

        # Get a clean visited set for each record to avoid cross-record issues
        visited = set()

        # Flatten the main record
        flattened = flatten_json(
            record,
            separator=separator,
            cast_to_string=cast_to_string,
            include_empty=include_empty,
            skip_arrays=True,
            skip_null=skip_null,
            visit_arrays=visit_arrays,
            abbreviate_field_names=abbreviate_field_names,
            max_field_component_length=max_field_component_length,
            preserve_leaf_component=preserve_leaf_component,
            custom_abbreviations=custom_abbreviations,
            mode="streaming",
            recovery_strategy=recovery_strategy,
        )

        # Add metadata
        annotated = annotate_with_metadata(
            flattened,
            parent_id=None,  # Main records have no parent
            extract_time=extract_time,
            id_field=id_field,
            parent_field=parent_field,
            time_field=time_field,
            source_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            in_place=True,
        )

        # Add to main records
        main_records.append(annotated)

        # Extract arrays with the record ID as parent
        record_id = annotated.get(id_field)
        arrays = extract_arrays(
            record,
            parent_id=record_id,
            entity_name=entity_name,
            separator=separator,
            cast_to_string=cast_to_string,
            include_empty=include_empty,
            skip_null=skip_null,
            extract_time=extract_time,
            visited=visited,
            shared_flatten_cache=shared_flatten_cache,
            abbreviate_enabled=abbreviate_table_names,
            max_component_length=max_table_component_length,
            preserve_leaf=preserve_leaf_component,
            custom_abbreviations=custom_abbreviations,
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            recovery_strategy=recovery_strategy,
        )

        # Merge arrays into all_arrays
        for table_name, table_records in arrays.items():
            if table_name not in all_arrays:
                all_arrays[table_name] = []
            all_arrays[table_name].extend(table_records)

    return main_records, all_arrays


def stream_process_records(
    records: List[JsonDict],
    entity_name: str,
    extract_time: Optional[Any] = None,
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    visit_arrays: bool = False,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    use_deterministic_ids: bool = False,
    **kwargs,
) -> Tuple[List[FlatDict], StreamingChildTables]:
    """
    Stream process records with parent-child relationship preservation.

    Unlike process_records_in_single_pass, this function doesn't build complete child tables
    in memory. Instead, it returns a generator that yields child table records in batches.

    Args:
        records: List of JSON records to process
        entity_name: Entity name for naming
        extract_time: Extraction timestamp
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        id_field: Field name for extract ID
        parent_field: Field name for parent ID
        time_field: Field name for timestamp
        visit_arrays: Whether to treat array items as fields
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        deterministic_id_fields: Dict mapping paths to field names for deterministic IDs
        id_generation_strategy: Custom function for ID generation
        use_deterministic_ids: Whether to use deterministic IDs
        **kwargs: Additional arguments for backwards compatibility

    Returns:
        Tuple of (list of flattened main records, generator of (table_name, records) tuples)
    """
    main_records = []
    child_table_buffers = {}
    child_table_buffer_size = 100  # Number of records to buffer before yielding

    # Get default values for any None parameters
    if max_table_component_length is None:
        max_table_component_length = settings.get_option("max_table_component_length")

    if max_field_component_length is None:
        max_field_component_length = settings.get_option("max_field_component_length")

    # Create a shared flatten cache for better performance within this batch
    shared_flatten_cache = {}

    # Process root IDs deterministically if configured
    root_deterministic_source = None
    if deterministic_id_fields and "" in deterministic_id_fields:
        root_deterministic_source = deterministic_id_fields[""]
    elif deterministic_id_fields and "*" in deterministic_id_fields:
        root_deterministic_source = deterministic_id_fields["*"]

    # Pre-process all main records first
    for record in records:
        # Check for deterministic IDs at the root level
        extract_id = None
        if root_deterministic_source and root_deterministic_source in record:
            source_value = record[root_deterministic_source]
            if source_value:
                extract_id = generate_deterministic_id(source_value)

        # Process the record using streaming versions of flattener and extractor
        flat_record, _ = stream_process_structure(
            data=record,
            entity_name=entity_name,
            parent_id=None,
            extract_time=extract_time,
            separator=separator,
            cast_to_string=cast_to_string,
            include_empty=include_empty,
            skip_null=skip_null,
            id_field=id_field,
            parent_field=parent_field,
            time_field=time_field,
            visit_arrays=visit_arrays,
            shared_flatten_cache=shared_flatten_cache,
            abbreviate_table_names=abbreviate_table_names,
            abbreviate_field_names=abbreviate_field_names,
            max_table_component_length=max_table_component_length,
            max_field_component_length=max_field_component_length,
            preserve_leaf_component=preserve_leaf_component,
            custom_abbreviations=custom_abbreviations,
            deterministic_id_fields=deterministic_id_fields,
            id_generation_strategy=id_generation_strategy,
        )

        # Force deterministic ID if we generated one
        if extract_id:
            flat_record[id_field] = extract_id

        main_records.append(flat_record)

    # Create a generator function to yield child records
    def child_tables_generator():
        # Process child records for each record
        for record in records:
            # Get the main record's ID to use as parent ID
            main_record_idx = records.index(record)
            parent_extract_id = main_records[main_record_idx].get(id_field)

            # Process the record using streaming versions of flattener and extractor
            _, child_records_gen = stream_process_structure(
                data=record,
                entity_name=entity_name,
                parent_id=parent_extract_id,  # Use the ID from the main record
                extract_time=extract_time,
                separator=separator,
                cast_to_string=cast_to_string,
                include_empty=include_empty,
                skip_null=skip_null,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                visit_arrays=visit_arrays,
                shared_flatten_cache=shared_flatten_cache,
                abbreviate_table_names=abbreviate_table_names,
                abbreviate_field_names=abbreviate_field_names,
                max_table_component_length=max_table_component_length,
                max_field_component_length=max_field_component_length,
                preserve_leaf_component=preserve_leaf_component,
                custom_abbreviations=custom_abbreviations,
                deterministic_id_fields=deterministic_id_fields,
                id_generation_strategy=id_generation_strategy,
            )

            # Process child records
            for table_name, child_record in child_records_gen:
                # Initialize buffer for this table if needed
                if table_name not in child_table_buffers:
                    child_table_buffers[table_name] = []

                # Add record to buffer
                child_table_buffers[table_name].append(child_record)

                # Yield records if buffer is full
                if len(child_table_buffers[table_name]) >= child_table_buffer_size:
                    yield table_name, child_table_buffers[table_name]
                    child_table_buffers[table_name] = []

        # Yield any remaining buffered records
        for table_name, buffered_records in child_table_buffers.items():
            if buffered_records:  # Only yield non-empty buffers
                yield table_name, buffered_records

    # Return the main records and the generator for child tables
    return main_records, child_tables_generator()


def stream_process_structure(
    data: JsonDict,
    entity_name: str,
    parent_id: Optional[str] = None,
    parent_path: str = "",
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    extract_time: Optional[Any] = None,
    visited: Optional[VisitedPath] = None,
    root_entity: Optional[str] = None,
    shared_flatten_cache: Optional[Dict[int, FlatDict]] = None,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    visit_arrays: bool = False,
) -> Tuple[FlatDict, Generator[Tuple[str, FlatDict], None, None]]:
    """
    Stream process a JSON structure, yielding child records as they are processed.

    Unlike process_structure, this function doesn't build complete child tables in memory.
    Instead, it returns a generator that yields child records as they are processed.

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
        visited: Object IDs for circular detection
        root_entity: Top-level entity name
        shared_flatten_cache: Cache of flattened objects
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        deterministic_id_fields: Dict mapping paths to field names for deterministic IDs
        id_generation_strategy: Custom function for ID generation
        id_field: Field name for extract ID
        parent_field: Field name for parent ID
        time_field: Field name for timestamp
        visit_arrays: Whether to treat array items as fields

    Returns:
        Tuple of (flattened main object, generator of (table_name, record) tuples)
    """
    # Initialize visited set if not provided
    if visited is None:
        visited = set()

    # Initialize shared flattening cache if not provided
    if shared_flatten_cache is None:
        shared_flatten_cache = {}

    # Initialize root_entity if at top level
    if root_entity is None:
        root_entity = entity_name

    # Prevent circular references
    obj_id = id(data)
    if obj_id in visited:
        # Return empty result for circular references
        empty_record = {}

        def empty_generator():
            yield from []

        return empty_record, empty_generator()

    visited.add(obj_id)

    # Check if we already flattened this object
    if obj_id in shared_flatten_cache:
        flattened_obj = shared_flatten_cache[obj_id]
    else:
        # Process this object - flatten it first
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
        )
        # Cache the result
        shared_flatten_cache[obj_id] = flattened_obj

    # Determine source field for deterministic ID generation
    source_field = None
    if deterministic_id_fields:
        # Normalize path for comparison
        normalized_path = parent_path.strip(separator) if parent_path else ""

        # Exact path match first
        if normalized_path in deterministic_id_fields:
            source_field = deterministic_id_fields[normalized_path]
        # Then try wildcard matches if exact match not found
        elif "*" in deterministic_id_fields:
            source_field = deterministic_id_fields["*"]
        # Try any path prefix matches (most specific to least)
        else:
            path_components = (
                normalized_path.split(separator) if normalized_path else []
            )
            for i in range(len(path_components), 0, -1):
                prefix = separator.join(path_components[:i])
                prefix_wildcard = f"{prefix}{separator}*"
                if prefix_wildcard in deterministic_id_fields:
                    source_field = deterministic_id_fields[prefix_wildcard]
                    break

    # Add metadata - use in_place for better performance
    annotated_obj = annotate_with_metadata(
        flattened_obj,
        parent_id=parent_id,
        extract_time=extract_time,
        in_place=True,
        source_field=source_field,
        id_generation_strategy=id_generation_strategy,
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
    )

    # Extract extract_id for use as parent_id in child records
    current_id = annotated_obj.get(id_field)

    # Create generator for extracting arrays
    def generate_child_records():
        # Skip array extraction if visit_arrays is False
        if not visit_arrays:
            return

        # Process each key-value pair in the data
        for key, value in data.items():
            # Process lists
            if isinstance(value, list):
                # Create sanitized key for path building
                sanitized_key = sanitize_name(key, separator, "")
                # Build current path
                current_path = (
                    f"{parent_path}{separator}{sanitized_key}"
                    if parent_path
                    else sanitized_key
                )

                # Get table name, possibly abbreviating it
                if abbreviate_table_names:
                    table_name = abbreviate_table_name(
                        path=current_path,
                        parent_entity=entity_name,
                        separator=separator,
                        max_component_length=max_table_component_length,
                        preserve_leaf=preserve_leaf_component,
                        abbreviation_dict=custom_abbreviations,
                    )
                else:
                    table_name = get_table_name(entity_name, current_path, separator)

                # Process each item in the list
                for item in value:
                    if isinstance(item, dict):
                        # Determine source field for this item's deterministic ID
                        item_source_field = None
                        if deterministic_id_fields:
                            # Normalize path for comparison
                            item_path = (
                                current_path.strip(separator) if current_path else ""
                            )

                            # Exact path match first
                            if item_path in deterministic_id_fields:
                                item_source_field = deterministic_id_fields[item_path]
                            # Then try wildcard matches if exact match not found
                            elif "*" in deterministic_id_fields:
                                item_source_field = deterministic_id_fields["*"]
                            # Try any path prefix matches (most specific to least)
                            else:
                                path_components = (
                                    item_path.split(separator) if item_path else []
                                )
                                for i in range(len(path_components), 0, -1):
                                    prefix = separator.join(path_components[:i])
                                    prefix_wildcard = f"{prefix}{separator}*"
                                    if prefix_wildcard in deterministic_id_fields:
                                        item_source_field = deterministic_id_fields[
                                            prefix_wildcard
                                        ]
                                        break

                        # Use cached flattened result if available
                        item_obj_id = id(item)
                        if item_obj_id in shared_flatten_cache:
                            flat_item = shared_flatten_cache[item_obj_id]
                        else:
                            # Flatten the array item
                            flat_item = flatten_json(
                                item,
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
                            )
                            # Cache the flattened result
                            shared_flatten_cache[item_obj_id] = flat_item

                        # Add metadata to the item
                        annotated_item = annotate_with_metadata(
                            flat_item,
                            parent_id=current_id,
                            extract_time=extract_time,
                            in_place=True,
                            source_field=item_source_field,
                            id_generation_strategy=id_generation_strategy,
                            id_field=id_field,
                            parent_field=parent_field,
                            time_field=time_field,
                        )

                        # Yield the table name and annotated item
                        yield (table_name, annotated_item)

                        # If item contains nested objects, process them recursively
                        if any(isinstance(v, dict) for v in item.values()) or any(
                            isinstance(v, list) for v in item.values()
                        ):
                            # Get the ID of this item for use as parent_id
                            item_id = annotated_item.get(id_field)

                            # Process nested structure
                            _, nested_gen = stream_process_structure(
                                data=item,
                                entity_name=entity_name,
                                parent_id=item_id,
                                parent_path=current_path,
                                separator=separator,
                                cast_to_string=cast_to_string,
                                include_empty=include_empty,
                                skip_null=skip_null,
                                extract_time=extract_time,
                                visited=visited,
                                root_entity=root_entity,
                                shared_flatten_cache=shared_flatten_cache,
                                abbreviate_table_names=abbreviate_table_names,
                                abbreviate_field_names=abbreviate_field_names,
                                max_table_component_length=max_table_component_length,
                                max_field_component_length=max_field_component_length,
                                preserve_leaf_component=preserve_leaf_component,
                                custom_abbreviations=custom_abbreviations,
                                deterministic_id_fields=deterministic_id_fields,
                                id_generation_strategy=id_generation_strategy,
                                id_field=id_field,
                                parent_field=parent_field,
                                time_field=time_field,
                                visit_arrays=visit_arrays,
                            )

                            # Yield nested records
                            for nested_table, nested_record in nested_gen:
                                yield (nested_table, nested_record)

            # Process nested objects
            elif isinstance(value, dict):
                # Create sanitized key for path building
                sanitized_key = sanitize_name(key, separator, "")
                # Build current path
                current_path = (
                    f"{parent_path}{separator}{sanitized_key}"
                    if parent_path
                    else sanitized_key
                )

                # Process nested structure
                _, nested_gen = stream_process_structure(
                    data=value,
                    entity_name=entity_name,
                    parent_id=current_id,
                    parent_path=current_path,
                    separator=separator,
                    cast_to_string=cast_to_string,
                    include_empty=include_empty,
                    skip_null=skip_null,
                    extract_time=extract_time,
                    visited=visited,
                    root_entity=root_entity,
                    shared_flatten_cache=shared_flatten_cache,
                    abbreviate_table_names=abbreviate_table_names,
                    abbreviate_field_names=abbreviate_field_names,
                    max_table_component_length=max_table_component_length,
                    max_field_component_length=max_field_component_length,
                    preserve_leaf_component=preserve_leaf_component,
                    custom_abbreviations=custom_abbreviations,
                    deterministic_id_fields=deterministic_id_fields,
                    id_generation_strategy=id_generation_strategy,
                    id_field=id_field,
                    parent_field=parent_field,
                    time_field=time_field,
                    visit_arrays=visit_arrays,
                )

                # Yield nested records
                for nested_table, nested_record in nested_gen:
                    yield (nested_table, nested_record)

    # Return the annotated object and generator
    return annotated_obj, generate_child_records()
