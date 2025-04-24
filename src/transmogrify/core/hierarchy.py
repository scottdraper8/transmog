"""
Hierarchy processing module for nested JSON structures.

Provides functions to process complete JSON structures with
parent-child relationship preservation.
"""

from typing import Any, Dict, List, Optional, Set, Tuple, Callable

from src.transmogrify.core.extractor import extract_arrays
from src.transmogrify.core.flattener import flatten_json
from src.transmogrify.core.metadata import (
    annotate_with_metadata,
    create_batch_metadata,
    generate_deterministic_id,
)
from src.transmogrify.config.settings import settings

# Type aliases
JsonDict = Dict[str, Any]
FlatDict = Dict[str, Any]
ArrayDict = Dict[str, List[Dict[str, Any]]]
ProcessResult = Tuple[FlatDict, ArrayDict]
VisitedPath = Set[int]


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
        deterministic_id_fields=deterministic_id_fields,
        id_generation_strategy=id_generation_strategy,
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
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    **kwargs,
) -> Tuple[List[FlatDict], ArrayDict]:
    """
    Process a list of records in a single pass, optimized for consistency and performance.

    This function processes all records in a single pass, which is more efficient
    when all records can fit in memory and have similar structure.

    Args:
        records: List of JSON records to process
        entity_name: Name of the entity for table naming
        extract_time: Timestamp for all records
        separator: Separator for flattened field names
        cast_to_string: Whether to cast values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        id_field: Field name for record ID
        parent_field: Field name for parent ID
        time_field: Field name for timestamp
        visit_arrays: Whether to treat arrays as simple values
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        deterministic_id_fields: Dict mapping paths to field names for deterministic IDs
        id_generation_strategy: Custom function for ID generation
        **kwargs: Additional options

    Returns:
        Tuple of (flattened records, child tables)
    """
    if not records:
        return [], {}

    # Set defaults from settings if needed
    if max_table_component_length is None:
        max_table_component_length = settings.get_option("max_table_component_length")

    if max_field_component_length is None:
        max_field_component_length = settings.get_option("max_field_component_length")

    # Prepare shared processing context
    shared_flatten_cache = {}
    all_arrays: Dict[str, List[Dict[str, Any]]] = {}

    # Flatten all records first (more efficient cache usage)
    flattened_records = []
    for record in records:
        obj_id = id(record)
        if obj_id not in shared_flatten_cache:
            flattened = flatten_json(
                record,
                parent_path="",
                separator=separator,
                cast_to_string=cast_to_string,
                include_empty=include_empty,
                skip_arrays=True,
                skip_null=skip_null,
                abbreviate_field_names=abbreviate_field_names,
                max_field_component_length=max_field_component_length,
                preserve_leaf_component=preserve_leaf_component,
                custom_abbreviations=custom_abbreviations,
            )
            shared_flatten_cache[obj_id] = flattened
        else:
            flattened = shared_flatten_cache[obj_id]

        flattened_records.append(flattened)

    # Determine source field for root-level deterministic ID generation
    source_field = None
    if deterministic_id_fields:
        # Look for root path
        if "" in deterministic_id_fields:
            source_field = deterministic_id_fields[""]
        # Try wildcard match
        elif "*" in deterministic_id_fields:
            source_field = deterministic_id_fields["*"]

    # Add metadata to all records at once (more efficient)
    annotated_records = []
    for i, record in enumerate(records):
        original_record = records[i]  # Original record for field value lookup
        flattened_record = flattened_records[i]  # Flattened record to annotate

        # For deterministic IDs, we need to check if the source field exists in the original record
        deterministic_source_value = None
        if source_field and source_field in original_record:
            deterministic_source_value = original_record.get(source_field)

        # Generate extract ID deterministically if needed
        extract_id = None
        if deterministic_source_value and source_field:
            extract_id = generate_deterministic_id(deterministic_source_value)
        elif id_generation_strategy:
            try:
                extract_id = id_generation_strategy(original_record)
            except Exception:
                extract_id = None  # Will fall back to random UUID

        # Annotate the flattened record with metadata
        annotated = annotate_with_metadata(
            flattened_record,
            extract_time=extract_time,
            in_place=True,
            id_field=id_field,
            parent_field=parent_field,
            time_field=time_field,
            extract_id=extract_id,  # Use pre-generated ID if available
        )
        annotated_records.append(annotated)

    # Extract arrays from all records
    visited = set()
    for i, record in enumerate(records):
        parent_id = annotated_records[i].get(id_field)

        arrays = extract_arrays(
            record,
            parent_id=parent_id,
            entity_name=entity_name,
            extract_time=extract_time,
            visited=visited,
            shared_flatten_cache=shared_flatten_cache,
            separator=separator,
            cast_to_string=cast_to_string,
            include_empty=include_empty,
            skip_null=skip_null,
            abbreviate_enabled=abbreviate_table_names,
            max_component_length=max_table_component_length,
            preserve_leaf=preserve_leaf_component,
            custom_abbreviations=custom_abbreviations,
            deterministic_id_fields=deterministic_id_fields,
            id_generation_strategy=id_generation_strategy,
        )

        # Combine arrays efficiently
        for array_name, array_items in arrays.items():
            if array_items:
                all_arrays.setdefault(array_name, []).extend(array_items)

    return annotated_records, all_arrays
