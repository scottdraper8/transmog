"""
Hierarchy processing module for nested JSON structures.

Provides functions to process complete JSON structures with
parent-child relationship preservation.
"""

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Callable,
    Generator,
    Iterator,
    Union,
)
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
from transmog.error import logger

# Type aliases
JsonDict = Dict[str, Any]
FlatDict = Dict[str, Any]
ArrayDict = Dict[str, List[Dict[str, Any]]]
ProcessResult = Tuple[FlatDict, ArrayDict]
StreamingChildTables = Generator[Tuple[str, List[FlatDict]], None, None]
ArrayResult = Union[ArrayDict, StreamingChildTables]


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
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    visit_arrays: bool = True,
    streaming: bool = False,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
) -> Tuple[FlatDict, ArrayResult]:
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
        root_entity: Top-level entity name
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        deterministic_id_fields: Dict mapping paths to field names for deterministic IDs
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
    # Initialize root_entity if at top level
    if root_entity is None:
        root_entity = entity_name

    # Check for empty data cases
    if data is None:
        empty_result = {
            id_field: generate_deterministic_id(str(entity_name) + "_empty"),
            time_field: extract_time or datetime.now(),
            "__original_entity": entity_name,
        }
        if parent_id:
            empty_result[parent_field] = parent_id

        # Return empty result based on streaming mode
        if streaming:

            def empty_generator():
                # Empty generator that yields nothing
                if (
                    False
                ):  # This is never executed but maintains the generator signature
                    yield "", {}
                return

            return empty_result, empty_generator()
        else:
            return empty_result, {}

    # Process this object - flatten it
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
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
        in_place=True,
        source_field=source_field,
        id_generation_strategy=id_generation_strategy,
    )

    # Skip array processing if visit_arrays is False
    if not visit_arrays:
        if streaming:

            def empty_generator():
                if False:  # Never executed
                    yield "", {}
                return

            return annotated_obj, empty_generator()
        else:
            return annotated_obj, {}

    # Handle array extraction based on streaming mode
    extract_id = annotated_obj.get(id_field)

    if streaming:
        # Define a generator function to yield child tables
        def generate_child_records():
            # Extract arrays as a generator to avoid building all in memory
            for table_name, records in extract_arrays(
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
                default_id_field=deterministic_id_fields,
                id_generation_strategy=id_generation_strategy,
                streaming=True,
                recovery_strategy=recovery_strategy,
                max_depth=max_depth,
            ).items():
                if records:  # Skip empty tables
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
            default_id_field=deterministic_id_fields,
            id_generation_strategy=id_generation_strategy,
            recovery_strategy=recovery_strategy,
            max_depth=max_depth,
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
    # Process in batches for better memory efficiency
    all_flattened_records = []
    combined_arrays = {}

    # Initialize function arguments
    args = {**kwargs}

    # Check for common parameters that are often needed
    if "extract_time" not in args:
        args["extract_time"] = kwargs.get("extract_time", datetime.now())

    # Ensure we get a dictionary of arrays, not a generator
    args["streaming"] = False

    # Process each batch
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]

        # Process each record in the batch
        for record in batch:
            # Process the record
            try:
                flattened_record, arrays = process_structure(
                    record, entity_name=entity_name, **args
                )
                all_flattened_records.append(flattened_record)

                # Combine arrays into a unified table mapping
                for array_name, array_records in arrays.items():
                    if array_name in combined_arrays:
                        combined_arrays[array_name].extend(array_records)
                    else:
                        combined_arrays[array_name] = array_records
            except Exception as e:
                # Check if we have a recovery strategy
                recovery_strategy = args.get("recovery_strategy")
                if recovery_strategy and hasattr(recovery_strategy, "recover"):
                    try:
                        # Simple recovery - return empty dict for record and continue
                        recovery_result = recovery_strategy.recover(
                            e, entity_name=entity_name
                        )
                        if recovery_result is not None:
                            all_flattened_records.append({})
                        continue
                    except Exception:
                        # If recovery fails, just propagate the original error
                        pass
                # If no recovery or recovery failed, reraise the original error
                raise

    return all_flattened_records, combined_arrays


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
    visit_arrays: bool = True,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    default_id_field: Optional[str] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    **kwargs,
) -> Tuple[List[FlatDict], ArrayDict]:
    """
    Process a list of records in a single pass, with optimized memory usage.

    This is an optimized version of process_record_batch that processes
    all records in a single pass, building array tables as it goes.

    Args:
        records: List of JSON records to process
        entity_name: Entity name
        extract_time: Extraction timestamp
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        visit_arrays: Whether to visit arrays
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        default_id_field: Default ID field name for deterministic IDs
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth for nested structures
        **kwargs: Additional arguments to pass to process_structure

    Returns:
        Tuple of (list of flattened main records, array tables dictionary)
    """
    if extract_time is None:
        extract_time = datetime.now()

    # Convert default_id_field to deterministic_id_fields format
    deterministic_id_fields = {}
    if default_id_field:
        deterministic_id_fields = {"": default_id_field}
    elif "deterministic_id_fields" in kwargs:
        deterministic_id_fields = kwargs["deterministic_id_fields"]

    # Process all records
    all_flattened = []
    all_arrays = {}

    # Set streaming to False to get dictionary result
    processing_args = {
        "separator": separator,
        "cast_to_string": cast_to_string,
        "include_empty": include_empty,
        "skip_null": skip_null,
        "extract_time": extract_time,
        "id_field": id_field,
        "parent_field": parent_field,
        "time_field": time_field,
        "visit_arrays": visit_arrays,
        "abbreviate_table_names": abbreviate_table_names,
        "abbreviate_field_names": abbreviate_field_names,
        "max_table_component_length": max_table_component_length,
        "max_field_component_length": max_field_component_length,
        "preserve_leaf_component": preserve_leaf_component,
        "custom_abbreviations": custom_abbreviations,
        "deterministic_id_fields": deterministic_id_fields,
        "id_generation_strategy": id_generation_strategy,
        "recovery_strategy": recovery_strategy,
        "streaming": False,
        "max_depth": max_depth,
        **kwargs,
    }

    for record in records:
        try:
            # Process single record
            # Filter out incompatible parameters
            filtered_args = {
                k: v
                for k, v in processing_args.items()
                if k != "preserve_root_component"
            }

            flattened, arrays = process_structure(
                data=record, entity_name=entity_name, **filtered_args
            )

            # Add to result collections
            all_flattened.append(flattened)

            # Combine arrays into a unified table mapping
            for array_name, array_records in arrays.items():
                if array_name in all_arrays:
                    all_arrays[array_name].extend(array_records)
                else:
                    all_arrays[array_name] = array_records

        except Exception as e:
            # If we have a recovery strategy, use it
            if recovery_strategy and hasattr(recovery_strategy, "recover"):
                try:
                    recovery_result = recovery_strategy.recover(
                        e, entity_name=entity_name, data=record
                    )
                    if recovery_result is not None:
                        # Add minimal valid record
                        empty_record = {
                            id_field: generate_deterministic_id(
                                str(entity_name) + "_error"
                            ),
                            time_field: extract_time,
                            "__extract_source": "error_recovery",
                            "__original_entity": entity_name,
                        }
                        all_flattened.append(empty_record)
                    continue
                except Exception as re:
                    # If recovery itself fails, propagate original error
                    # but with diagnostic info about the recovery failure
                    logger.warning(f"Recovery failed: {re}")
            raise

    return all_flattened, all_arrays


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
    visit_arrays: bool = True,
    abbreviate_table_names: bool = True,
    abbreviate_field_names: bool = True,
    max_table_component_length: int = None,
    max_field_component_length: int = None,
    preserve_leaf_component: bool = True,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
    use_deterministic_ids: bool = False,
    max_depth: int = 100,
    **kwargs,
) -> Tuple[List[FlatDict], StreamingChildTables]:
    """
    Stream process a list of records, yielding child tables as they are processed.

    This function processes records and streams child tables one by one, which
    is more memory-efficient for large datasets.

    Args:
        records: List of JSON records to process
        entity_name: Entity name
        extract_time: Extraction timestamp
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        visit_arrays: Whether to visit arrays
        abbreviate_table_names: Whether to abbreviate table names
        abbreviate_field_names: Whether to abbreviate field names
        max_table_component_length: Maximum length for table name components
        max_field_component_length: Maximum length for field name components
        preserve_leaf_component: Whether to preserve leaf components
        custom_abbreviations: Custom abbreviation dictionary
        deterministic_id_fields: Dict mapping paths to field names for deterministic IDs
        id_generation_strategy: Custom function for ID generation
        use_deterministic_ids: Whether to use deterministic IDs
        max_depth: Maximum recursion depth for nested structures
        **kwargs: Additional arguments to pass to process_structure

    Returns:
        Tuple of (list of flattened main records, generator of child tables)
    """
    if extract_time is None:
        extract_time = datetime.now()

    # Convert use_deterministic_ids to deterministic_id_fields
    if use_deterministic_ids and deterministic_id_fields is None:
        # If we want deterministic IDs but no fields specified, use the ID field for all
        deterministic_id_fields = {"": id_field}

    # Process all main records first
    all_flattened = []
    all_child_data = {}

    # Setup processing arguments
    processing_args = {
        "separator": separator,
        "cast_to_string": cast_to_string,
        "include_empty": include_empty,
        "skip_null": skip_null,
        "extract_time": extract_time,
        "id_field": id_field,
        "parent_field": parent_field,
        "time_field": time_field,
        "visit_arrays": visit_arrays,
        "abbreviate_table_names": abbreviate_table_names,
        "abbreviate_field_names": abbreviate_field_names,
        "max_table_component_length": max_table_component_length,
        "max_field_component_length": max_field_component_length,
        "preserve_leaf_component": preserve_leaf_component,
        "custom_abbreviations": custom_abbreviations,
        "deterministic_id_fields": deterministic_id_fields,
        "id_generation_strategy": id_generation_strategy,
        "recovery_strategy": recovery_strategy,
        "streaming": False,
        "max_depth": max_depth,
        **kwargs,
    }

    # Process the main records
    for record in records:
        try:
            # Process the record
            flattened, arrays = process_structure(
                data=record, entity_name=entity_name, **processing_args
            )

            # Add the main record to results
            all_flattened.append(flattened)

            # Collect child tables
            for table_name, records in arrays.items():
                if table_name not in all_child_data:
                    all_child_data[table_name] = []
                all_child_data[table_name].extend(records)

        except Exception as e:
            # Handle errors similarly to the batch version
            recovery_strategy = kwargs.get("recovery_strategy")
            if recovery_strategy and hasattr(recovery_strategy, "recover"):
                try:
                    recovery_result = recovery_strategy.recover(
                        e, entity_name=entity_name, data=record
                    )
                    if recovery_result is not None:
                        # Add minimal valid record
                        empty_record = {
                            id_field: generate_deterministic_id(
                                str(entity_name) + "_error"
                            ),
                            time_field: extract_time,
                            "__extract_source": "error_recovery",
                            "__original_entity": entity_name,
                        }
                        all_flattened.append(empty_record)
                    continue
                except Exception as re:
                    logger.warning(f"Recovery failed: {re}")
            raise

    # Define a generator function to yield child tables
    def child_tables_generator():
        # Process child records for each record
        for i, record in enumerate(records):
            try:
                # Process this record with streaming enabled
                _, arrays_gen = process_structure(
                    record,
                    entity_name=entity_name,
                    extract_time=extract_time,
                    separator=separator,
                    cast_to_string=cast_to_string,
                    include_empty=include_empty,
                    skip_null=skip_null,
                    id_field=id_field,
                    parent_field=parent_field,
                    time_field=time_field,
                    visit_arrays=visit_arrays,
                    abbreviate_table_names=abbreviate_table_names,
                    abbreviate_field_names=abbreviate_field_names,
                    max_table_component_length=max_table_component_length,
                    max_field_component_length=max_field_component_length,
                    preserve_leaf_component=preserve_leaf_component,
                    custom_abbreviations=custom_abbreviations,
                    deterministic_id_fields=deterministic_id_fields,
                    id_generation_strategy=id_generation_strategy,
                    streaming=True,
                    recovery_strategy=recovery_strategy,
                    max_depth=max_depth,
                    **kwargs,
                )

                # Yield all child tables from this record
                for table_name, table_records in arrays_gen:
                    yield table_name, table_records

            except Exception as e:
                # Handle errors with recovery strategy if provided
                if recovery_strategy and hasattr(recovery_strategy, "recover"):
                    try:
                        recovery_strategy.recover(
                            e, entity_name=entity_name, data=record
                        )
                        # Continue to next record
                        continue
                    except Exception as re:
                        logger.warning(f"Recovery failed: {re}")
                # Re-raise if no recovery or recovery failed
                raise

    return all_flattened, child_tables_generator()
