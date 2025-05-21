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
from transmog.types.base import ArrayDict, FlatDict, JsonDict

# Type aliases
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
    deeply_nested_threshold: int = 4,
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
        deeply_nested_threshold: Threshold for when to consider a path deeply nested
            (default 4)
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
        deeply_nested_threshold=deeply_nested_threshold,
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

            # Path resolution with simplified naming scheme
            if normalized_path in default_id_field:
                source_field = default_id_field[normalized_path]
            elif "*" in default_id_field:
                source_field = default_id_field["*"]
            else:
                # Try matching based on path components without level numbers
                for path_key in default_id_field:
                    if path_key.endswith("*"):
                        prefix = path_key[:-1]
                        if normalized_path.startswith(prefix):
                            source_field = default_id_field[path_key]
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
        # Stream extraction version
        return annotated_obj, stream_extract_arrays(
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
            deeply_nested_threshold=deeply_nested_threshold,
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            recovery_strategy=recovery_strategy,
            max_depth=max_depth,
        )
    else:
        # Batch extraction version
        return annotated_obj, extract_arrays(
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
            deeply_nested_threshold=deeply_nested_threshold,
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            recovery_strategy=recovery_strategy,
            max_depth=max_depth,
        )


def process_record_batch(
    records: list[JsonDict], entity_name: str, batch_size: int = 100, **kwargs: Any
) -> tuple[list[FlatDict], ArrayDict]:
    """Process a batch of records separately and combine the results.

    This is a convenience function to process multiple top-level
    records with a single function call.

    Args:
        records: List of top-level records to process
        entity_name: Entity name for all records
        batch_size: Number of records to process in a single batch
        **kwargs: Additional keyword args to pass to process_structure

    Returns:
        Tuple of (flattened main objects, combined array tables)
    """
    # Process records in smaller batches for better memory usage
    return process_records_in_single_pass(records, entity_name=entity_name, **kwargs)


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
    deeply_nested_threshold: int = 4,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    recovery_strategy: Optional[Any] = None,
    max_depth: int = 100,
    **kwargs: Any,
) -> tuple[list[FlatDict], ArrayDict]:
    r"""Process multiple records and combine the results.

    This function processes all records in a single pass, optimizing for throughput
    over memory usage.

    Args:
        records: List of records to process
        entity_name: Entity name for all records
        extract_time: Extraction timestamp
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        visit_arrays: Whether to visit and process arrays
        deeply_nested_threshold: Threshold for when to consider a path deeply
            nested (default 4)
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        recovery_strategy: Strategy for error recovery
        max_depth: Maximum recursion depth for nested structures
        **kwargs: Additional keyword args

    Returns:
        Tuple of (flattened main objects, array tables)
    """
    # Timestamp for all records in batch if not provided
    if extract_time is None:
        extract_time = datetime.now()

    # Initialize results
    main_records: list[FlatDict] = []
    arrays: ArrayDict = {}

    # Process each record
    for i, record in enumerate(records):
        try:
            # Process a single record - Don't pass batch_size to process_structure
            process_kwargs = {
                k: v
                for k, v in kwargs.items()
                if k != "batch_size" and k != "use_deterministic_ids"
            }
            main_record, child_tables = process_structure(
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
                deeply_nested_threshold=deeply_nested_threshold,
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=recovery_strategy,
                max_depth=max_depth,
                **process_kwargs,
            )

            # Add main record to results
            main_records.append(main_record)

            # Merge child tables
            if isinstance(child_tables, dict):
                for table_name, records in child_tables.items():
                    if table_name not in arrays:
                        arrays[table_name] = []
                    arrays[table_name].extend(records)

        except Exception as e:
            # Apply recovery strategy if available
            if recovery_strategy and hasattr(recovery_strategy, "recover"):
                try:
                    recovery_result = recovery_strategy.recover(
                        e, entity_name=entity_name, path=[], record_index=i
                    )
                    if recovery_result is not None:
                        logger.info(
                            f"Recovery successful for record {i} in batch processing"
                        )
                        if (
                            isinstance(recovery_result, tuple)
                            and len(recovery_result) == 2
                        ):
                            rec, child_tabs = recovery_result
                            main_records.append(rec)
                            for table_name, records in child_tabs.items():
                                if table_name not in arrays:
                                    arrays[table_name] = []
                                arrays[table_name].extend(records)
                        continue
                except Exception as re:
                    logger.warning(f"Recovery failed during batch processing: {re}")
            # Re-raise original exception if recovery failed
            logger.error(f"Error processing record {i} in batch: {e}")
            raise

    return main_records, arrays


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
    deeply_nested_threshold: int = 4,
    default_id_field: Optional[Union[str, dict[str, str]]] = None,
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None,
    max_depth: int = 100,
    **kwargs: Any,
) -> tuple[list[FlatDict], StreamingChildTables]:
    r"""Process records in streaming mode.

    This function is memory-efficient, yielding results from child tables
    as they are processed instead of accumulating them in memory.

    Args:
        records: List of records to process
        entity_name: Entity name for all records
        extract_time: Extraction timestamp
        separator: Separator for path components
        cast_to_string: Whether to cast all values to strings
        include_empty: Whether to include empty values
        skip_null: Whether to skip null values
        id_field: ID field name
        parent_field: Parent ID field name
        time_field: Timestamp field name
        visit_arrays: Whether to visit and process arrays
        deeply_nested_threshold: Threshold for when to consider a path deeply
            nested (default 4)
        default_id_field: Field name or dict mapping paths to field names for
            deterministic IDs
        id_generation_strategy: Custom function for ID generation
        max_depth: Maximum recursion depth for nested structures
        **kwargs: Additional keyword args

    Returns:
        Tuple of (flattened main objects, generator for array tables)
    """
    # Timestamp for all records in batch if not provided
    if extract_time is None:
        extract_time = datetime.now()

    # Process each record with streaming enabled
    main_records: list[FlatDict] = []
    all_generators: list[Generator[tuple[str, list[dict[str, Any]]], None, None]] = []

    for record in records:
        # Don't pass batch_size to process_structure
        process_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k != "batch_size" and k != "use_deterministic_ids"
        }
        main_record, child_generator = process_structure(
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
            deeply_nested_threshold=deeply_nested_threshold,
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            streaming=True,  # Enable streaming mode
            max_depth=max_depth,
            **process_kwargs,
        )

        # Add main record to results
        main_records.append(main_record)

        # Store generator for later use
        if isinstance(child_generator, Generator):
            all_generators.append(child_generator)

    # Convert list of generators to single generator that merges all results
    def dict_to_generator(
        d: dict[str, list[dict[str, Any]]],
    ) -> Generator[tuple[str, list[dict[str, Any]]], None, None]:
        """Convert a dictionary to a generator of (key, value) pairs."""
        for key, value in d.items():
            if value:  # Only yield non-empty lists
                yield key, value

    # Define a generator function that combines results from all generators
    def child_tables_generator() -> Generator[
        tuple[str, list[dict[str, Any]]], None, None
    ]:
        # Collect records for each table
        all_tables: dict[str, list[dict[str, Any]]] = {}

        # Process each generator
        for gen in all_generators:
            try:
                for table_name, records in gen:
                    if table_name not in all_tables:
                        all_tables[table_name] = []

                    # Ensure records is a list of dictionaries (array records)
                    if isinstance(records, list):
                        all_tables[table_name].extend(records)
                    elif isinstance(records, dict):
                        all_tables[table_name].append(records)
            except StopIteration:
                continue

        # Yield all collected records
        yield from dict_to_generator(all_tables)

    return main_records, child_tables_generator()
