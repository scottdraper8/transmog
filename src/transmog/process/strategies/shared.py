"""Shared batch processing utilities for processing strategies."""

from typing import Any, Callable, Optional, Union

from ...core.extractor import extract_arrays
from ...core.hierarchy import process_records_in_single_pass
from ...core.memory import get_global_gc_manager, get_global_memory_monitor
from ...core.metadata import annotate_with_metadata
from ...error import (
    ProcessingError,
    build_error_context,
    format_error_message,
    get_recovery_strategy,
    logger,
)
from ..result import ProcessingResult


def process_batch_main_records(
    strategy_instance: Any,
    batch: list[dict[str, Any]],
    entity_name: str,
    extract_time: Any,
    result: ProcessingResult,
    id_field: str,
    parent_field: str,
    time_field: str,
    default_id_field: Optional[Union[str, dict[str, str]]],
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]],
    params: dict[str, Any],
) -> list[str]:
    """Process main records for a batch with memory optimization.

    Args:
        strategy_instance: The strategy instance calling this function
        batch: Batch of records to process
        entity_name: Name of the entity being processed
        extract_time: Extraction timestamp
        result: ProcessingResult to store results
        id_field: Field name for record IDs
        parent_field: Field name for parent IDs
        time_field: Field name for timestamps
        default_id_field: Default ID field configuration
        id_generation_strategy: Strategy for generating IDs
        params: Processing parameters

    Returns:
        List of generated record IDs
    """
    # Get memory monitoring instances
    memory_monitor = get_global_memory_monitor()
    gc_manager = get_global_gc_manager()

    # Remove array fields from records if not keeping arrays (in-place optimization)
    if not params.get("keep_arrays", False):
        for record in batch:
            strategy_instance._remove_array_fields_from_record(record)

        # Process records in single pass with memory optimization
    nested_threshold_value = params.get("nested_threshold")
    if nested_threshold_value is not None:
        nested_threshold_value = int(nested_threshold_value)
    else:
        nested_threshold_value = 4  # Default value

    processed_records, child_arrays = process_records_in_single_pass(
        batch,
        entity_name=entity_name,
        separator=params["separator"],
        nested_threshold=nested_threshold_value,
        cast_to_string=params.get("cast_to_string", True),
        include_empty=params.get("include_empty", False),
        skip_null=params.get("skip_null", True),
        id_field=id_field,
        parent_field=parent_field,
        time_field=time_field,
        default_id_field=default_id_field,
        id_generation_strategy=id_generation_strategy,
        transmog_time=extract_time,
        max_depth=params.get("max_depth", 100),
    )

    # Add child arrays to result if any
    if child_arrays:
        result.add_child_tables(child_arrays)

    # Add processed records to result with in-place annotation
    main_ids = []
    for processed_record in processed_records:
        # Annotate with metadata in-place for memory efficiency
        annotated_record = annotate_with_metadata(
            processed_record,
            transmog_time=extract_time,
            id_field=id_field,
            time_field=time_field,
            parent_field=parent_field,
            id_generation_strategy=id_generation_strategy,
            in_place=True,  # Use in-place modification
        )

        result.add_main_record(annotated_record)
        main_ids.append(annotated_record.get(id_field, ""))

    # Strategic garbage collection after batch processing
    if memory_monitor.should_reduce_usage():
        collected = gc_manager.collect_after_batch()
        if collected > 0:
            logger.debug(
                f"Memory optimization: collected {collected} objects "
                f"after batch processing"
            )

    return main_ids


def process_batch_arrays(
    strategy_instance: Any,
    batch: list[dict[str, Any]],
    entity_name: str,
    extract_time: Any,
    result: ProcessingResult,
    id_field: str,
    main_ids: list[str],
    default_id_field: Optional[Union[str, dict[str, str]]],
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]],
    params: dict[str, Any],
) -> None:
    """Process arrays for a batch of records.

    Args:
        strategy_instance: The strategy instance calling this function
        batch: Batch of records to process
        entity_name: Name of the entity being processed
        extract_time: Extraction timestamp
        result: ProcessingResult to store results
        id_field: Field name for record IDs
        main_ids: List of main record IDs
        default_id_field: Default ID field configuration
        id_generation_strategy: Strategy for generating IDs
        params: Processing parameters
    """
    if not params.get("visit_arrays", True):
        return

    parent_field = params["parent_field"]
    time_field = params["time_field"]

    for i, record in enumerate(batch):
        if i >= len(main_ids):
            logger.warning(
                f"Skipping array processing for record {i}: no corresponding main ID"
            )
            continue

        main_id = main_ids[i]

        # Extract arrays from the record
        try:
            nested_threshold_value = params.get("nested_threshold")
            if nested_threshold_value is not None:
                nested_threshold_value = int(nested_threshold_value)
            else:
                nested_threshold_value = 4  # Default value

            arrays = extract_arrays(
                record,
                entity_name=entity_name,
                separator=params["separator"],
                nested_threshold=nested_threshold_value,
                cast_to_string=params.get("cast_to_string", True),
                include_empty=params.get("include_empty", False),
                skip_null=params.get("skip_null", True),
                parent_id=main_id,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                transmog_time=extract_time,
                max_depth=params.get("max_depth", 100),
            )

            # Add arrays to result
            if arrays:
                result.add_child_tables(arrays)

        except Exception as e:
            # Handle errors using standardized recovery strategy
            strategy = get_recovery_strategy(params.get("recovery_strategy", "strict"))
            context = build_error_context(
                entity_name=f"record_{i}",
                entity_type="record",
                operation="array extraction",
                source=entity_name,
                record_index=i,
            )

            try:
                recovery_result = strategy.recover(e, **context)
                if recovery_result is not None:
                    # Log the recovery and continue
                    logger.warning(
                        format_error_message("array_processing", e, **context)
                        + f" - recovered with: {recovery_result}"
                    )
                # Continue processing other records
                continue
            except Exception:
                # Re-raise with formatted message
                error_msg = format_error_message("array_processing", e, **context)
                raise ProcessingError(error_msg) from e
