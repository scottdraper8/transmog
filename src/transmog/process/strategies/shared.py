"""Shared batch processing utilities for processing strategies."""

from typing import Any

from transmog.config import TransmogConfig
from transmog.core.hierarchy import process_records_in_single_pass
from transmog.core.memory import get_global_gc_manager, get_global_memory_monitor
from transmog.core.metadata import annotate_with_metadata
from transmog.error import logger
from transmog.process.result import ProcessingResult
from transmog.types import ProcessingContext


def process_batch_main_records(
    strategy_instance: Any,
    batch: list[dict[str, Any]],
    entity_name: str,
    config: TransmogConfig,
    context: ProcessingContext,
    result: ProcessingResult,
) -> list[str]:
    """Process main records for a batch with memory optimization.

    Args:
        strategy_instance: The strategy instance calling this function
        batch: Batch of records to process
        entity_name: Name of the entity being processed
        config: Configuration settings
        context: Processing context
        result: ProcessingResult to store results

    Returns:
        List of generated record IDs
    """
    memory_monitor = get_global_memory_monitor()
    gc_manager = get_global_gc_manager()

    processed_records, child_arrays = process_records_in_single_pass(
        batch,
        entity_name=entity_name,
        config=config,
        context=context,
    )

    if child_arrays:
        result.add_child_tables(child_arrays)

    main_ids = []
    for processed_record in processed_records:
        annotated_record = annotate_with_metadata(
            processed_record,
            config=config,
            transmog_time=context.extract_time,
            in_place=True,
        )

        result.add_main_record(annotated_record)
        main_ids.append(annotated_record.get(config.id_field, ""))

    # Strategic garbage collection after batch processing
    if memory_monitor.should_reduce_usage():
        collected = gc_manager.collect_after_batch()
        if collected > 0:
            logger.debug(
                f"Memory optimization: collected {collected} objects "
                f"after batch processing"
            )

    return main_ids
