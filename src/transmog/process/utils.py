"""
Utility functions for the Transmog processor.

This module contains common utility functions used across the process module.
"""

from typing import Any, Dict, Optional

from ..core.metadata import get_current_timestamp


def get_common_config_params(
    processor, extract_time: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Get common configuration parameters used across processing methods.

    Args:
        processor: Processor instance
        extract_time: Optional extraction timestamp override

    Returns:
        Dictionary of common configuration parameters
    """
    # Use current timestamp if not provided
    if extract_time is None:
        extract_time = get_current_timestamp()

    return {
        # Naming config
        "separator": processor.config.naming.separator,
        "abbreviate_table_names": processor.config.naming.abbreviate_table_names,
        "abbreviate_field_names": processor.config.naming.abbreviate_field_names,
        "max_table_component_length": processor.config.naming.max_table_component_length,
        "max_field_component_length": processor.config.naming.max_field_component_length,
        "preserve_leaf_component": processor.config.naming.preserve_leaf_component,
        "custom_abbreviations": processor.config.naming.custom_abbreviations,
        # Processing config
        "cast_to_string": processor.config.processing.cast_to_string,
        "include_empty": processor.config.processing.include_empty,
        "skip_null": processor.config.processing.skip_null,
        "visit_arrays": processor.config.processing.visit_arrays,
        # Metadata config
        "id_field": processor.config.metadata.id_field,
        "parent_field": processor.config.metadata.parent_field,
        "time_field": processor.config.metadata.time_field,
        "deterministic_id_fields": processor.config.metadata.deterministic_id_fields,
        "id_generation_strategy": processor.config.metadata.id_generation_strategy,
        # Timestamps
        "extract_time": extract_time,
    }


def get_batch_size(processor, chunk_size: Optional[int] = None) -> int:
    """
    Get the batch size, using provided value or config default.

    Args:
        processor: Processor instance
        chunk_size: Optional chunk size override

    Returns:
        Batch size to use
    """
    return chunk_size or processor.config.processing.batch_size
