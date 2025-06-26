"""Core functionality for Transmog data processing.

This package contains the core processing functions for flattening, extraction,
and handling complex data structures.
"""

from .extractor import extract_arrays, stream_extract_arrays
from .flattener import clear_caches, flatten_json, refresh_cache_config
from .hierarchy import (
    process_record_batch,
    process_records_in_single_pass,
    process_structure,
    stream_process_records,
)
from .id_discovery import (
    DEFAULT_ID_FIELD_PATTERNS,
    build_id_field_mapping,
    discover_id_field,
    get_record_id,
    should_add_transmog_id,
)
from .metadata import (
    TRANSMOG_NAMESPACE,
    annotate_with_metadata,
    create_batch_metadata,
    generate_composite_id,
    generate_deterministic_id,
    generate_transmog_id,
    get_current_timestamp,
)

__all__ = [
    # Array extraction
    "extract_arrays",
    "stream_extract_arrays",
    # JSON flattening
    "flatten_json",
    "clear_caches",
    "refresh_cache_config",
    # Metadata generation
    "generate_transmog_id",
    "generate_deterministic_id",
    "generate_composite_id",
    "get_current_timestamp",
    "annotate_with_metadata",
    "create_batch_metadata",
    "TRANSMOG_NAMESPACE",
    # Hierarchy
    "process_structure",
    "process_record_batch",
    "process_records_in_single_pass",
    "stream_process_records",
    # ID discovery
    "DEFAULT_ID_FIELD_PATTERNS",
    "build_id_field_mapping",
    "discover_id_field",
    "get_record_id",
    "should_add_transmog_id",
]
