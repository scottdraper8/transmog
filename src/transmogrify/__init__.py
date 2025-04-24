"""
Transmogrify: JSON to tabular normalization and flattening utility

A library for transforming nested JSON structures into flattened formats
with parent-child relationship preservation and metadata annotation.
"""

__version__ = "0.1.0"

# Imports for use within the package itself
from src.transmogrify.core.flattener import flatten_json
from src.transmogrify.core.extractor import extract_arrays
from src.transmogrify.core.hierarchy import (
    process_structure,
    process_record_batch,
    process_records_in_single_pass,
)
from src.transmogrify.core.metadata import (
    generate_extract_id,
    annotate_with_metadata,
    get_current_timestamp,
    create_batch_metadata,
)
from src.transmogrify.naming.conventions import (
    get_table_name,
    sanitize_name,
    get_standard_field_name,
)
from src.transmogrify.naming.abbreviator import (
    abbreviate_table_name,
    abbreviate_field_name,
    get_common_abbreviations,
    merge_abbreviation_dicts,
)

# High-level processor class for one-step processing
from src.transmogrify.processor import Processor, ProcessingResult

# Configuration functionality
from src.transmogrify.config import (
    settings,
    extensions,
    load_profile,
    load_config,
    configure,
)

# IO utilities - using lazy imports to avoid circular dependencies
import importlib.util
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

# Track IO module availability
_io_module_checked = False
_io_module_available = False
_writer_registry = None


def _ensure_io_module():
    """
    Lazily import the IO module to avoid circular imports.

    Returns:
        bool: Whether the IO module is available
    """
    global _io_module_checked, _io_module_available, _writer_registry

    if not _io_module_checked:
        try:
            # Use importlib for lazy loading
            io_module = importlib.import_module("src.transmogrify.io")
            _writer_registry = io_module.WriterRegistry
            _io_module_available = True
        except (ImportError, AttributeError):
            _io_module_available = False

        _io_module_checked = True

    return _io_module_available


def list_available_formats() -> List[str]:
    """
    List all available output formats.

    Returns:
        List of available format names
    """
    if _ensure_io_module() and _writer_registry is not None:
        return _writer_registry.list_available_formats()
    return []


def is_format_available(format_name: str) -> bool:
    """
    Check if a specific output format is available.

    Args:
        format_name: Name of the format to check

    Returns:
        Whether the format is available
    """
    if _ensure_io_module() and _writer_registry is not None:
        return _writer_registry.is_format_available(format_name)
    return False
