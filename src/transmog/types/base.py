"""Base type definitions for Transmog.

This module defines common type aliases used throughout the package.
"""

from enum import Enum
from typing import Any, TypeVar

# Type aliases for common data structures
JsonDict = dict[str, Any]
FlatDict = dict[str, Any]
ArrayDict = dict[str, list[dict[str, Any]]]

# Type variable for generic writer types
T = TypeVar("T")


class ArrayMode(Enum):
    """Defines how arrays are handled during flattening.

    SMART mode (default):
        Preserves simple arrays (strings, numbers, booleans) as native arrays
        in the output. Extracts complex arrays (containing objects or nested
        structures) into separate child tables.

    Example:
            {"tags": ["a", "b"]} → tags field contains ["a", "b"]
            {"orders": [{"id": 1}]} → separate orders table created

    SEPARATE mode:
        Extracts all arrays into separate child tables with parent-child
        relationships, regardless of content type.

    Example:
            {"tags": ["a", "b"]} → separate tags table created
            {"orders": [{"id": 1}]} → separate orders table created

    INLINE mode:
        Serializes all arrays as JSON strings within the main table.

    Example:
            {"tags": ["a", "b"]} → tags field contains '["a", "b"]'
            {"orders": [{"id": 1}]} → orders field contains '[{"id": 1}]'

    SKIP mode:
        Ignores all arrays during processing. Array fields are omitted
        from the output.

    Example:
            {"name": "x", "tags": ["a"]} → only name field appears
    """

    SMART = "smart"
    SEPARATE = "separate"
    INLINE = "inline"
    SKIP = "skip"


class RecoveryMode(Enum):
    """Defines error recovery behavior during processing.

    STRICT mode (default):
        Raises exceptions immediately when errors occur. Processing stops
        at the first error. Use when data integrity is critical.

    SKIP mode:
        Logs errors and continues processing, skipping problematic records.
        Failed records are omitted from output. Use for batch processing
        where a few failures should not stop the entire operation.

    PARTIAL mode:
        Attempts to extract usable data from partially valid records.
        Records with errors include error information in the output.
        Use when partial data is valuable.
    """

    STRICT = "strict"
    SKIP = "skip"
    PARTIAL = "partial"
