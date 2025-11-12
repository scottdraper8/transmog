"""Type definitions for Transmog package."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

JsonDict = dict[str, Any]


class ArrayMode(Enum):
    """Defines how arrays are handled during flattening.

    SMART mode (default):
        Preserves simple arrays (strings, numbers, booleans) as native arrays
        in the output. Extracts complex arrays (containing objects or nested
        structures) into separate child tables.

    SEPARATE mode:
        Extracts all arrays into separate child tables with parent-child
        relationships, regardless of content type.

    INLINE mode:
        Serializes all arrays as JSON strings within the main table.

    SKIP mode:
        Ignores all arrays during processing. Array fields are omitted
        from the output.
    """

    SMART = "smart"
    SEPARATE = "separate"
    INLINE = "inline"
    SKIP = "skip"


@dataclass
class ProcessingContext:
    """Runtime state during processing, separate from configuration.

    Tracks depth, path components, and processing timestamp. The extract_time
    is set once at context creation and preserved across all nested operations
    to ensure consistent timestamping throughout a processing run.
    """

    current_depth: int = 0
    path_components: list[str] = field(default_factory=list)
    extract_time: str = ""


__all__ = [
    "JsonDict",
    "ArrayMode",
    "ProcessingContext",
]
