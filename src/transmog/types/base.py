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

    Attributes:
        SMART: Intelligently handle arrays - explode complex arrays (objects/nested)
            into child tables, preserve simple arrays (primitives) as native arrays
        SEPARATE: Extract arrays into separate child tables with parent-child
            relationships
        INLINE: Keep arrays as JSON strings within the main table
        SKIP: Ignore arrays completely during processing
    """

    SMART = "smart"
    SEPARATE = "separate"
    INLINE = "inline"
    SKIP = "skip"
