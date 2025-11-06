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
        SEPARATE: Extract arrays into separate child tables with parent-child
            relationships
        INLINE: Keep arrays as JSON strings within the main table
        SKIP: Ignore arrays completely during processing
    """

    SEPARATE = "separate"
    INLINE = "inline"
    SKIP = "skip"
