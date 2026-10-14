"""Type definitions for Transmog package."""

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

JsonDict = dict[str, Any]
ProgressCallback = Callable[[int, int | None], None]


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

    Tracks depth, path components, and processing timestamp. The processing
    root supplies extract_time and nested operations preserve it. The empty
    default does not generate a timestamp independently.
    """

    current_depth: int = 0
    path_components: list[str] = field(default_factory=list)
    extract_time: str = ""


__all__ = [
    "JsonDict",
    "ProgressCallback",
    "ArrayMode",
    "ProcessingContext",
]
