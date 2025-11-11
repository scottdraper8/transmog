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


class RecoveryMode(Enum):
    """Defines error recovery behavior during processing.

    STRICT mode (default):
        Raises exceptions immediately when errors occur. Processing stops
        at the first error. Use when data integrity is critical.

    SKIP mode:
        Logs errors and continues processing, skipping problematic records.
        Failed records are omitted from output. Use for batch processing
        where a few failures should not stop the entire operation.
    """

    STRICT = "strict"
    SKIP = "skip"


class NullHandling(Enum):
    """Defines how null and empty values are handled during processing.

    SKIP mode (default):
        Skips null values and empty strings. These fields are omitted from output.
        Use when you want clean output without null/empty fields.

    INCLUDE mode:
        Includes null values as empty strings and includes empty string values.
        Use for CSV output where consistent column structure is required.
    """

    SKIP = "skip"
    INCLUDE = "include"


@dataclass
class ProcessingContext:
    """Runtime state during processing, separate from configuration.

    Tracks depth, path components, and processing timestamp. The extract_time
    is set once at context creation and preserved across all nested operations
    to ensure consistent timestamping throughout a processing run.
    """

    current_depth: int = 0
    path_components: list[str] = field(default_factory=list)
    extract_time: str = field(default_factory=lambda: "")

    def descend(self, component: str) -> "ProcessingContext":
        """Create context for descending into nested structure.

        Args:
            component: Path component to add

        Returns:
            New context with incremented depth and updated path
        """
        return ProcessingContext(
            current_depth=self.current_depth + 1,
            path_components=self.path_components + [component],
            extract_time=self.extract_time,
        )

    def build_path(self, separator: str = "_") -> str:
        """Build complete path string from components.

        Args:
            separator: Separator character for joining components

        Returns:
            Joined path string
        """
        return separator.join(self.path_components) if self.path_components else ""


__all__ = [
    "JsonDict",
    "ArrayMode",
    "RecoveryMode",
    "NullHandling",
    "ProcessingContext",
]
