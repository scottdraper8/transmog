"""Processing context for runtime state during data transformation.

This module defines ProcessingContext, which holds mutable runtime state
during processing, separate from immutable configuration.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class ProcessingContext:
    """Runtime state during processing, separate from configuration.

    Tracks depth, path components, and processing timestamp. The extract_time
    is set once at context creation and preserved across all nested operations
    to ensure consistent timestamping throughout a processing run.
    """

    current_depth: int = 0
    path_components: list[str] = field(default_factory=list)
    extract_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def create(cls, extract_time: Optional[datetime] = None) -> "ProcessingContext":
        """Create a new processing context with optional explicit timestamp.

        Args:
            extract_time: Optional timestamp to use. If None, current UTC time is used.

        Returns:
            New ProcessingContext instance
        """
        if extract_time is None:
            extract_time = datetime.now(timezone.utc)
        return cls(extract_time=extract_time)

    def descend(self, component: str, nested_threshold: int = 4) -> "ProcessingContext":
        """Create context for descending into nested structure.

        Args:
            component: Path component to add
            nested_threshold: Threshold for path simplification

        Returns:
            New context with incremented depth and updated path
        """
        new_depth = self.current_depth + 1

        if new_depth >= nested_threshold and len(self.path_components) >= 2:
            new_components = [self.path_components[0], "nested", component]
        elif len(self.path_components) == 0:
            new_components = [component]
        else:
            new_components = self.path_components + [component]

        return ProcessingContext(
            current_depth=new_depth,
            path_components=new_components,
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
