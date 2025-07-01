"""Naming configuration for Transmog.

This module provides configuration options for naming conventions.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class NamingOptions:
    """Configuration options for naming conventions."""

    separator: str = "_"
    nested_threshold: int = 4

    def __post_init__(self) -> None:
        """Validate options after initialization."""
        if not self.separator:
            raise ValueError("Separator character cannot be empty")
        if self.nested_threshold < 2:
            raise ValueError("Nested threshold must be at least 2")


def configure_naming(
    separator: Optional[str] = None,
    nested_threshold: Optional[int] = None,
) -> NamingOptions:
    """Configure naming options.

    Args:
        separator: Separator character for path components
        nested_threshold: Threshold for when to consider a path deeply nested

    Returns:
        NamingOptions configured with specified options
    """
    options = NamingOptions()
    if separator is not None:
        options.separator = separator
    if nested_threshold is not None:
        options.nested_threshold = nested_threshold
    # Validate after all settings are applied
    options.__post_init__()
    return options
