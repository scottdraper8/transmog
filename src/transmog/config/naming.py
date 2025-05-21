"""Naming configuration for Transmog.

This module provides configuration options for naming conventions.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class NamingOptions:
    """Configuration options for naming conventions."""

    separator: str = "_"
    deeply_nested_threshold: int = 4

    def __post_init__(self) -> None:
        """Validate options after initialization."""
        if not self.separator:
            raise ValueError("Separator character cannot be empty")
        if self.deeply_nested_threshold < 2:
            raise ValueError("Deeply nested threshold must be at least 2")


def configure_naming(
    separator: Optional[str] = None,
    deeply_nested_threshold: Optional[int] = None,
) -> NamingOptions:
    """Configure naming options.

    Args:
        separator: Separator character for path components
        deeply_nested_threshold: Threshold for when to consider a path deeply nested

    Returns:
        NamingOptions configured with specified options
    """
    options = NamingOptions()
    if separator is not None:
        options.separator = separator
    if deeply_nested_threshold is not None:
        options.deeply_nested_threshold = deeply_nested_threshold
    # Validate after all settings are applied
    options.__post_init__()
    return options
