"""Base type definitions for Transmog.

This module defines common type aliases used throughout the package.
"""

from typing import Any, TypeVar

# Type aliases for common data structures
JsonDict = dict[str, Any]
FlatDict = dict[str, Any]
ArrayDict = dict[str, list[dict[str, Any]]]

# Type variable for generic writer types
T = TypeVar("T")
