"""
Base type definitions for Transmog.

This module defines common type aliases used throughout the package.
"""

from typing import Any, Dict, List, TypeVar

# Type aliases for common data structures
JsonDict = Dict[str, Any]
FlatDict = Dict[str, Any]
ArrayDict = Dict[str, List[Dict[str, Any]]]

# Type variable for generic writer types
T = TypeVar("T")
