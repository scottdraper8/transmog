"""Naming and path handling package for Transmog.

This package provides utilities for naming conventions and path handling
with a simplified approach that combines field names with separators.
"""

from transmog.naming.conventions import (
    get_standard_field_name,
    get_table_name,
    sanitize_name,
)

__all__ = [
    "get_table_name",
    "sanitize_name",
    "get_standard_field_name",
]
