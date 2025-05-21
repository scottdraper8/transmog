"""Naming and path handling package for Transmog.

This package provides utilities for naming conventions and path handling
with a simplified approach that combines field names with separators.
"""

from transmog.naming.conventions import (
    get_standard_field_name,
    get_table_name,
    handle_deeply_nested_path,
    join_path,
    sanitize_name,
    split_path,
)

__all__ = [
    "get_table_name",
    "sanitize_name",
    "get_standard_field_name",
    "split_path",
    "join_path",
    "handle_deeply_nested_path",
]
