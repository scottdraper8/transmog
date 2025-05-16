"""Naming and path handling package for Transmog.

This package provides utilities for naming conventions, path handling,
and abbreviation of field and table names.
"""

from transmog.naming.abbreviator import (
    abbreviate_component,
    abbreviate_field_name,
    abbreviate_table_name,
    merge_abbreviation_dicts,
)
from transmog.naming.conventions import (
    get_standard_field_name,
    get_table_name,
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
    "abbreviate_component",
    "abbreviate_table_name",
    "abbreviate_field_name",
    "merge_abbreviation_dicts",
]
