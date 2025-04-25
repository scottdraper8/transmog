"""
Naming and path handling package for Transmog.

This package provides utilities for naming conventions, path handling,
and abbreviation of field and table names.
"""

from transmog.naming.conventions import (
    get_table_name,
    sanitize_name,
    get_standard_field_name,
    split_path,
    join_path,
)

from transmog.naming.abbreviator import (
    abbreviate_component,
    abbreviate_table_name,
    abbreviate_field_name,
    get_common_abbreviations,
    merge_abbreviation_dicts,
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
    "get_common_abbreviations",
    "merge_abbreviation_dicts",
]
