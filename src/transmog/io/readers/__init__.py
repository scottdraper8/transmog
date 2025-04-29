"""
Reader implementations for different file formats.

This module provides readers for various file formats.
"""

from ..formats import FormatRegistry

# Register available reader formats
FormatRegistry.register_reader_format("json")
FormatRegistry.register_reader_format("jsonl")
FormatRegistry.register_reader_format("csv")

# Import reader classes
from .json import JsonReader, JsonlReader
from .csv import CSVReader as CsvReader

__all__ = [
    "JsonReader",
    "JsonlReader",
    "CsvReader",
]
