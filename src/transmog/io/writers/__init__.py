"""
Writer implementations for different file formats.

This module provides writers for various file formats.
"""

from ..formats import FormatRegistry, DependencyManager

# Register basic writer formats
FormatRegistry.register_writer_format("json")
FormatRegistry.register_writer_format("csv")

# Register optional formats if dependencies are available
if DependencyManager.has_dependency("pyarrow"):
    FormatRegistry.register_writer_format("parquet")

# Import writer classes
from .json import JsonWriter
from .csv import CsvWriter

# Import optional writers if dependencies are available
if DependencyManager.has_dependency("pyarrow"):
    from .parquet import ParquetWriter

    __all__ = ["JsonWriter", "CsvWriter", "ParquetWriter"]
else:
    __all__ = ["JsonWriter", "CsvWriter"]
