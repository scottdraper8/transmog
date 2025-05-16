"""Writer implementations for different file formats.

This module provides writers for various file formats.
"""

from transmog.dependencies import DependencyManager

from ..formats import FormatRegistry
from .csv import CsvWriter
from .json import JsonWriter

# Register basic writer formats
FormatRegistry.register_writer_format("json")
FormatRegistry.register_writer_format("csv")

# Register optional formats if dependencies are available
if DependencyManager.has_dependency("pyarrow"):
    FormatRegistry.register_writer_format("parquet")
    from .parquet import ParquetWriter

    __all__ = ["JsonWriter", "CsvWriter", "ParquetWriter"]
else:
    __all__ = ["JsonWriter", "CsvWriter"]
