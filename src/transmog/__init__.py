"""Transmog - Data flattening library.

Transmog transforms complex nested data structures into flat, tabular formats
while preserving relationships between parent and child records.

Basic Usage:
    >>> import transmog as tm
    >>> result = tm.flatten({"name": "Product", "tags": ["sale", "clearance"]})
    >>> result.main  # Main table
    >>> result.tables  # Child tables
    >>> result.save("output.csv")  # Save to file

Advanced Usage:
    >>> # For very large datasets, use streaming
    >>> tm.flatten_stream(large_data, "output/", output_format="parquet")
"""

__version__ = "2.0.1"

from transmog.api import FlattenResult, flatten, flatten_stream
from transmog.config import TransmogConfig
from transmog.exceptions import MissingDependencyError, TransmogError, ValidationError
from transmog.types import ArrayMode

__all__ = [
    "flatten",
    "flatten_stream",
    "FlattenResult",
    "TransmogConfig",
    "ArrayMode",
    "TransmogError",
    "ValidationError",
    "MissingDependencyError",
    "__version__",
]
