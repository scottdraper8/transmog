"""Transmog - Data flattening library.

Transmog transforms complex nested data structures into flat, tabular formats
while preserving relationships between parent and child records.

Basic Usage:
    >>> import transmog as tm
    >>> result = tm.flatten({"name": "Product", "tags": ["sale", "clearance"]})
    >>> result.main  # Main table
    >>> result.tables  # Child tables
    >>> result.save("output.json")  # Save to file

Advanced Usage:
    >>> # For very large datasets, use streaming
    >>> tm.flatten_stream(large_data, "output/", format="parquet")
"""

__version__ = "1.1.1"

# Import the simplified API
from .api import FlattenResult, flatten, flatten_file, flatten_stream
from .config import TransmogConfig
from .error import TransmogError, ValidationError
from .types import ArrayMode, NullHandling, RecoveryMode

__all__ = [
    "flatten",
    "flatten_file",
    "flatten_stream",
    "FlattenResult",
    "TransmogConfig",
    "ArrayMode",
    "NullHandling",
    "RecoveryMode",
    "TransmogError",
    "ValidationError",
    "__version__",
]
