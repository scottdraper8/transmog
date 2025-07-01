"""Transmog v1.1.0 - Simple data flattening library.

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

    >>> # For advanced features, use the Processor directly
    >>> from transmog.process import Processor
    >>> processor = Processor()
    >>> processor.stream_process(...)
"""

__version__ = "1.1.0"

# Import the simplified API
from .api import FlattenResult, flatten, flatten_file, flatten_stream

# Import the main error classes for user convenience
from .error import TransmogError, ValidationError

# Public API - only these are available to users
__all__ = [
    # Main functions
    "flatten",
    "flatten_file",
    "flatten_stream",
    # Result class
    "FlattenResult",
    # Error handling
    "TransmogError",
    "ValidationError",
    # Version
    "__version__",
]

# For users who need advanced features, they can import:
# from transmog.process import Processor
# from transmog.config import TransmogConfig
# etc.
