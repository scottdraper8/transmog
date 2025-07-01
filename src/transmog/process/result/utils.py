"""Utility functions for ProcessingResult.

Contains helper functions, dependency checks, and caching utilities
used across the result processing modules.
"""

import hashlib
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .core import ProcessingResult

logger = logging.getLogger(__name__)


def _check_pyarrow_available() -> bool:
    """Check if PyArrow is available for Parquet and Arrow operations.

    Returns:
        True if PyArrow is available, False otherwise
    """
    try:
        import pyarrow  # noqa: F401

        return True
    except ImportError:
        return False


def _check_orjson_available() -> bool:
    """Check if orjson is available for optimized JSON operations.

    Returns:
        True if orjson is available, False otherwise
    """
    try:
        import orjson  # noqa: F401

        return True
    except ImportError:
        return False


def _get_cache_key(
    table_data: Any, format_type: str, **options: Any
) -> tuple[int, str, str]:
    """Generate a cache key for conversion results.

    Args:
        table_data: Data to generate key for
        format_type: Format type (e.g., 'json_bytes', 'csv_bytes')
        **options: Additional options that affect the conversion

    Returns:
        Tuple of (data_hash, format_type, options_hash)
    """
    # Create a hash of the data
    data_str = str(table_data)
    data_hash = hash(data_str)

    # Create a hash of the options
    options_str = str(sorted(options.items()))
    options_hash = hashlib.sha256(options_str.encode()).hexdigest()[:16]

    return (data_hash, format_type, options_hash)


class ResultUtils:
    """Utility methods for ProcessingResult operations."""

    def __init__(self, result: "ProcessingResult"):
        """Initialize with a ProcessingResult instance.

        Args:
            result: ProcessingResult instance
        """
        self.result = result

    def count_records(self) -> dict[str, int]:
        """Count records in all tables.

        Returns:
            Dictionary mapping table names to record counts
        """
        counts = {"main": len(self.result.main_table)}
        for table_name, table_data in self.result.child_tables.items():
            counts[table_name] = len(table_data)
        return counts

    @staticmethod
    def is_parquet_available() -> bool:
        """Check if PyArrow is available for Parquet operations.

        Returns:
            True if PyArrow is available, False otherwise
        """
        return _check_pyarrow_available()

    @staticmethod
    def is_orjson_available() -> bool:
        """Check if orjson is available for optimized JSON operations.

        Returns:
            True if orjson is available, False otherwise
        """
        return _check_orjson_available()

    def clear_intermediate_data(self) -> None:
        """Clear intermediate data representations to save memory.

        Should only be called after final output is generated in memory-efficient mode.
        """
        # Clear converted format cache if it exists
        if hasattr(self.result, "_converted_formats"):
            self.result._converted_formats.clear()

        # In extreme memory-efficient mode, we could also clear the original data
        # but this would make the result object unusable for further operations
        # Uncomment the following lines for extreme memory efficiency if needed:
        # self.result.main_table = []
        # self.result.child_tables = {}
