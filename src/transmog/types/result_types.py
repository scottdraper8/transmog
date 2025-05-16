"""Result type interfaces for Transmog.

This module defines interfaces for processing results to break circular dependencies.
"""

from typing import Any, Literal, Optional, Protocol

from .base import JsonDict

# Type alias for conversion mode
ConversionModeType = Literal["eager", "lazy", "memory_efficient"]


class ResultInterface(Protocol):
    """Protocol for processing results."""

    def get_main_table(self) -> list[JsonDict]:
        """Get the main table data."""
        ...

    def get_child_table(self, table_name: str) -> list[JsonDict]:
        """Get a child table by name."""
        ...

    def get_table_names(self) -> list[str]:
        """Get list of all child table names."""
        ...

    def get_formatted_table_name(self, table_name: str) -> str:
        """Get a formatted table name suitable for file saving."""
        ...

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary representation."""
        ...

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert to a JSON string."""
        ...

    def to_json_objects(self) -> dict[str, list[JsonDict]]:
        """Convert to a dictionary of JSON-serializable objects."""
        ...

    def to_pyarrow_tables(self) -> dict[str, Any]:
        """Convert to PyArrow tables."""
        ...

    def to_parquet_bytes(self, **kwargs: Any) -> dict[str, bytes]:
        """Convert to Parquet bytes."""
        ...

    def to_csv_bytes(self, **kwargs: Any) -> dict[str, bytes]:
        """Convert to CSV bytes."""
        ...

    def to_json_bytes(self, **kwargs: Any) -> dict[str, bytes]:
        """Convert to JSON bytes."""
        ...

    def write(self, format_name: str, base_path: str, **kwargs: Any) -> dict[str, str]:
        """Write data to files in the specified format."""
        ...

    def write_all_parquet(self, base_path: str, **kwargs: Any) -> dict[str, str]:
        """Write data to Parquet files."""
        ...

    def write_all_json(self, base_path: str, **kwargs: Any) -> dict[str, str]:
        """Write data to JSON files."""
        ...

    def write_all_csv(self, base_path: str, **kwargs: Any) -> dict[str, str]:
        """Write data to CSV files."""
        ...
