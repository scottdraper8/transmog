"""Simplified public API for Transmog.

This module provides the primary user-facing functions for flattening
nested data structures into tabular formats.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any, Optional, Union

from transmog.config import TransmogConfig
from transmog.process import Processor
from transmog.process.result import ProcessingResult as _ProcessingResult
from transmog.process.streaming import stream_process
from transmog.types.base import JsonDict

# Type aliases
DataInput = Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes]


class FlattenResult:
    """Result of flattening nested data.

    Provides intuitive access to flattened tables with convenience methods
    for saving and converting data.
    """

    def __init__(self, processing_result: _ProcessingResult):
        """Initialize from internal processing result."""
        self._result = processing_result

    @property
    def main(self) -> list[JsonDict]:
        """Get the main flattened table."""
        return self._result.main_table

    @property
    def tables(self) -> dict[str, list[JsonDict]]:
        """Get all child tables as a dictionary."""
        return self._result.child_tables

    @property
    def all_tables(self) -> dict[str, list[JsonDict]]:
        """Get all tables including main table."""
        all_tables = {self._result.entity_name: self.main}
        all_tables.update(self.tables)
        return all_tables

    def save(
        self,
        path: Union[str, Path],
        output_format: Optional[str] = None,
    ) -> Union[list[str], dict[str, str]]:
        """Save all tables to files.

        Args:
            path: Output path (file or directory depending on format)
            output_format: Output format (auto-detected from extension if not specified)
                          Options: 'csv', 'parquet'

        Returns:
            List of created file paths
        """
        path = Path(path)

        if output_format is None:
            output_format = path.suffix.lower().lstrip(".")
            if not output_format:
                output_format = "csv"

        valid_formats = ["csv", "parquet"]
        if output_format not in valid_formats:
            raise ValueError(
                f"Unsupported format: {output_format}. Must be one of {valid_formats}"
            )

        if len(self.tables) > 0:
            if path.suffix:
                path = path.parent / path.stem
            return self._save_all_tables(path, output_format)
        else:
            if not path.suffix:
                path = path.with_suffix(f".{output_format}")
            return self._save_single_table(path, output_format)

    def __repr__(self) -> str:
        """Provide clear representation for debugging."""
        table_info = [
            f"  - {self._result.entity_name}: {len(self.main)} records (main)"
        ]
        for name, data in self.tables.items():
            table_info.append(f"  - {name}: {len(data)} records")

        return f"FlattenResult with {len(self.all_tables)} tables:\n" + "\n".join(
            table_info
        )

    def __len__(self) -> int:
        """Return number of records in main table."""
        return len(self.main)

    def __iter__(self) -> Iterator[JsonDict]:
        """Iterate over main table records."""
        return iter(self.main)

    def __getitem__(self, key: str) -> list[JsonDict]:
        """Get a specific table by name."""
        if key == self._result.entity_name or key == "main":
            return self.main
        elif key in self.tables:
            return self.tables[key]
        else:
            raise KeyError(
                f"Table '{key}' not found. Available: {list(self.all_tables.keys())}"
            )

    def __contains__(self, key: str) -> bool:
        """Check if a table exists."""
        if key == "main" or key == self._result.entity_name:
            return True
        return key in self.tables

    def keys(self) -> Any:
        """Get all table names."""
        return self.all_tables.keys()

    def values(self) -> Any:
        """Get all table data."""
        return self.all_tables.values()

    def items(self) -> Any:
        """Get all table name-data pairs."""
        return self.all_tables.items()

    def get_table(
        self, name: str, default: Optional[list[JsonDict]] = None
    ) -> Optional[list[JsonDict]]:
        """Get a table by name with optional default."""
        if name == "main":
            return self.main
        return self.all_tables.get(name, default)

    def table_info(self) -> dict[str, dict[str, Any]]:
        """Get information about all tables."""
        info = {}
        for name, data in self.all_tables.items():
            info[name] = {
                "records": len(data),
                "fields": list(data[0].keys()) if data else [],
                "is_main": name == self._result.entity_name,
            }
        return info

    def _save_all_tables(self, base_path: Path, output_format: str) -> dict[str, str]:
        """Save all tables to a directory."""
        base_path.mkdir(parents=True, exist_ok=True)

        if output_format == "csv":
            return self._result.write_all_csv(str(base_path))
        elif output_format == "parquet":
            return self._result.write_all_parquet(str(base_path))
        else:
            return {}

    def _save_single_table(self, file_path: Path, output_format: str) -> list[str]:
        """Save single table to a file."""
        import os

        file_path.parent.mkdir(parents=True, exist_ok=True)

        if output_format == "csv":
            paths = self._result.write("csv", str(file_path.parent))
            if paths:
                first_path = next(iter(paths.values()))
                os.rename(first_path, str(file_path))
                return [str(file_path)]
        elif output_format == "parquet":
            paths = self._result.write("parquet", str(file_path.parent))
            if paths:
                first_path = next(iter(paths.values()))
                os.rename(first_path, str(file_path))
                return [str(file_path)]

        return []


def flatten(
    data: DataInput,
    name: str = "data",
    config: Optional[TransmogConfig] = None,
) -> FlattenResult:
    """Flatten nested data structures into tabular format.

    This is the primary API for transforming complex nested JSON-like structures
    into flat tables with preserved parent-child relationships.

    Args:
        data: Input data - can be dict, list of dicts, file path, or JSON string
        name: Base name for the flattened tables
        config: Optional configuration (uses defaults if not provided)

    Returns:
        FlattenResult with flattened tables

    Examples:
        >>> # Basic usage with defaults
        >>> result = flatten({"name": "Product", "tags": ["sale", "clearance"]})
        >>> result.main
        [{'name': 'Product', '_id': '...'}]

        >>> # Custom configuration
        >>> config = TransmogConfig(separator=".", cast_to_string=False)
        >>> result = flatten(data, config=config)

        >>> # Use factory methods
        >>> result = flatten(data, config=TransmogConfig.for_parquet())

        >>> # Save to file
        >>> result.save("output.csv")
    """
    if config is None:
        config = TransmogConfig()

    processor = Processor(config)

    if isinstance(data, Path):
        data = str(data)

    processing_result = processor.process(data, entity_name=name)

    return FlattenResult(processing_result)


def flatten_file(
    path: Union[str, Path],
    name: Optional[str] = None,
    config: Optional[TransmogConfig] = None,
) -> FlattenResult:
    """Flatten data from a file.

    Convenience function for processing files with auto-detection of format.

    Args:
        path: Path to input file
        name: Base name for tables (defaults to filename without extension)
        config: Optional configuration

    Returns:
        FlattenResult with flattened tables

    Examples:
        >>> result = flatten_file("products.json")
        >>> result = flatten_file("data.jsonl", config=TransmogConfig(separator="."))
    """
    path = Path(path)

    if name is None:
        name = path.stem

    return flatten(str(path), name=name, config=config)


def flatten_stream(
    data: DataInput,
    output_path: Union[str, Path],
    name: str = "data",
    output_format: str = "csv",
    config: Optional[TransmogConfig] = None,
    **format_options: Any,
) -> None:
    r"""Stream flatten data directly to files for memory-efficient processing.

    This function processes data and writes directly to output files without
    keeping results in memory, making it ideal for very large datasets.

    Args:
        data: Input data - can be dict, list of dicts, file path, or JSON string
        output_path: Directory path where output files will be written
        name: Base name for the flattened tables
        output_format: Output format ("csv", "parquet")
        config: Optional configuration (optimized for memory if not provided)
        **format_options: Format-specific writer options:

            CSV options:
                - compression: str - Compression type (None, "gzip", "bz2", "xz")
                - encoding: str - Text encoding (default: "utf-8")
                - line_terminator: str - Line ending (default: "\n")

            Parquet options:
                - compression: str - Compression codec
                  ("snappy", "gzip", "brotli", "lz4", "zstd", None)
                - row_group_size: int - Rows per row group (default: 10000)
                - coerce_timestamps: str - Timestamp resolution ("ms", "us")

            Universal options:
                - buffer_size: int - Write buffer size in bytes
                - progress_callback: Callable[[int], None] - Progress function

    Examples:
        >>> # Stream large dataset to CSV files
        >>> flatten_stream(large_data, "output/", output_format="csv")

        >>> # Stream with custom config
        >>> config = TransmogConfig.for_memory()
        >>> flatten_stream(data, "output/", config=config)

        >>> # Stream to compressed Parquet with specific row group size
        >>> flatten_stream(data, "output/", output_format="parquet",
        ...                compression="snappy", row_group_size=50000)

        >>> # Stream CSV with gzip compression
        >>> flatten_stream(data, "output/", output_format="csv",
        ...                compression="gzip")
    """
    if config is None:
        config = TransmogConfig.for_memory()

    processor = Processor(config)

    if isinstance(data, Path):
        data = str(data)

    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    stream_process(
        processor=processor,
        data=data,
        entity_name=name,
        output_format=output_format,
        output_destination=str(output_path),
        **format_options,
    )


__all__ = [
    "flatten",
    "flatten_file",
    "flatten_stream",
    "FlattenResult",
    "TransmogConfig",
]
