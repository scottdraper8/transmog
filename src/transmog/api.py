"""Simplified public API for Transmog.

This module provides the primary user-facing functions for flattening
nested data structures into tabular formats.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any, Optional, Union

from transmog.config import TransmogConfig
from transmog.core.hierarchy import process_record_batch
from transmog.core.metadata import get_current_timestamp
from transmog.error import (
    ConfigurationError,
    OutputError,
    ProcessingError,
    ValidationError,
    logger,
)
from transmog.io.writer_factory import create_writer
from transmog.io.writer_interface import sanitize_filename
from transmog.process.data_iterators import get_data_iterator
from transmog.process.result import ProcessingResult as _ProcessingResult
from transmog.process.streaming import stream_process
from transmog.types import JsonDict, ProcessingContext


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
        return self._result.all_tables()

    def save(
        self,
        path: Union[str, Path],
        output_format: Optional[str] = None,
        **format_options: Any,
    ) -> Union[list[str], dict[str, str]]:
        """Save all tables to files.

        Args:
            path: Output path (file or directory depending on format)
            output_format: Output format (auto-detected from extension if not specified)
                          Options: 'csv', 'parquet'
            **format_options: Additional writer-specific options forwarded to
                the underlying writer implementation.

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
            return self._save_all_tables(path, output_format, **format_options)
        else:
            if not path.suffix:
                path = path.with_suffix(f".{output_format}")
            return self._save_single_table(path, output_format, **format_options)

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

    def _save_all_tables(
        self,
        base_path: Path,
        output_format: str,
        **format_options: Any,
    ) -> dict[str, str]:
        """Save all tables to a directory."""
        base_path.mkdir(parents=True, exist_ok=True)

        writer = create_writer(output_format, **format_options)
        extension = ".csv" if output_format == "csv" else f".{output_format}"
        saved_paths: dict[str, str] = {}

        for table_name, records in self._result.all_tables().items():
            if not records:
                continue

            safe_name = sanitize_filename(table_name)
            destination = base_path / f"{safe_name or 'table'}{extension}"

            try:
                written_path = writer.write(records, str(destination), **format_options)
            except Exception as exc:
                raise OutputError(
                    f"Failed to write {output_format.upper()} for table '{table_name}' "
                    f"to '{destination}': {exc}"
                ) from exc

            saved_paths[table_name] = str(written_path)

        return saved_paths

    def _save_single_table(
        self,
        file_path: Path,
        output_format: str,
        **format_options: Any,
    ) -> list[str]:
        """Save single table to a file."""
        file_path.parent.mkdir(parents=True, exist_ok=True)

        writer = create_writer(output_format, **format_options)
        written_path = writer.write(self.main, str(file_path), **format_options)
        return [str(written_path)]


def _build_iterator(
    config: TransmogConfig,
    data: Union[
        dict[str, Any],
        list[dict[str, Any]],
        str,
        bytes,
        Iterator[dict[str, Any]],
    ],
) -> Iterator[JsonDict]:
    """Build data iterator from various input types."""
    if isinstance(data, dict):
        return iter([data])
    if isinstance(data, list):
        return iter(data)

    # Create a minimal processor-like object for get_data_iterator
    class ConfigWrapper:
        def __init__(self, config: TransmogConfig) -> None:
            self.config = config

    try:
        return get_data_iterator(ConfigWrapper(config), data)
    except (ValidationError, ProcessingError) as exc:
        raise ConfigurationError(str(exc)) from exc


def _consume_iterator(
    iterator: Iterator[JsonDict],
    entity_name: str,
    config: TransmogConfig,
    context: ProcessingContext,
    result: _ProcessingResult,
) -> None:
    """Consume iterator and process records in batches."""
    batch: list[JsonDict] = []
    batch_size = max(1, config.batch_size)

    try:
        for record in iterator:
            if not isinstance(record, dict):
                raise ConfigurationError(
                    f"Unsupported record type: {type(record).__name__}"
                )

            batch.append(record)
            if len(batch) >= batch_size:
                flattened_records, child_tables = process_record_batch(
                    records=batch,
                    entity_name=entity_name,
                    config=config,
                    context=context,
                )

                if child_tables:
                    result.add_child_tables(child_tables)

                for record in flattened_records:
                    result.add_main_record(record)
                batch = []

        if batch:
            flattened_records, child_tables = process_record_batch(
                records=batch,
                entity_name=entity_name,
                config=config,
                context=context,
            )

            if child_tables:
                result.add_child_tables(child_tables)

            for record in flattened_records:
                result.add_main_record(record)
    except ProcessingError as exc:
        raise ConfigurationError(str(exc)) from exc


def flatten(
    data: Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes],
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
        >>> result = flatten(data, config=TransmogConfig.for_csv())

        >>> # Save to file
        >>> result.save("output.csv")
    """
    if config is None:
        config = TransmogConfig()

    if isinstance(data, Path):
        data = str(data)

    try:
        result = _ProcessingResult(
            main_table=[],
            child_tables={},
            entity_name=name,
        )

        iterator = _build_iterator(config, data)
        timestamp = get_current_timestamp()
        context = ProcessingContext(extract_time=timestamp)

        _consume_iterator(iterator, name, config, context, result)
        return FlattenResult(result)
    except Exception as e:
        logger.error(f"Failed to process data: {e}")
        if not isinstance(e, (ConfigurationError, ProcessingError, ValidationError)):
            raise ProcessingError(f"Failed to process data: {e}") from e
        raise


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
    data: Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes],
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
                - compression: str - Compression type (None, "gzip")

            Parquet options:
                - compression: str - Compression codec
                  ("snappy", "gzip", "brotli", None)
                - row_group_size: int - Rows per row group (default: 10000)

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

    if isinstance(data, Path):
        data = str(data)

    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    # Create a minimal processor-like object for stream_process
    class ConfigWrapper:
        def __init__(self, config: TransmogConfig) -> None:
            self.config = config

    stream_process(
        processor=ConfigWrapper(config),
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
