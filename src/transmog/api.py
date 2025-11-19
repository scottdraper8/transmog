"""Simplified public API for Transmog.

This module provides the primary user-facing functions for flattening
nested data structures into tabular formats.
"""

from pathlib import Path
from typing import Any

from transmog.config import TransmogConfig
from transmog.exceptions import (
    ConfigurationError,
    OutputError,
)
from transmog.flattening import get_current_timestamp, process_record_batch
from transmog.iterators import get_data_iterator
from transmog.streaming import stream_process
from transmog.types import JsonDict, ProcessingContext
from transmog.writers import create_writer
from transmog.writers.base import _sanitize_filename


class FlattenResult:
    """Container for flattened tables."""

    def __init__(
        self,
        entity_name: str,
        main_table: list[JsonDict] | None = None,
        child_tables: dict[str, list[JsonDict]] | None = None,
    ):
        """Initialize flattened data container."""
        self._entity_name = entity_name
        self._main_table = list(main_table) if main_table else []
        self._child_tables = (
            {name: list(records) for name, records in child_tables.items()}
            if child_tables
            else {}
        )

    @property
    def entity_name(self) -> str:
        """Get the entity name associated with the main table."""
        return self._entity_name

    @property
    def main(self) -> list[JsonDict]:
        """Get the main flattened table."""
        return self._main_table

    @property
    def tables(self) -> dict[str, list[JsonDict]]:
        """Get all child tables as a dictionary."""
        return self._child_tables

    @property
    def all_tables(self) -> dict[str, list[JsonDict]]:
        """Get all tables including main table."""
        tables: dict[str, list[JsonDict]] = {self._entity_name: self._main_table}
        tables.update(self._child_tables)
        return tables

    def _extend_main(self, records: list[JsonDict]) -> None:
        """Append flattened records to the main table."""
        if records:
            self._main_table.extend(records)

    def _merge_child_tables(self, tables: dict[str, list[JsonDict]]) -> None:
        """Merge child table batches into the container."""
        if not tables:
            return
        for table_name, table_records in tables.items():
            if not table_records:
                continue
            target = self._child_tables.setdefault(table_name, [])
            target.extend(table_records)

    def save(
        self,
        path: str | Path,
        output_format: str | None = None,
        **format_options: Any,
    ) -> list[str] | dict[str, str]:
        """Save all tables to files.

        Args:
            path: Output path (file or directory depending on format)
            output_format: Output format (auto-detected from extension if not specified)
                          Options: 'csv', 'parquet', 'orc'
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

        valid_formats = ["csv", "parquet", "orc"]
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

        for table_name, records in self.all_tables.items():
            if not records:
                continue

            safe_name = _sanitize_filename(table_name)
            destination = base_path / f"{safe_name or 'table'}{extension}"

            try:
                written_path = writer.write(records, str(destination))
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
        written_path = writer.write(self.main, str(file_path))
        return [str(written_path)]


def flatten(
    data: dict[str, Any] | list[dict[str, Any]] | str | Path | bytes,
    name: str = "data",
    config: TransmogConfig | None = None,
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
        >>> config = TransmogConfig(include_nulls=True, batch_size=500)
        >>> result = flatten(data, config=config)

        >>> # Save to file
        >>> result.save("output.csv")
    """
    if config is None:
        config = TransmogConfig()

    result = FlattenResult(entity_name=name)

    if isinstance(data, dict):
        iterator = iter([data])
    elif isinstance(data, list):
        iterator = iter(data)
    else:
        iterator = get_data_iterator(data)

    timestamp = get_current_timestamp()
    context = ProcessingContext(extract_time=timestamp)
    batch: list[JsonDict] = []
    batch_size = max(1, config.batch_size)

    def flush_batch() -> None:
        if not batch:
            return
        flattened_records, child_tables = process_record_batch(
            records=batch,
            entity_name=name,
            config=config,
            _context=context,
        )
        result._merge_child_tables(child_tables)
        result._extend_main(flattened_records)
        batch.clear()

    for record in iterator:
        if not isinstance(record, dict):
            raise ConfigurationError(
                f"Unsupported record type: {type(record).__name__}"
            )
        batch.append(record)
        if len(batch) >= batch_size:
            flush_batch()

    flush_batch()

    return result


def flatten_stream(
    data: dict[str, Any] | list[dict[str, Any]] | str | Path | bytes,
    output_path: str | Path,
    name: str = "data",
    output_format: str = "csv",
    config: TransmogConfig | None = None,
    **format_options: Any,
) -> None:
    r"""Stream flatten data directly to files for memory-efficient processing.

    This function processes data and writes directly to output files without
    keeping results in memory, making it ideal for very large datasets.

    Args:
        data: Input data - can be dict, list of dicts, file path, or JSON string
        output_path: Directory path where output files will be written
        name: Base name for the flattened tables
        output_format: Output format ("csv", "parquet", "orc")
        config: Optional configuration (optimized for memory if not provided)
        **format_options: Format-specific writer options:

            Parquet options:
                - compression: str - Compression codec
                  ("snappy", "gzip", "brotli", None)
                - row_group_size: int - Rows per row group (default: 10000)

            ORC options:
                - compression: str - Compression codec
                  ("zstd", "snappy", "lz4", "zlib", None)
                - batch_size: int - Rows per batch (default: 10000)

    Examples:
        >>> # Stream large dataset to CSV files
        >>> flatten_stream(large_data, "output/", output_format="csv")

        >>> # Stream with custom config
        >>> config = TransmogConfig(batch_size=100)
        >>> flatten_stream(data, "output/", config=config)

        >>> # Stream to compressed Parquet with specific row group size
        >>> flatten_stream(data, "output/", output_format="parquet",
        ...                compression="snappy", row_group_size=50000)

        >>> # Stream to compressed ORC with specific batch size
        >>> flatten_stream(data, "output/", output_format="orc",
        ...                compression="zstd", batch_size=50000)
    """
    if config is None:
        config = TransmogConfig(batch_size=100)

    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    stream_process(
        config=config,
        data=data,
        entity_name=name,
        output_format=output_format,
        output_destination=str(output_path),
        **format_options,
    )


__all__ = [
    "flatten",
    "flatten_stream",
    "FlattenResult",
    "TransmogConfig",
]
