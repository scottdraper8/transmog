"""Simplified public API for Transmog v1.1.0.

This module provides the primary user-facing functions for flattening
nested data structures into tabular formats.
"""

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal, Optional, Union

from transmog.config import MetadataConfig, ProcessingMode, TransmogConfig
from transmog.io import initialize_io_features
from transmog.process import Processor
from transmog.process.result.core import ProcessingResult as _ProcessingResult
from transmog.process.streaming import stream_process
from transmog.types.base import JsonDict
from transmog.validation import validate_api_parameters

# Initialize IO features to register writers
initialize_io_features()

# Type aliases for clarity
DataInput = Union[dict[str, Any], list[dict[str, Any]], str, Path, bytes]
ArrayHandling = Literal["separate", "inline", "skip"]
ErrorHandling = Literal["raise", "skip", "warn"]
IdSource = Union[str, dict[str, str], None]


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
                          Options: 'csv', 'json', 'parquet'

        Returns:
            List of created file paths
        """
        path = Path(path)

        # Auto-detect format from extension
        if output_format is None:
            output_format = path.suffix.lower().lstrip(".")
            if not output_format:
                output_format = "json"  # Default to JSON

        # Validate format
        valid_formats = ["csv", "json", "parquet"]
        if output_format not in valid_formats:
            raise ValueError(
                f"Unsupported format: {output_format}. Must be one of {valid_formats}"
            )

        # Handle directory vs file output
        if len(self.tables) > 0:
            # Multiple tables - use directory
            if path.suffix:
                # Remove extension if provided
                path = path.parent / path.stem
            return self._save_all_tables(path, output_format)
        else:
            # Single table - can use file
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

        if output_format == "json":
            return self._result.write_all_json(str(base_path))
        elif output_format == "csv":
            return self._result.write_all_csv(str(base_path))
        elif output_format == "parquet":
            return self._result.write_all_parquet(str(base_path))
        else:
            # Return empty dict for unknown formats
            return {}

    def _save_single_table(self, file_path: Path, output_format: str) -> list[str]:
        """Save single table to a file."""
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if output_format == "json":
            paths = self._result.write("json", str(file_path.parent))
            # Rename to match requested filename
            if paths:
                # paths is a dict, get the first file path
                first_path = next(iter(paths.values()))
                os.rename(first_path, str(file_path))
                return [str(file_path)]
        elif output_format == "csv":
            paths = self._result.write("csv", str(file_path.parent))
            if paths:
                # paths is a dict, get the first file path
                first_path = next(iter(paths.values()))
                os.rename(first_path, str(file_path))
                return [str(file_path)]
        elif output_format == "parquet":
            paths = self._result.write("parquet", str(file_path.parent))
            if paths:
                # paths is a dict, get the first file path
                first_path = next(iter(paths.values()))
                os.rename(first_path, str(file_path))
                return [str(file_path)]

        return []


def flatten(
    data: DataInput,
    *,
    name: str = "data",
    # Naming options
    separator: str = "_",
    nested_threshold: int = 4,
    # ID options
    id_field: IdSource = None,
    parent_id_field: str = "_parent_id",
    add_timestamp: bool = False,
    # Array handling
    arrays: ArrayHandling = "separate",
    # Data options
    preserve_types: bool = False,
    skip_null: bool = True,
    skip_empty: bool = True,
    # Error handling
    errors: ErrorHandling = "raise",
    # Performance
    batch_size: int = 1000,
    low_memory: bool = False,
) -> FlattenResult:
    """Flatten nested data structures into tabular format.

    This is the primary API for transforming complex nested JSON-like structures
    into flat tables with preserved parent-child relationships.

    Args:
        data: Input data - can be dict, list of dicts, file path, or JSON string
        name: Base name for the flattened tables

        Naming Options:
        separator: Character to separate nested field names (default: "_")
        nested_threshold: Depth at which to simplify deeply nested names (default: 4)

        ID Options:
        id_field: Field to use as ID. Can be:
                 - None: Generate unique IDs
                 - String: Field name to use as ID
                 - Dict: Mapping of table names to ID fields
        parent_id_field: Name for parent reference field (default: "_parent_id")
        add_timestamp: Add timestamp field to records (default: False)

        Array Handling:
        arrays: How to handle arrays:
               - "separate": Extract to child tables (default)
               - "inline": Keep in main record as JSON
               - "skip": Ignore arrays

        Data Options:
        preserve_types: Keep original types instead of converting to strings
                       (default: False)
        skip_null: Skip null values in output (default: True)
        skip_empty: Skip empty strings/lists/dicts (default: True)

        Error Handling:
        errors: How to handle errors:
               - "raise": Raise exception on error (default)
               - "skip": Skip problematic records
               - "warn": Log warnings and continue

        Performance:
        batch_size: Number of records to process at once (default: 1000)
        low_memory: Optimize for low memory usage (default: False)

    Returns:
        FlattenResult with flattened tables

    Examples:
        >>> # Basic usage
        >>> result = flatten({"name": "Product", "tags": ["sale", "clearance"]})
        >>> result.main
        [{'name': 'Product', '_id': '...'}]
        >>> result.tables['data_tags']
        [{'value': 'sale', '_parent_id': '...'},
         {'value': 'clearance', '_parent_id': '...'}]

        >>> # Save to file
        >>> result.save("output.json")

        >>> # Use existing ID field
        >>> result = flatten(data, id_field="product_id")

        >>> # Custom separator for nested fields
        >>> result = flatten(nested_data, separator=".")
    """
    # Validate API parameters
    validate_api_parameters(
        data=data,
        name=name,
        separator=separator,
        nested_threshold=nested_threshold,
        id_field=id_field,
        parent_id_field=parent_id_field,
        arrays=arrays,
        errors=errors,
        batch_size=batch_size,
    )

    # Build configuration from simplified options
    config = _build_config(
        separator=separator,
        nested_threshold=nested_threshold,
        id_field=id_field,
        parent_id_field=parent_id_field,
        add_timestamp=add_timestamp,
        arrays=arrays,
        preserve_types=preserve_types,
        skip_null=skip_null,
        skip_empty=skip_empty,
        errors=errors,
        batch_size=batch_size,
        low_memory=low_memory,
    )

    # Create processor and process data
    processor = Processor(config)

    # Handle file paths
    if isinstance(data, Path):
        data = str(data)

    # Process the data
    processing_result = processor.process(data, entity_name=name)

    # Return wrapped result
    return FlattenResult(processing_result)


def flatten_file(
    path: Union[str, Path],
    *,
    name: Optional[str] = None,
    file_format: Optional[str] = None,
    **options: Any,
) -> FlattenResult:
    """Flatten data from a file.

    Convenience function for processing files with auto-detection of format.

    Args:
        path: Path to input file
        name: Base name for tables (defaults to filename without extension)
        file_format: File format (auto-detected if not specified)
        **options: Same options as flatten()

    Returns:
        FlattenResult with flattened tables

    Examples:
        >>> result = flatten_file("products.json")
        >>> result = flatten_file("data.csv", separator=".")
    """
    # Validate API parameters
    validate_api_parameters(format=file_format)

    path = Path(path)

    # Auto-detect name from filename
    if name is None:
        name = path.stem

    # Pass to main flatten function
    return flatten(str(path), name=name, **options)


def flatten_stream(
    data: DataInput,
    output_path: Union[str, Path],
    *,
    name: str = "data",
    output_format: str = "json",
    # All the same options as flatten()
    separator: str = "_",
    nested_threshold: int = 4,
    id_field: IdSource = None,
    parent_id_field: str = "_parent_id",
    add_timestamp: bool = False,
    arrays: ArrayHandling = "separate",
    preserve_types: bool = False,
    skip_null: bool = True,
    skip_empty: bool = True,
    errors: ErrorHandling = "raise",
    batch_size: int = 1000,
    # Streaming-specific options
    **format_options: Any,
) -> None:
    """Stream flatten data directly to files for memory-efficient processing.

    This function processes data and writes directly to output files without
    keeping results in memory, making it ideal for very large datasets.

    Args:
        data: Input data - can be dict, list of dicts, file path, or JSON string
        output_path: Directory path where output files will be written
        name: Base name for the flattened tables
        output_format: Output format ("json", "csv", "parquet")

        # Same options as flatten() function
        separator: Character to separate nested field names
        nested_threshold: Depth at which to simplify deeply nested names
        id_field: Field to use as ID
        parent_id_field: Name for parent reference field
        add_timestamp: Add timestamp field to records
        arrays: How to handle arrays ("separate", "inline", "skip")
        preserve_types: Keep original types instead of converting to strings
        skip_null: Skip null values in output
        skip_empty: Skip empty strings/lists/dicts
        errors: How to handle errors ("raise", "skip", "warn")
        batch_size: Number of records to process at once

        **format_options: Format-specific options (compression, etc.)

    Returns:
        None - data is written directly to files

    Examples:
        >>> # Stream large dataset to JSON files
        >>> flatten_stream(large_data, "output/", output_format="json")

        >>> # Stream to compressed Parquet
        >>> flatten_stream(data, "output/", output_format="parquet",
        ...                compression="snappy")

        >>> # Stream CSV file processing
        >>> flatten_stream("large_file.csv", "output/", output_format="csv")
    """
    # Validate API parameters
    validate_api_parameters(
        data=data,
        name=name,
        format=output_format,
        separator=separator,
        nested_threshold=nested_threshold,
        id_field=id_field,
        parent_id_field=parent_id_field,
        arrays=arrays,
        errors=errors,
        batch_size=batch_size,
    )

    # Build configuration from options
    config = _build_config(
        separator=separator,
        nested_threshold=nested_threshold,
        id_field=id_field,
        parent_id_field=parent_id_field,
        add_timestamp=add_timestamp,
        arrays=arrays,
        preserve_types=preserve_types,
        skip_null=skip_null,
        skip_empty=skip_empty,
        errors=errors,
        batch_size=batch_size,
        low_memory=True,  # Always use low memory for streaming
    )

    # Use the advanced streaming API
    processor = Processor(config)

    # Handle file paths
    if isinstance(data, Path):
        data = str(data)

    # Create output directory
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    # Import and use streaming function

    # Stream process the data
    stream_process(
        processor=processor,
        data=data,
        entity_name=name,
        output_format=output_format,
        output_destination=str(output_path),
        batch_size=batch_size,
        **format_options,
    )


def _build_config(
    separator: str,
    nested_threshold: int,
    id_field: IdSource,
    parent_id_field: str,
    add_timestamp: bool,
    arrays: ArrayHandling,
    preserve_types: bool,
    skip_null: bool,
    skip_empty: bool,
    errors: ErrorHandling,
    batch_size: int,
    low_memory: bool,
) -> TransmogConfig:
    """Build internal configuration from simplified options."""
    # Apply array handling
    visit_arrays = arrays != "skip"
    keep_arrays = arrays == "inline"

    # Apply error handling
    if errors == "raise":
        strategy = "strict"
        allow_malformed = False
    elif errors == "skip":
        strategy = "skip"
        allow_malformed = True
    elif errors == "warn":
        strategy = "partial"
        allow_malformed = True
    else:
        raise ValueError(f"Invalid error handling option: {errors}")

        # Build metadata configuration with proper timestamp handling
    metadata_config = MetadataConfig(
        id_field="_id",
        parent_field=parent_id_field,
        time_field="_timestamp" if add_timestamp else None,
    )

    # Handle ID field configuration by creating a new metadata config
    if id_field is not None:
        if isinstance(id_field, str):
            # Single field name - use for natural ID discovery
            metadata_config = MetadataConfig(
                id_field=metadata_config.id_field,
                parent_field=metadata_config.parent_field,
                time_field=metadata_config.time_field,
                id_field_patterns=[id_field],
            )
        elif isinstance(id_field, dict):
            # Mapping of table to field names
            metadata_config = MetadataConfig(
                id_field=metadata_config.id_field,
                parent_field=metadata_config.parent_field,
                time_field=metadata_config.time_field,
                id_field_mapping=id_field,
            )

    # Create configuration with explicit metadata
    config = TransmogConfig(
        # Component configs
        metadata=metadata_config,
        # Convenience parameters
        separator=separator,
        nested_threshold=nested_threshold,
        cast_to_string=not preserve_types,
        include_empty=not skip_empty,
        skip_null=skip_null,
        visit_arrays=visit_arrays,
        batch_size=batch_size,
        recovery_strategy=strategy,
        allow_malformed_data=allow_malformed,
    )

    # Set processing mode and keep_arrays which don't have convenience parameters
    config.processing.processing_mode = (
        ProcessingMode.LOW_MEMORY if low_memory else ProcessingMode.STANDARD
    )
    config.processing.keep_arrays = keep_arrays

    return config
