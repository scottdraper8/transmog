"""ProcessingResult module for managing processing outputs.

This module contains the ProcessingResult class for managing and
writing the results of processing nested JSON structures.
"""

import io
import json
import logging
import os
from enum import Enum
from typing import (
    Any,
    BinaryIO,
    Callable,
    Optional,
    Union,
    cast,
)

from transmog.error import MissingDependencyError, OutputError
from transmog.io.writer_factory import (
    create_streaming_writer,
    create_writer,
    is_format_available,
)
from transmog.types.base import JsonDict
from transmog.types.result_types import ConversionModeType, ResultInterface

logger = logging.getLogger(__name__)

# Cache for conversions to avoid redundant work
_conversion_cache: dict[tuple[int, str, str], Any] = {}


def _check_pyarrow_available() -> bool:
    """Check if PyArrow is available for use.

    Returns:
        bool: Whether PyArrow is available
    """
    try:
        # Use find_spec to check if module exists without importing it
        from importlib.util import find_spec

        return find_spec("pyarrow") is not None
    except ImportError:
        return False


def _check_orjson_available() -> bool:
    """Check if orjson is available for use.

    Returns:
        bool: Whether orjson is available
    """
    try:
        # Use find_spec to check if module exists without importing it
        from importlib.util import find_spec

        return find_spec("orjson") is not None
    except ImportError:
        return False


def _get_cache_key(
    table_data: Any, format_type: str, **options: Any
) -> tuple[int, str, str]:
    """Generate a cache key for table data conversions.

    Args:
        table_data: The data to be converted
        format_type: The target format type
        **options: Format-specific options

    Returns:
        A hashable cache key
    """
    # For mutable collections use id() as part of the key
    data_id = id(table_data)
    options_str = str(sorted(options.items()))
    return (data_id, format_type, options_str)


class ConversionMode(Enum):
    """Conversion mode for ProcessingResult."""

    EAGER = "eager"  # Convert immediately, keep all data in memory
    LAZY = "lazy"  # Convert only when needed
    MEMORY_EFFICIENT = "memory_efficient"  # Discard intermediate data after conversion


class ProcessingResult(ResultInterface):
    """Container for processing results including main and child tables.

    The ProcessingResult manages the outputs of processing, providing
    access to the main table and child tables, as well as methods to
    convert the data to different formats or save to files.
    """

    def __init__(
        self,
        main_table: list[JsonDict],
        child_tables: dict[str, list[JsonDict]],
        entity_name: str,
        source_info: Optional[dict[str, Any]] = None,
        conversion_mode: Union[
            ConversionMode, ConversionModeType
        ] = ConversionMode.EAGER,
    ):
        """Initialize with main and child tables.

        Args:
            main_table: List of records for the main table
            child_tables: Dictionary of child tables keyed by name
            entity_name: Name of the entity
            source_info: Information about the source data
            conversion_mode: How to handle data conversion
        """
        self.main_table = main_table
        self.child_tables = child_tables
        self.entity_name = entity_name
        self.source_info = source_info or {}

        # Convert string conversion mode to enum if needed
        if isinstance(conversion_mode, str):
            if conversion_mode == "eager":
                self.conversion_mode = ConversionMode.EAGER
            elif conversion_mode == "lazy":
                self.conversion_mode = ConversionMode.LAZY
            elif conversion_mode == "memory_efficient":
                self.conversion_mode = ConversionMode.MEMORY_EFFICIENT
            else:
                self.conversion_mode = ConversionMode.EAGER
        else:
            self.conversion_mode = conversion_mode

        self._converted_formats: dict[str, Any] = {}
        self._conversion_functions: dict[
            str, Callable[[ProcessingResult, Any], Any]
        ] = {}

        # Initialize cache
        global _conversion_cache
        _conversion_cache = {}

    def get_main_table(self) -> list[JsonDict]:
        """Get the main table data."""
        return self.main_table

    def get_child_table(self, table_name: str) -> list[JsonDict]:
        """Get a child table by name."""
        return self.child_tables.get(table_name, [])

    def get_table_names(self) -> list[str]:
        """Get list of all child table names."""
        return list(self.child_tables.keys())

    def get_formatted_table_name(self, table_name: str) -> str:
        """Get a formatted table name suitable for file saving.

        Args:
            table_name: The table name to format

        Returns:
            Formatted table name
        """
        # Simplified formatting for table names - just replace problematic characters
        return table_name.replace(".", "_").replace("/", "_")

    def add_main_record(self, record: JsonDict) -> None:
        """Add a record to the main table.

        Args:
            record: Record to add to the main table
        """
        self.main_table.append(record)

    def add_child_tables(self, tables: dict[str, list[JsonDict]]) -> None:
        """Add child tables to the result.

        Args:
            tables: Dictionary of child tables to add
        """
        for table_name, records in tables.items():
            if table_name in self.child_tables:
                self.child_tables[table_name].extend(records)
            else:
                self.child_tables[table_name] = records

    def add_child_record(self, table_name: str, record: JsonDict) -> None:
        """Add a single record to a child table.

        Args:
            table_name: Name of the child table
            record: Record to add to the child table
        """
        if table_name in self.child_tables:
            self.child_tables[table_name].append(record)
        else:
            self.child_tables[table_name] = [record]

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary representation.

        Returns:
            Dict with main and child tables
        """
        if "dict" in self._converted_formats:
            return cast(dict[str, Any], self._converted_formats["dict"])

        result: dict[str, Any] = {
            "main_table": self.main_table,
            "child_tables": self.child_tables,
            "entity_name": self.entity_name,
            "source_info": self.source_info,
        }

        if self.conversion_mode == ConversionMode.EAGER:
            self._converted_formats["dict"] = result

        return result

    def to_json(self, indent: Optional[int] = 2) -> str:
        """Convert to JSON string.

        Args:
            indent: Indentation level for JSON formatting

        Returns:
            JSON string representation
        """
        key = f"json_{indent}"
        if key in self._converted_formats:
            return cast(str, self._converted_formats[key])

        try:
            json_string = json.dumps(self.to_dict(), indent=indent)

            if self.conversion_mode == ConversionMode.EAGER:
                self._converted_formats[key] = json_string

            return json_string
        except Exception as e:
            logger.error(f"Error converting to JSON: {e}")
            raise OutputError(f"Failed to convert result to JSON: {e}") from e

    def to_json_objects(self) -> dict[str, list[dict[str, Any]]]:
        """Convert all tables to JSON-serializable Python objects.

        This ensures all values in the dictionaries are serializable by JSON encoders.

        Returns:
            Dict with 'main' and child table names as keys, and lists of JSON-
                serializable records as values
        """
        if "json_objects" in self._converted_formats:
            return cast(
                dict[str, list[dict[str, Any]]], self._converted_formats["json_objects"]
            )

        result: dict[str, list[dict[str, Any]]] = {}

        # Process main table
        result["main"] = self._ensure_json_serializable(self.main_table)

        # Process child tables
        for table_name, table_data in self.child_tables.items():
            result[table_name] = self._ensure_json_serializable(table_data)

        if self.conversion_mode == ConversionMode.EAGER:
            self._converted_formats["json_objects"] = result

        return result

    def _ensure_json_serializable(
        self, data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Ensure all values in list of dictionaries are JSON serializable.

        Handles cases like dates, custom objects, etc. by converting to strings.

        Args:
            data: List of dictionaries to process

        Returns:
            List of dictionaries with all values JSON serializable
        """
        result = []
        for record in data:
            json_record: dict[str, Union[str, int, float, bool, None, list, dict]] = {}
            for k, v in record.items():
                if v is None:
                    json_record[k] = None  # None is valid in JSON
                elif isinstance(v, (str, int, float, bool)):
                    json_record[k] = v
                elif isinstance(v, (list, dict)):
                    # For nested structures, convert to JSON string
                    try:
                        json_record[k] = json.dumps(v)
                    except TypeError:
                        # If not serializable, use string representation
                        json_record[k] = str(v)
                else:
                    # Default handling - convert anything else to string
                    json_record[k] = str(v)
            result.append(json_record)
        return result

    def to_pyarrow_tables(self) -> dict[str, Any]:
        """Convert all tables to PyArrow tables.

        Returns:
            Dictionary of table names to PyArrow tables

        Raises:
            MissingDependencyError: If PyArrow is not available
            OutputError: If conversion fails
        """
        # Check for cached result first
        cache_key = _get_cache_key(self.to_dict(), "pyarrow_tables")
        if cache_key in _conversion_cache:
            return cast(dict[str, Any], _conversion_cache[cache_key])

        if not _check_pyarrow_available():
            raise MissingDependencyError(
                "PyArrow is required for PyArrow table conversion. "
                "Install with 'pip install pyarrow'.",
                package="pyarrow",
                feature="arrow",
            )

        try:
            # Convert tables to JSON objects first
            tables = self.to_json_objects()
            result: dict[str, Any] = {}

            # Convert each table to PyArrow
            for table_name, records in tables.items():
                # Skip empty tables
                if not records:
                    continue

                # Convert to PyArrow table using configuration-driven type handling
                result[table_name] = self._dict_list_to_pyarrow(records)

            # Cache the result if using eager mode
            if self.conversion_mode == ConversionMode.EAGER:
                _conversion_cache[cache_key] = result

            return result
        except Exception as e:
            logger.error(f"Error converting to PyArrow tables: {e}")
            raise OutputError(f"Failed to convert to PyArrow tables: {e}") from e

    def _dict_list_to_pyarrow(
        self, data: list[dict[str, Any]], force_string_types: Optional[bool] = None
    ) -> Any:
        """Convert a list of dictionaries to a PyArrow Table.

        Args:
            data: List of records to convert
            force_string_types: If True, all values will be converted to strings
                               If None, will use the global cast_to_string configuration

        Returns:
            PyArrow Table

        Raises:
            ImportError: If PyArrow is not available
        """
        if not data:
            import pyarrow as pa

            return pa.table({})

        import pyarrow as pa

        # Get the cast_to_string value from configuration if not explicitly provided
        from transmog.config import settings

        use_string_types = (
            force_string_types
            if force_string_types is not None
            else settings.get_option("cast_to_string", False)
        )

        # Extract columns from dictionaries
        columns = {}
        for key in data[0].keys():
            values = [record.get(key) for record in data]

            # Convert all values to strings if configured to do so
            if use_string_types:
                # Convert non-None values to strings
                values = [str(val) if val is not None else None for val in values]
                columns[key] = pa.array(values, type=pa.string())
            else:
                columns[key] = values

        # If using explicit string types, create the table with the prepared arrays
        if use_string_types:
            return pa.table(columns)
        else:
            # Otherwise use the default PyArrow table creation which infers types
            return pa.table(columns)

    def to_parquet_bytes(
        self, compression: str = "snappy", **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert the results to Parquet bytes.

        Args:
            compression: Compression format for Parquet
            **kwargs: Additional Parquet writer options

        Returns:
            Dictionary of table names to Parquet bytes

        Raises:
            MissingDependencyError: If PyArrow is not available
            OutputError: If conversion fails
        """
        # Check for cached result first
        cache_key = _get_cache_key(
            self.to_dict(), "parquet_bytes", compression=compression, **kwargs
        )
        if cache_key in _conversion_cache:
            return cast(dict[str, bytes], _conversion_cache[cache_key])

        if not _check_pyarrow_available():
            raise MissingDependencyError(
                "PyArrow is required for Parquet conversion. "
                "Install with 'pip install pyarrow'.",
                package="pyarrow",
                feature="parquet",
            )

        try:
            import pyarrow.parquet as pq
            from pyarrow.lib import ArrowInvalid

            # Convert tables to PyArrow format first with config-driven type handling
            arrow_tables = {}
            for table_name, records in self.to_json_objects().items():
                arrow_tables[table_name] = self._dict_list_to_pyarrow(records)

            result: dict[str, bytes] = {}

            # Convert each table to Parquet bytes
            for table_name, arrow_table in arrow_tables.items():
                buffer = io.BytesIO()
                try:
                    pq.write_table(
                        arrow_table, buffer, compression=compression, **kwargs
                    )
                    # Get the bytes from the buffer
                    buffer.seek(0)
                    result[table_name] = buffer.getvalue()
                except ArrowInvalid as e:
                    logger.error(f"Error converting {table_name} to Parquet: {e}")
                    # Skip this table if it can't be converted
                    continue
                finally:
                    buffer.close()

            # Cache the result if using eager mode
            if self.conversion_mode == ConversionMode.EAGER:
                _conversion_cache[cache_key] = result

            return result
        except Exception as e:
            logger.error(f"Error converting to Parquet: {e}")
            raise OutputError(f"Failed to convert result to Parquet: {e}") from e

    def stream_to_parquet(
        self,
        base_path: str,
        compression: str = "snappy",
        row_group_size: int = 10000,
        **kwargs: Any,
    ) -> dict[str, str]:
        """Stream the results to Parquet files.

        This method is memory-efficient for large datasets as it writes
        data in chunks rather than loading everything in memory.

        Args:
            base_path: Base path for output files
            compression: Compression format for Parquet
            row_group_size: Number of rows per row group
            **kwargs: Additional Parquet writer options

        Returns:
            Dictionary of table names to file paths

        Raises:
            MissingDependencyError: If PyArrow is not available
            OutputError: If streaming fails
        """
        if not _check_pyarrow_available():
            raise MissingDependencyError(
                "PyArrow is required for Parquet conversion. "
                "Install with 'pip install pyarrow'.",
                package="pyarrow",
                feature="parquet",
            )

        try:
            import pyarrow.parquet as pq
            from pyarrow.lib import ArrowInvalid

            # Create the base directory if it doesn't exist
            os.makedirs(base_path, exist_ok=True)

            # Convert to dictionary structure with tables
            tables_dict = self.to_json_objects()

            # Map of table names to file paths
            file_paths: dict[str, str] = {}

            # Process each table
            for table_name, records in tables_dict.items():
                # Skip empty tables
                if not records:
                    continue

                # Create formatted output path
                formatted_name = self.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.parquet")
                file_paths[table_name] = file_path

                # For schema evolution support, collect all fields first
                all_fields: set[str] = set()
                for record in records:
                    all_fields.update(record.keys())

                # Ensure all records have all fields (with nulls for missing ones)
                complete_records = []
                for record in records:
                    complete_record = dict.fromkeys(all_fields)
                    complete_record.update(record)
                    complete_records.append(complete_record)

                # Convert to PyArrow table with configuration-driven type handling
                arrow_table = self._dict_list_to_pyarrow(complete_records)

                # Write directly to file
                try:
                    pq.write_table(
                        arrow_table,
                        file_path,
                        compression=compression,
                        row_group_size=row_group_size,
                        **kwargs,
                    )
                except ArrowInvalid as e:
                    logger.error(f"Error writing {table_name} to Parquet: {e}")
                    # Skip this table if it can't be converted
                    continue

            return file_paths
        except Exception as e:
            logger.error(f"Error streaming to Parquet: {e}")
            raise OutputError(f"Failed to stream results to Parquet: {e}") from e

    def write_all_parquet(
        self, base_path: str, compression: str = "snappy", **kwargs: Any
    ) -> dict[str, str]:
        """Write all tables to Parquet files.

        Args:
            base_path: Base path for output files
            compression: Compression format for Parquet
            **kwargs: Additional Parquet writer options

        Returns:
            Dictionary of table names to file paths

        Raises:
            MissingDependencyError: If PyArrow is not available
            OutputError: If writing fails
        """
        if not _check_pyarrow_available():
            raise MissingDependencyError(
                "PyArrow is required for Parquet support. "
                "Install with 'pip install pyarrow'.",
                package="pyarrow",
                feature="parquet",
            )

        # Create the base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Convert to dictionary structure with tables
        tables = self.to_json_objects()

        # Keep track of the paths
        file_paths: dict[str, str] = {}

        try:
            import pyarrow.parquet as pq

            # Process each table
            for table_name, records in tables.items():
                # Skip empty tables
                if not records:
                    continue

                # Create the formatted table name
                formatted_name = self.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.parquet")

                # Convert records to PyArrow table with config-driven type handling
                arrow_table = self._dict_list_to_pyarrow(records)

                # Write to Parquet file
                pq.write_table(
                    arrow_table, file_path, compression=compression, **kwargs
                )

                # Record the file path
                file_paths[table_name] = file_path

            return file_paths
        except Exception as e:
            raise OutputError(
                f"Failed to write Parquet files: {e}",
                output_format="parquet",
                path=base_path,
            ) from e

    def to_csv_bytes(
        self, include_header: bool = True, **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert all tables to CSV bytes.

        Args:
            include_header: Whether to include header row
            **kwargs: Additional CSV formatting options

        Returns:
            Dictionary of table names to CSV bytes

        Raises:
            OutputError: If conversion fails
        """
        # Check cache first
        cache_key = _get_cache_key(
            self.to_dict(), "csv_bytes", include_header=include_header, **kwargs
        )
        if cache_key in _conversion_cache:
            return cast(dict[str, bytes], _conversion_cache[cache_key])

        # Try PyArrow first if available
        if _check_pyarrow_available():
            try:
                result = self._to_csv_bytes_pyarrow(include_header, **kwargs)

                # Cache the result if using eager mode
                if self.conversion_mode == ConversionMode.EAGER:
                    _conversion_cache[cache_key] = result

                return result
            except Exception as e:
                logger.debug(
                    f"PyArrow CSV conversion failed, falling back to stdlib: {e}"
                )
                # Fall back to stdlib if PyArrow fails

        # Use Python's standard library CSV module
        result = self._to_csv_bytes_stdlib(include_header, **kwargs)

        # Cache the result if using eager mode
        if self.conversion_mode == ConversionMode.EAGER:
            _conversion_cache[cache_key] = result

        return result

    def _to_csv_bytes_pyarrow(
        self, include_header: bool = True, **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert tables to CSV bytes using PyArrow.

        Args:
            include_header: Whether to include header
            **kwargs: Additional CSV options

        Returns:
            Dictionary of table names to CSV bytes

        Raises:
            MissingDependencyError: If PyArrow is not available
        """
        if not _check_pyarrow_available():
            raise MissingDependencyError(
                "PyArrow is required for optimized CSV conversion. "
                "Falling back to standard library.",
                package="pyarrow",
                feature="csv",
            )

        try:
            from pyarrow import csv as pa_csv

            # Convert tables to PyArrow format first
            arrow_tables = self.to_pyarrow_tables()
            result: dict[str, bytes] = {}

            # Process each table
            for table_name, table in arrow_tables.items():
                buffer = io.BytesIO()

                # Configure write options
                write_options = pa_csv.WriteOptions(
                    include_header=include_header,
                )

                # Write the table to CSV
                pa_csv.write_csv(table, buffer, write_options=write_options)

                # Get the bytes from the buffer
                buffer.seek(0)
                result[table_name] = buffer.getvalue()

            return result
        except Exception as e:
            logger.error(f"Error converting to CSV with PyArrow: {e}")
            raise OutputError(
                f"Failed to convert to CSV: {e}", output_format="csv"
            ) from e

    def _to_csv_bytes_stdlib(
        self, include_header: bool = True, **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert tables to CSV bytes using standard library.

        Args:
            include_header: Whether to include header
            **kwargs: Additional CSV options

        Returns:
            Dictionary of table names to CSV bytes
        """
        import csv

        result: dict[str, bytes] = {}

        # Process each table (main table and child tables)
        tables = {"main": self.main_table, **self.child_tables}

        for table_name, records in tables.items():
            # Skip empty tables
            if not records:
                result[table_name] = b""
                continue

            # Create in-memory buffer
            buffer = io.StringIO()

            # Get field names from records
            if records:
                fieldnames = list(records[0].keys())
            else:
                fieldnames = []

            # Create CSV writer
            writer = csv.DictWriter(buffer, fieldnames=fieldnames, **kwargs)

            # Write header if requested
            if include_header:
                writer.writeheader()

            # Write records
            writer.writerows(records)

            # Convert to bytes
            result[table_name] = buffer.getvalue().encode("utf-8")

        return result

    def to_json_bytes(
        self, indent: Optional[int] = None, **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert all tables to JSON bytes.

        Args:
            indent: Indentation level for JSON formatting
            **kwargs: Additional JSON formatting options

        Returns:
            Dictionary of table names to JSON bytes
        """
        # Check cache first
        cache_key = _get_cache_key(
            self.to_dict(), "json_bytes", indent=indent, **kwargs
        )
        if cache_key in _conversion_cache:
            return cast(dict[str, bytes], _conversion_cache[cache_key])

        # Try orjson first if available
        if _check_orjson_available():
            try:
                result = self._to_json_bytes_orjson(indent, **kwargs)

                # Cache the result if using eager mode
                if self.conversion_mode == ConversionMode.EAGER:
                    _conversion_cache[cache_key] = result

                return result
            except Exception as e:
                logger.debug(f"orjson conversion failed, falling back to stdlib: {e}")
                # Fall back to stdlib if orjson fails

        # Use Python's standard library json module
        result = self._to_json_bytes_stdlib(indent, **kwargs)

        # Cache the result if using eager mode
        if self.conversion_mode == ConversionMode.EAGER:
            _conversion_cache[cache_key] = result

        return result

    def _to_json_bytes_orjson(
        self, indent: Optional[int] = None, **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert tables to JSON bytes using orjson.

        Args:
            indent: Indentation level (ignored for orjson)
            **kwargs: Additional orjson options

        Returns:
            Dictionary of table names to JSON bytes

        Raises:
            MissingDependencyError: If orjson is not available
        """
        if not _check_orjson_available():
            raise MissingDependencyError(
                "orjson is required for optimized JSON conversion. "
                "Falling back to standard library.",
                package="orjson",
                feature="json",
            )

        try:
            import orjson

            # Convert to JSON-serializable dict
            tables = self.to_json_objects()
            result: dict[str, bytes] = {}

            # Options for orjson
            options = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NON_STR_KEYS
            if indent is not None:
                options |= orjson.OPT_INDENT_2

            # Convert each table
            for table_name, records in tables.items():
                result[table_name] = orjson.dumps(records, option=options)

            return result
        except Exception as e:
            logger.error(f"Error converting to JSON with orjson: {e}")
            raise OutputError(
                f"Failed to convert to JSON: {e}", output_format="json"
            ) from e

    def _to_json_bytes_stdlib(
        self, indent: Optional[int] = None, **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert tables to JSON bytes using standard library.

        Args:
            indent: Indentation level for JSON formatting
            **kwargs: Additional JSON formatting options

        Returns:
            Dictionary of table names to JSON bytes
        """
        # Convert to JSON-serializable dict
        tables = self.to_json_objects()
        result: dict[str, bytes] = {}

        # Convert each table
        for table_name, records in tables.items():
            json_str = json.dumps(records, indent=indent, **kwargs)
            result[table_name] = json_str.encode("utf-8")

        return result

    def write(
        self, format_name: str, base_path: str, **format_options: Any
    ) -> dict[str, str]:
        """Write all tables to files of the specified format.

        Args:
            format_name: Format to write (e.g., 'csv', 'json', 'parquet')
            base_path: Base path for output files
            **format_options: Format-specific options

        Returns:
            Dictionary mapping table names to output file paths

        Raises:
            OutputError: If the output format is not supported
        """
        # Ensure the output directory exists
        os.makedirs(base_path, exist_ok=True)

        # Check if format is supported
        if not is_format_available(format_name):
            raise OutputError(f"Output format {format_name} is not available")

        # Get writer for this format
        writer = create_writer(format_name)

        # Write each table to a file
        output_files = {}

        # Write main table
        main_table_name = self.entity_name
        main_filename = os.path.join(base_path, f"{main_table_name}.{format_name}")
        writer.write(self.main_table, main_filename, **format_options)
        output_files[main_table_name] = main_filename

        # Write child tables
        for table_name, data in self.child_tables.items():
            # Get a safe filename for the table
            safe_table_name = self.get_formatted_table_name(table_name)
            output_filename = os.path.join(
                base_path, f"{safe_table_name}.{format_name}"
            )
            writer.write(data, output_filename, **format_options)
            output_files[table_name] = output_filename

        return output_files

    @classmethod
    def combine_results(
        cls,
        results: list["ProcessingResult"],
        entity_name: Optional[str] = None,
    ) -> "ProcessingResult":
        """Combine multiple processing results into a single result.

        Args:
            results: List of ProcessingResult objects to combine
            entity_name: Entity name for the combined result
                (default: use first result's entity name)

        Returns:
            Combined ProcessingResult object
        """
        if not results:
            return cls([], {}, "empty_result")

        # Take entity name from first result if not provided
        if entity_name is None:
            entity_name = results[0].entity_name

        # Combine main tables
        combined_main: list[dict[str, Any]] = []
        for result in results:
            combined_main.extend(result.main_table)

        # Combine child tables
        combined_children: dict[str, list[dict[str, Any]]] = {}
        for result in results:
            for table_name, records in result.child_tables.items():
                if table_name in combined_children:
                    combined_children[table_name].extend(records)
                else:
                    combined_children[table_name] = records.copy()

        # Create a new result
        combined_result = cls(
            combined_main,
            combined_children,
            entity_name,
            # Combine source_info from all results
            source_info={"combined_from": len(results)},
        )

        return combined_result

    def register_converter(
        self, format_name: str, converter_func: Callable[["ProcessingResult", Any], Any]
    ) -> None:
        """Register a custom converter function for a specific format.

        Args:
            format_name: Format name to register
            converter_func: Function that converts this result to the desired format
        """
        # Store the converter function
        self._conversion_functions[format_name] = converter_func

    def convert_to(self, format_name: str, **options: Any) -> Any:
        """Convert the result to a specific format using registered converters.

        Args:
            format_name: Format to convert to
            **options: Format-specific options

        Returns:
            Converted data in the requested format

        Raises:
            ValueError: If the format has no registered converter
        """
        # Check if we have a converter function for this format
        if format_name in self._conversion_functions:
            # Call the converter function with this result
            converter = self._conversion_functions[format_name]
            # Call the converter function with self and options parameters
            return converter(self, options)

        # Format not found - try using built-in methods
        format_method_name = f"to_{format_name}"
        if hasattr(self, format_method_name):
            method = getattr(self, format_method_name)
            return method(**options)

        # No converter found
        raise ValueError(f"No converter registered for format: {format_name}")

    def _clear_intermediate_data(self) -> None:
        """Clear intermediate data representations to save memory.

        Should only be called after final output is generated in memory-efficient mode.
        """
        # Clear converted format cache
        self._converted_formats.clear()

        # In extreme memory-efficient mode, we could also clear the original data
        # but this would make the result object unusable for further operations
        # Uncomment the following lines for extreme memory efficiency if needed:
        # self.main_table = []
        # self.child_tables = {}

    def with_conversion_mode(self, mode: ConversionMode) -> "ProcessingResult":
        """Create a new result with a different conversion mode.

        Args:
            mode: New conversion mode to use

        Returns:
            New ProcessingResult instance with the specified mode
        """
        return ProcessingResult(
            main_table=self.main_table,
            child_tables=self.child_tables,
            entity_name=self.entity_name,
            source_info=self.source_info,
            conversion_mode=mode,
        )

    def __repr__(self) -> str:
        """Get string representation."""
        record_counts = self.count_records()
        main_count = record_counts.get("main", 0)
        child_counts = {k: v for k, v in record_counts.items() if k != "main"}
        return (
            f"ProcessingResult(entity={self.entity_name}, "
            f"main_records={main_count}, "
            f"child_tables={len(child_counts)})"
        )

    def write_to_file(
        self,
        format_name: str,
        output_directory: str,
        **options: Any,
    ) -> dict[str, str]:
        """Write results to files using the specified writer.

        Args:
            format_name: Format to write in
            output_directory: Directory to write files to
            **options: Writer-specific options

        Returns:
            Dictionary mapping table names to output file paths

        Raises:
            OutputError: If the writer is not available or writing fails
        """
        if not is_format_available(format_name):
            raise OutputError(
                f"Format '{format_name}' is not available",
                output_format=format_name,
            )

        writer = create_writer(format_name)
        os.makedirs(output_directory, exist_ok=True)
        file_paths: dict[str, str] = {}

        try:
            # Write main table
            main_path = os.path.join(
                output_directory, f"{self.entity_name}.{format_name}"
            )
            with open(main_path, "wb") as f:
                writer.write(self.main_table, f, **options)
            file_paths["main"] = main_path

            # Write child tables
            for table_name, table_data in self.child_tables.items():
                # Skip empty tables
                if not table_data:
                    continue

                # Format the table name
                formatted_name = self.get_formatted_table_name(table_name)
                file_path = os.path.join(
                    output_directory, f"{formatted_name}.{format_name}"
                )

                # Write the data
                with open(file_path, "wb") as f:
                    writer.write(table_data, f, **options)
                file_paths[table_name] = file_path

            return file_paths
        except Exception as e:
            raise OutputError(
                f"Failed to write {format_name} files: {e}",
                output_format=format_name,
                path=output_directory,
            ) from e

    def stream_to_output(
        self,
        format_name: str,
        output_destination: Optional[Union[str, BinaryIO]] = None,
        **options: Any,
    ) -> None:
        """Stream results to an output destination.

        Args:
            format_name: Format to stream in
            output_destination: Output destination (file path or file-like object)
            **options: Format-specific options

        Raises:
            OutputError: If streaming fails
        """
        if not is_format_available(format_name):
            raise OutputError(
                f"Format '{format_name}' is not available",
                output_format=format_name,
            )

        # Create a writer for the format
        writer = create_streaming_writer(format_name, destination=output_destination)

        try:
            # Write all tables
            if hasattr(writer, "write_all_tables"):
                writer.write_all_tables(
                    self.main_table, self.child_tables, self.entity_name, **options
                )
            else:
                # Fall back to individual writes
                writer.write_main_records(self.main_table, **options)
                for table_name, table_data in self.child_tables.items():
                    writer.write_child_records(table_name, table_data, **options)
        except Exception as e:
            raise OutputError(
                f"Failed to stream {format_name} data: {e}",
                output_format=format_name,
            ) from e

    def count_records(self) -> dict[str, int]:
        """Count records in all tables."""
        counts = {"main": len(self.main_table)}
        for table_name, table_data in self.child_tables.items():
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

    def write_all_json(
        self, base_path: str, indent: Optional[int] = 2, **kwargs: Any
    ) -> dict[str, str]:
        """Write all tables to JSON files.

        Args:
            base_path: Base path for output files
            indent: Indentation level for JSON formatting
            **kwargs: Additional JSON writer options

        Returns:
            Dictionary of table names to file paths

        Raises:
            OutputError: If writing fails
        """
        # Create the base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Convert to dictionary structure with tables
        tables = self.to_json_objects()

        # Keep track of the paths
        file_paths: dict[str, str] = {}

        try:
            # Process each table
            for table_name, records in tables.items():
                # Skip empty tables
                if not records:
                    continue

                # Create the formatted table name
                formatted_name = self.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.json")

                # Write to JSON file
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(records, f, indent=indent, **kwargs)

                # Record the file path
                file_paths[table_name] = file_path

            return file_paths
        except Exception as e:
            raise OutputError(
                f"Failed to write JSON files: {e}",
                output_format="json",
                path=base_path,
            ) from e

    def write_all_csv(
        self, base_path: str, include_header: bool = True, **kwargs: Any
    ) -> dict[str, str]:
        """Write all tables to CSV files.

        Args:
            base_path: Base path for output files
            include_header: Whether to include headers in CSV files
            **kwargs: Additional CSV writer options

        Returns:
            Dictionary of table names to file paths

        Raises:
            OutputError: If writing fails
        """
        # Create the base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Get CSV bytes for each table
        csv_bytes = self.to_csv_bytes(include_header=include_header, **kwargs)

        # Keep track of the paths
        file_paths: dict[str, str] = {}

        try:
            # Process each table
            for table_name, data in csv_bytes.items():
                # Skip empty tables
                if not data:
                    continue

                # Create the formatted table name
                formatted_name = self.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.csv")

                # Write to CSV file
                with open(file_path, "wb") as f:
                    f.write(data)

                # Record the file path
                file_paths[table_name] = file_path

            return file_paths
        except Exception as e:
            raise OutputError(
                f"Failed to write CSV files: {e}",
                output_format="csv",
                path=base_path,
            ) from e
