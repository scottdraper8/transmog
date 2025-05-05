"""
ProcessingResult module for managing processing outputs.

This module contains the ProcessingResult class for managing and
writing the results of processing nested JSON structures.
"""

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    Callable,
    BinaryIO,
    TextIO,
)
import os
import logging
import io
import json
from enum import Enum

from transmog.types.base import JsonDict
from transmog.types.result_types import ResultInterface, ConversionModeType
from transmog.types.io_types import WriterProtocol
from transmog.error import OutputError
from transmog.io.writer_factory import (
    create_writer,
    is_format_available,
)

logger = logging.getLogger(__name__)

# Cache for conversions to avoid redundant work
_conversion_cache = {}


def _check_pyarrow_available() -> bool:
    """
    Check if PyArrow is available for use.

    Returns:
        bool: Whether PyArrow is available
    """
    try:
        import pyarrow

        return True
    except ImportError:
        return False


def _check_orjson_available() -> bool:
    """
    Check if orjson is available for use.

    Returns:
        bool: Whether orjson is available
    """
    try:
        import orjson

        return True
    except ImportError:
        return False


def _get_cache_key(table_data, format_type, **options):
    """
    Generate a cache key for table data conversions.

    Args:
        table_data: The data to be converted
        format_type: The target format type
        **options: Format-specific options

    Returns:
        A hashable cache key
    """
    # For mutable collections we use id() as part of the key
    # Not perfect, but helps with common cases
    data_id = id(table_data)
    options_str = str(sorted(options.items()))
    return (data_id, format_type, options_str)


class ConversionMode(Enum):
    """Conversion mode for ProcessingResult."""

    EAGER = "eager"  # Convert immediately, keep all data in memory
    LAZY = "lazy"  # Convert only when needed
    MEMORY_EFFICIENT = "memory_efficient"  # Discard intermediate data after conversion


class ProcessingResult(ResultInterface):
    """
    Container for processing results including main and child tables.

    The ProcessingResult manages the outputs of processing, providing
    access to the main table and child tables, as well as methods to
    convert the data to different formats or save to files.
    """

    def __init__(
        self,
        main_table: List[JsonDict],
        child_tables: Dict[str, List[JsonDict]],
        entity_name: str,
        source_info: Optional[Dict[str, Any]] = None,
        conversion_mode: Union[
            ConversionMode, ConversionModeType
        ] = ConversionMode.EAGER,
    ):
        """
        Initialize with main and child tables.

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

        self._converted_formats = {}
        self._conversion_functions = {}

        # Initialize cache
        global _conversion_cache
        _conversion_cache = {}

    def get_main_table(self) -> List[JsonDict]:
        """Get the main table data."""
        return self.main_table

    def get_child_table(self, table_name: str) -> List[JsonDict]:
        """Get a child table by name."""
        return self.child_tables.get(table_name, [])

    def get_table_names(self) -> List[str]:
        """Get list of all child table names."""
        return list(self.child_tables.keys())

    def get_formatted_table_name(self, table_name: str) -> str:
        """
        Get a formatted table name suitable for file saving.

        Args:
            table_name: The table name to format

        Returns:
            Formatted table name
        """
        return table_name.replace(".", "_").replace("/", "_")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary representation.

        Returns:
            Dict with main and child tables
        """
        if "dict" in self._converted_formats:
            return self._converted_formats["dict"]

        result = {
            "main_table": self.main_table,
            "child_tables": self.child_tables,
            "entity_name": self.entity_name,
            "source_info": self.source_info,
        }

        if self.conversion_mode == ConversionMode.EAGER:
            self._converted_formats["dict"] = result

        return result

    def to_json(self, indent: Optional[int] = 2) -> str:
        """
        Convert to JSON string.

        Args:
            indent: Indentation level for JSON formatting

        Returns:
            JSON string representation
        """
        key = f"json_{indent}"
        if key in self._converted_formats:
            return self._converted_formats[key]

        try:
            json_string = json.dumps(self.to_dict(), indent=indent)

            if self.conversion_mode == ConversionMode.EAGER:
                self._converted_formats[key] = json_string

            return json_string
        except Exception as e:
            logger.error(f"Error converting to JSON: {e}")
            raise OutputError(f"Failed to convert result to JSON: {e}")

    def to_json_objects(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Convert all tables to JSON-serializable Python objects.

        This ensures all values in the dictionaries are serializable by JSON encoders.

        Returns:
            Dict with 'main' and child table names as keys, and lists of JSON-serializable records as values
        """
        # Use cache key based on object identity
        cache_key = _get_cache_key(self, "json_objects")
        if cache_key in _conversion_cache:
            return _conversion_cache[cache_key]

        # Convert main table
        result = {"main": self._ensure_json_serializable(self.main_table)}

        # Convert child tables
        for table_name, table_data in self.child_tables.items():
            result[table_name] = self._ensure_json_serializable(table_data)

        # Cache the result
        _conversion_cache[cache_key] = result
        return result

    def _ensure_json_serializable(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Ensure data is JSON serializable by converting non-serializable values.

        Args:
            data: List of records to convert

        Returns:
            List of records with JSON-serializable values
        """
        if not data:
            return []

        result = []
        for record in data:
            json_record = {}
            for key, value in record.items():
                if value is None:
                    json_record[key] = None
                elif isinstance(value, (str, int, float, bool)):
                    json_record[key] = value
                else:
                    # Try to convert to string
                    try:
                        json_record[key] = str(value)
                    except Exception:
                        json_record[key] = None
            result.append(json_record)
        return result

    def to_pyarrow_tables(self) -> Dict[str, Any]:
        """
        Convert all tables to PyArrow Tables.

        Returns:
            Dict with 'main' and child table names as keys, and PyArrow Tables as values

        Raises:
            ImportError: If PyArrow is not available
        """
        if not _check_pyarrow_available():
            raise ImportError(
                "PyArrow is required for this operation. "
                "Install with: pip install pyarrow"
            )

        # Use cache key based on object identity
        cache_key = _get_cache_key(self, "pyarrow_tables")
        if cache_key in _conversion_cache:
            return _conversion_cache[cache_key]

        import pyarrow as pa

        # Convert tables to PyArrow Tables
        result = {}

        # Convert main table
        if self.main_table:
            main_table = self._dict_list_to_pyarrow(self.main_table)
            result["main"] = main_table
        else:
            # Empty table with no schema
            result["main"] = pa.table({})

        # Convert child tables
        for table_name, table_data in self.child_tables.items():
            if table_data:
                child_table = self._dict_list_to_pyarrow(table_data)
                result[table_name] = child_table
            else:
                # Empty table with no schema
                result[table_name] = pa.table({})

        # Cache the result
        _conversion_cache[cache_key] = result
        return result

    def _dict_list_to_pyarrow(self, data: List[Dict[str, Any]]) -> Any:
        """
        Convert a list of dictionaries to a PyArrow Table.

        Args:
            data: List of records to convert

        Returns:
            PyArrow Table

        Raises:
            ImportError: If PyArrow is not available
        """
        if not data:
            import pyarrow as pa

            return pa.table({})

        # Extract columns from dictionaries
        columns = {}
        for key in data[0].keys():
            columns[key] = [record.get(key) for record in data]

        import pyarrow as pa

        return pa.table(columns)

    def to_parquet_bytes(
        self, compression: str = "snappy", **kwargs
    ) -> Dict[str, bytes]:
        """
        Convert all tables to Parquet bytes.

        Args:
            compression: Compression format (snappy, gzip, None, etc.)
            **kwargs: Additional PyArrow Parquet options

        Returns:
            Dict mapping table names to Parquet bytes

        Raises:
            ImportError: If PyArrow is not available
        """
        if not _check_pyarrow_available():
            raise ImportError(
                "PyArrow is required for Parquet serialization. "
                "Install with: pip install pyarrow"
            )

        # Use cache key
        cache_key = _get_cache_key(
            self, "parquet_bytes", compression=compression, **kwargs
        )
        if cache_key in _conversion_cache:
            return _conversion_cache[cache_key]

        import pyarrow.parquet as pq

        # Get PyArrow tables
        tables = self.to_pyarrow_tables()

        # Convert each table to Parquet bytes
        result = {}
        for table_name, table in tables.items():
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression=compression, **kwargs)
            buffer.seek(0)
            result[table_name] = buffer.getvalue()

        # Cache the result
        _conversion_cache[cache_key] = result
        return result

    def to_csv_bytes(self, include_header: bool = True, **kwargs) -> Dict[str, bytes]:
        """
        Convert all tables to CSV bytes.

        Args:
            include_header: Whether to include header row
            **kwargs: Additional CSV options

        Returns:
            Dict mapping table names to CSV bytes
        """
        # Use cache key
        cache_key = _get_cache_key(
            self, "csv_bytes", include_header=include_header, **kwargs
        )
        if cache_key in _conversion_cache:
            return _conversion_cache[cache_key]

        # Check if we can create a CSV writer
        if is_format_available("csv"):
            # Use the writer to get bytes
            writer = create_writer("csv")
            result = {}

            # Process main table
            buffer = io.BytesIO()
            writer.write(
                self.main_table, buffer, include_header=include_header, **kwargs
            )
            result["main"] = buffer.getvalue()

            # Process child tables
            for table_name, table_data in self.child_tables.items():
                buffer = io.BytesIO()
                writer.write(
                    table_data, buffer, include_header=include_header, **kwargs
                )
                result[table_name] = buffer.getvalue()

            # Cache and return
            _conversion_cache[cache_key] = result
            return result

        # If writer not available, fall back to direct implementation
        # Based on availability of PyArrow
        if _check_pyarrow_available():
            return self._to_csv_bytes_pyarrow(include_header, **kwargs)
        else:
            return self._to_csv_bytes_stdlib(include_header, **kwargs)

    def _to_csv_bytes_pyarrow(
        self, include_header: bool = True, **kwargs
    ) -> Dict[str, bytes]:
        """
        Convert all tables to CSV bytes using PyArrow.

        Args:
            include_header: Whether to include header row
            **kwargs: Additional CSV options

        Returns:
            Dict mapping table names to CSV bytes
        """
        # Use cache key
        cache_key = _get_cache_key(
            self, "csv_bytes_pyarrow", include_header=include_header, **kwargs
        )
        if cache_key in _conversion_cache:
            return _conversion_cache[cache_key]

        import pyarrow as pa
        import pyarrow.csv as pa_csv
        from transmog.naming.conventions import sanitize_column_names

        # Get PyArrow tables
        tables = self.to_pyarrow_tables()

        # Get separator for sanitization
        separator = kwargs.get("separator", "_")
        sanitize_header = kwargs.get("sanitize_header", True)

        # Write options
        write_options = pa_csv.WriteOptions(include_header=include_header)

        # Convert each table to CSV bytes
        result = {}
        for table_name, table in tables.items():
            # Sanitize column names if requested
            if sanitize_header:
                column_names = list(table.column_names)
                sanitized_names = sanitize_column_names(
                    column_names, separator=separator, sql_safe=True
                )

                # Rename columns if needed
                if column_names != sanitized_names:
                    columns = {}
                    for i, col in enumerate(column_names):
                        if i < len(sanitized_names):
                            columns[sanitized_names[i]] = table.column(col)
                    table = pa.Table.from_pydict(columns)

            # Write to buffer
            buffer = io.BytesIO()
            pa_csv.write_csv(table, buffer, write_options)
            buffer.seek(0)
            result[table_name] = buffer.getvalue()

        # Cache the result
        _conversion_cache[cache_key] = result
        return result

    def _to_csv_bytes_stdlib(
        self, include_header: bool = True, **kwargs
    ) -> Dict[str, bytes]:
        """
        Convert all tables to CSV bytes using standard library.

        Args:
            include_header: Whether to include header row
            **kwargs: Additional CSV options

        Returns:
            Dict mapping table names to CSV bytes
        """
        # Use cache key
        cache_key = _get_cache_key(
            self, "csv_bytes_stdlib", include_header=include_header, **kwargs
        )
        if cache_key in _conversion_cache:
            return _conversion_cache[cache_key]

        import csv
        from transmog.naming.conventions import sanitize_column_names

        # Get options
        separator = kwargs.get("separator", "_")
        sanitize_header = kwargs.get("sanitize_header", True)
        delimiter = kwargs.get("delimiter", ",")
        quotechar = kwargs.get("quotechar", '"')

        result = {}

        # Process main table
        if self.main_table:
            buffer = io.StringIO()
            fieldnames = list(self.main_table[0].keys())

            # Sanitize column names if requested
            if sanitize_header:
                fieldnames = sanitize_column_names(
                    fieldnames, separator=separator, sql_safe=True
                )

            # Create CSV writer
            writer = csv.writer(buffer, delimiter=delimiter, quotechar=quotechar)

            # Write header if requested
            if include_header:
                writer.writerow(fieldnames)

            # Write data rows
            for record in self.main_table:
                row = []
                for i, orig_key in enumerate(record.keys()):
                    if i < len(fieldnames):
                        value = record[orig_key]
                        # Convert None to empty string for CSV
                        if value is None:
                            value = ""
                        row.append(value)
                writer.writerow(row)

            result["main"] = buffer.getvalue().encode("utf-8")
        else:
            result["main"] = b""

        # Process child tables
        for table_name, table_data in self.child_tables.items():
            if table_data:
                buffer = io.StringIO()
                fieldnames = list(table_data[0].keys())

                # Sanitize column names if requested
                if sanitize_header:
                    fieldnames = sanitize_column_names(
                        fieldnames, separator=separator, sql_safe=True
                    )

                # Create CSV writer
                writer = csv.writer(buffer, delimiter=delimiter, quotechar=quotechar)

                # Write header if requested
                if include_header:
                    writer.writerow(fieldnames)

                # Write data rows
                for record in table_data:
                    row = []
                    for i, orig_key in enumerate(record.keys()):
                        if i < len(fieldnames):
                            value = record[orig_key]
                            # Convert None to empty string for CSV
                            if value is None:
                                value = ""
                            row.append(value)
                    writer.writerow(row)

                result[table_name] = buffer.getvalue().encode("utf-8")
            else:
                result[table_name] = b""

        # Cache the result
        _conversion_cache[cache_key] = result
        return result

    def to_json_bytes(self, indent: Optional[int] = None, **kwargs) -> Dict[str, bytes]:
        """
        Convert all tables to JSON bytes.

        Args:
            indent: Indentation level (None for no indentation)
            **kwargs: Additional JSON options

        Returns:
            Dict mapping table names to JSON bytes
        """
        # Try orjson first for best performance
        if _check_orjson_available():
            return self._to_json_bytes_orjson(indent, **kwargs)
        else:
            return self._to_json_bytes_stdlib(indent, **kwargs)

    def _to_json_bytes_orjson(
        self, indent: Optional[int] = None, **kwargs
    ) -> Dict[str, bytes]:
        """
        Convert all tables to JSON bytes using orjson.

        Args:
            indent: Indentation level (None for no indentation)
            **kwargs: Additional orjson options

        Returns:
            Dict mapping table names to JSON bytes
        """
        # Use cache key
        cache_key = _get_cache_key(self, "json_bytes_orjson", indent=indent, **kwargs)
        if cache_key in _conversion_cache:
            return _conversion_cache[cache_key]

        import orjson

        # Get JSON serializable objects
        json_objects = self.to_json_objects()

        result = {}
        for table_name, table_data in json_objects.items():
            # orjson doesn't support indentation natively
            # For indented output, we'll need to decode and re-encode
            if indent is not None:
                import json

                # Convert to string (pretty), then encode to bytes
                json_str = json.dumps(table_data, indent=indent)
                result[table_name] = json_str.encode("utf-8")
            else:
                # Direct orjson bytes output (faster)
                result[table_name] = orjson.dumps(table_data, **kwargs)

        # Cache the result
        _conversion_cache[cache_key] = result
        return result

    def _to_json_bytes_stdlib(
        self, indent: Optional[int] = None, **kwargs
    ) -> Dict[str, bytes]:
        """
        Convert all tables to JSON bytes using standard library.

        Args:
            indent: Indentation level (None for no indentation)
            **kwargs: Additional JSON options

        Returns:
            Dict mapping table names to JSON bytes
        """
        # Use cache key
        cache_key = _get_cache_key(self, "json_bytes_stdlib", indent=indent, **kwargs)
        if cache_key in _conversion_cache:
            return _conversion_cache[cache_key]

        import json

        # Get JSON serializable objects
        json_objects = self.to_json_objects()

        result = {}
        for table_name, table_data in json_objects.items():
            json_str = json.dumps(table_data, indent=indent, **kwargs)
            result[table_name] = json_str.encode("utf-8")

        # Cache the result
        _conversion_cache[cache_key] = result
        return result

    def write(
        self, format_name: str, base_path: str, **format_options
    ) -> Dict[str, str]:
        """
        Write all tables to the specified format.

        Args:
            format_name: Format to write (json, parquet, csv, etc.)
            base_path: Base directory for output files
            **format_options: Format-specific options

        Returns:
            Dictionary of table names to file paths

        Raises:
            ImportError: If the required writer dependencies are not available
            ValueError: If the format is not supported
        """
        # Check if the format is available
        if not is_format_available(format_name):
            # Try to import format writers to make them available
            from transmog.io import initialize_io_features

            initialize_io_features()

            # Check again after initialization
            if not is_format_available(format_name):
                from transmog.io import get_supported_formats

                available = get_supported_formats()

                if not available:
                    available_msg = "(none - install optional dependencies)"
                else:
                    available_msg = ", ".join(available)

                raise ValueError(
                    f"Format '{format_name}' is not available. "
                    f"You may need to install additional dependencies. "
                    f"Available formats: {available_msg}"
                )

        # Create the writer and use it
        writer = create_writer(format_name)

        # Create the output directory
        os.makedirs(base_path, exist_ok=True)

        result = {}

        # Write main table
        main_path = os.path.join(base_path, f"{self.entity_name}.{format_name}")
        with open(main_path, "wb") as f:
            writer.write(self.main_table, f, **format_options)
        result["main"] = main_path

        # Write child tables
        for table_name, table_data in self.child_tables.items():
            formatted_name = self.get_formatted_table_name(table_name)
            file_path = os.path.join(base_path, f"{formatted_name}.{format_name}")
            with open(file_path, "wb") as f:
                writer.write(table_data, f, **format_options)
            result[table_name] = file_path

        return result

    def write_all_parquet(
        self, base_path: str, compression: str = "snappy", **kwargs
    ) -> Dict[str, str]:
        """
        Write all tables to Parquet files.

        This is a convenience method that first converts data to PyArrow Tables
        internally, then writes to the file system.

        Args:
            base_path: Base output directory
            compression: Compression format
            **kwargs: Additional Parquet options

        Returns:
            Dictionary of table names to file paths
        """
        try:
            # Get Parquet bytes
            parquet_data = self.to_parquet_bytes(compression=compression, **kwargs)

            # Create base directory
            os.makedirs(base_path, exist_ok=True)

            # Write each table to disk
            result = {}

            # Write main table
            main_path = os.path.join(base_path, f"{self.entity_name}.parquet")
            with open(main_path, "wb") as f:
                f.write(parquet_data["main"])
            result["main"] = main_path

            # Write child tables
            for table_name, data in parquet_data.items():
                if table_name == "main":
                    continue

                formatted_name = self.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.parquet")
                with open(file_path, "wb") as f:
                    f.write(data)
                result[table_name] = file_path

            return result

        except ImportError as e:
            logger.warning(f"Could not write Parquet files: {str(e)}")
            logger.warning("PyArrow is required for Parquet output.")
            logger.warning("Install with: pip install pyarrow")

            # Create the base directory if it doesn't exist
            os.makedirs(base_path, exist_ok=True)

            # Create dummy output paths for backward compatibility
            results = {"main": f"{base_path}/{self.entity_name}.parquet"}

            # Create dummy file with warning message
            with open(results["main"], "w") as f:
                f.write(
                    "PyArrow required for Parquet output. This is a placeholder file."
                )

            # Add dummy paths for child tables
            for table_name in self.child_tables:
                formatted_name = self.get_formatted_table_name(table_name)
                file_path = f"{base_path}/{formatted_name}.parquet"
                results[table_name] = file_path

                # Create dummy file with warning message
                with open(file_path, "w") as f:
                    f.write(
                        "PyArrow required for Parquet output. This is a placeholder file."
                    )

            return results

    def write_all_json(
        self, base_path: str, indent: Optional[int] = 2, **kwargs
    ) -> Dict[str, str]:
        """
        Write all tables to JSON files.

        This is a convenience method that first converts data to JSON bytes
        internally, then writes to the file system.

        Args:
            base_path: Base output directory
            indent: JSON indentation (None for no indentation)
            **kwargs: Additional JSON options

        Returns:
            Dictionary of table names to file paths
        """
        # Get JSON bytes
        json_data = self.to_json_bytes(indent=indent, **kwargs)

        # Create base directory
        os.makedirs(base_path, exist_ok=True)

        # Write each table to disk
        result = {}

        # Write main table
        main_path = os.path.join(base_path, f"{self.entity_name}.json")
        with open(main_path, "wb") as f:
            f.write(json_data["main"])
        result["main"] = main_path

        # Write child tables
        for table_name, data in json_data.items():
            if table_name == "main":
                continue

            formatted_name = self.get_formatted_table_name(table_name)
            file_path = os.path.join(base_path, f"{formatted_name}.json")
            with open(file_path, "wb") as f:
                f.write(data)
            result[table_name] = file_path

        return result

    def write_all_csv(
        self, base_path: str, include_header: bool = True, **kwargs
    ) -> Dict[str, str]:
        """
        Write all tables to CSV files.

        This method uses the writer registry if available, or falls back to
        direct implementation.

        Args:
            base_path: Base output directory
            include_header: Whether to include header row
            **kwargs: Additional CSV options

        Returns:
            Dictionary of table names to file paths
        """
        # Try to create a CSV writer
        if is_format_available("csv"):
            writer = create_writer("csv")

            # Create the output directory
            os.makedirs(base_path, exist_ok=True)

            result = {}

            # Write main table
            main_path = os.path.join(base_path, f"{self.entity_name}.csv")
            with open(main_path, "wb") as f:
                writer.write(
                    self.main_table, f, include_header=include_header, **kwargs
                )
            result["main"] = main_path

            # Write child tables
            for table_name, table_data in self.child_tables.items():
                formatted_name = self.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.csv")
                with open(file_path, "wb") as f:
                    writer.write(table_data, f, include_header=include_header, **kwargs)
                result[table_name] = file_path

            return result

        # Fall back to direct implementation if writer not available
        # First get CSV bytes
        csv_data = self.to_csv_bytes(include_header=include_header, **kwargs)

        # Create base directory
        os.makedirs(base_path, exist_ok=True)

        # Write each table to disk
        result = {}

        # Write main table
        main_path = os.path.join(base_path, f"{self.entity_name}.csv")
        with open(main_path, "wb") as f:
            f.write(csv_data["main"])
        result["main"] = main_path

        # Write child tables
        for table_name, data in csv_data.items():
            if table_name == "main":
                continue

            formatted_name = self.get_formatted_table_name(table_name)
            file_path = os.path.join(base_path, f"{formatted_name}.csv")
            with open(file_path, "wb") as f:
                f.write(data)
            result[table_name] = file_path

        return result

    @classmethod
    def combine_results(
        cls,
        results: List["ProcessingResult"],
        entity_name: Optional[str] = None,
    ) -> "ProcessingResult":
        """
        Combine multiple processing results into a single result.

        This is useful when processing data in parallel or in chunks.

        Args:
            results: List of ProcessingResult objects to combine
            entity_name: Optional entity name for the combined result

        Returns:
            Combined ProcessingResult
        """
        if not results:
            return cls([], {}, entity_name or "combined")

        # Use the entity name from the first result if not provided
        if entity_name is None:
            entity_name = results[0].entity_name

        # Combine main tables
        combined_main = []
        for result in results:
            combined_main.extend(result.main_table)

        # Combine child tables
        combined_children = {}
        for result in results:
            for table_name, table_data in result.child_tables.items():
                if table_name not in combined_children:
                    combined_children[table_name] = []
                combined_children[table_name].extend(table_data)

        return cls(combined_main, combined_children, entity_name)

    def register_converter(
        self, format_name: str, converter_func: Callable[["ProcessingResult"], Any]
    ) -> None:
        """
        Register a custom converter function for a format.

        Args:
            format_name: Name of the format to convert to
            converter_func: Function that converts this result to the format
        """
        self._conversion_functions[format_name] = converter_func

    def convert_to(self, format_name: str, **options) -> Any:
        """
        Convert the result to the specified format.

        Uses registered converters or custom conversion functions.

        Args:
            format_name: Format to convert to
            **options: Format-specific options

        Returns:
            Converted result
        """
        # Check if we have a registered converter
        if format_name in self._conversion_functions:
            return self._conversion_functions[format_name](self, **options)

        # Handle built-in formats
        if format_name == "dict":
            return self.to_dict()
        elif format_name == "json":
            return self.to_json(indent=options.get("indent", 2))
        else:
            raise ValueError(f"Unsupported format: {format_name}")

    def _clear_intermediate_data(self) -> None:
        """
        Clear intermediate data representations to save memory.

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
        """
        Create a new result with a different conversion mode.

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
        **options,
    ) -> Dict[str, str]:
        """
        Write results to files in the specified format.

        Args:
            format_name: Output format name
            output_directory: Directory to write files to
            **options: Format-specific options

        Returns:
            Dict mapping table names to output file paths
        """
        from transmog.io.writer_factory import create_writer

        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)

        # Create writer for the format
        writer = create_writer(format_name)

        # Write the result
        result_paths = writer.write_all_tables(
            main_table=self.main_table,
            child_tables=self.child_tables,
            base_path=output_directory,
            entity_name=self.entity_name,
            **options,
        )

        # Clean up intermediate representations if in memory-efficient mode
        if self.conversion_mode == ConversionMode.MEMORY_EFFICIENT:
            self._clear_intermediate_data()

        return result_paths

    def stream_to_output(
        self,
        format_name: str,
        output_destination: Optional[Union[str, BinaryIO, TextIO]] = None,
        **options,
    ) -> None:
        """
        Stream the result to the output destination in the specified format.

        This is a memory-efficient way to output large datasets without
        keeping everything in memory.

        Args:
            format_name: Output format name
            output_destination: Directory path or file-like object
            **options: Format-specific options

        Returns:
            None
        """
        from transmog.io.writer_factory import create_streaming_writer

        # Create streaming writer for the format
        writer = create_streaming_writer(
            format_name=format_name,
            destination=output_destination,
            entity_name=self.entity_name,
            **options,
        )

        try:
            # Write main table
            writer.initialize_main_table()
            writer.write_main_records(self.main_table)

            # Write child tables
            for table_name, table_data in self.child_tables.items():
                writer.initialize_child_table(table_name)
                writer.write_child_records(table_name, table_data)

            # Finalize output
            writer.finalize()
        finally:
            writer.close()

        # Clean up intermediate representations if in memory-efficient mode
        if self.conversion_mode == ConversionMode.MEMORY_EFFICIENT:
            self._clear_intermediate_data()

    def count_records(self) -> Dict[str, int]:
        """Count records in all tables."""
        counts = {"main": len(self.main_table)}
        for table_name, table_data in self.child_tables.items():
            counts[table_name] = len(table_data)
        return counts
