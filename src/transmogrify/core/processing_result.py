"""
ProcessingResult module for managing processing outputs.

This module contains the ProcessingResult class for managing and
writing the results of processing nested JSON structures.
"""

from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable, BinaryIO
import os
import logging
import importlib
import io

logger = logging.getLogger(__name__)

# Define a global variable to track if writers have been initialized
_WRITERS_REGISTRY = None
_WRITERS_AVAILABLE = False

# Cache for conversions to avoid redundant work
_conversion_cache = {}


def _get_writer_registry():
    """
    Lazy import the writer registry to avoid circular imports.

    Returns:
        The WriterRegistry class or None if it's not available
    """
    global _WRITERS_REGISTRY, _WRITERS_AVAILABLE

    # Only attempt to import once
    if _WRITERS_REGISTRY is None and not _WRITERS_AVAILABLE:
        try:
            # Use importlib for lazy loading to avoid circular imports
            io_module = importlib.import_module("src.transmogrify.io.writer_registry")
            _WRITERS_REGISTRY = io_module.WriterRegistry
            _WRITERS_AVAILABLE = True
        except (ImportError, AttributeError):
            _WRITERS_AVAILABLE = False
            logger.debug("Writer registry not available for import")

    return _WRITERS_REGISTRY


def _check_pyarrow_available():
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


def _check_orjson_available():
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


class ProcessingResult:
    """
    Container for processing results including main and child tables.

    The ProcessingResult manages the outputs of processing, providing
    access to the main table and child tables, as well as methods to
    convert the data to different formats or save to files.
    """

    def __init__(
        self,
        main_table: List[Dict[str, Any]],
        child_tables: Dict[str, List[Dict[str, Any]]],
        entity_name: str = "entity",
    ):
        """
        Initialize with main and child tables.

        Args:
            main_table: List of records for the main table
            child_tables: Dictionary of child tables keyed by name
            entity_name: Name of the entity
        """
        self.main_table = main_table
        self.child_tables = child_tables
        self.entity_name = entity_name

        # Initialize cache
        global _conversion_cache
        _conversion_cache = {}

    def get_main_table(self) -> List[Dict[str, Any]]:
        """Get the main table data."""
        return self.main_table

    def get_child_table(self, table_name: str) -> List[Dict[str, Any]]:
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

    def to_dict(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Convert all tables to a dictionary of Python dictionaries/lists.

        Returns:
            Dict with 'main' and child table names as keys, and lists of records as values
        """
        result = {"main": self.main_table}
        for table_name, table_data in self.child_tables.items():
            result[table_name] = table_data
        return result

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

        # Try to get the writer registry
        writer_registry = _get_writer_registry()
        if writer_registry and writer_registry.is_format_available("csv"):
            # Use the writer interface to get bytes
            writer = writer_registry.get_writer("csv")
            result = {}

            # Process main table
            buffer = io.BytesIO()
            writer.write_table(
                self.main_table, buffer, include_header=include_header, **kwargs
            )
            result["main"] = buffer.getvalue()

            # Process child tables
            for table_name, table_data in self.child_tables.items():
                buffer = io.BytesIO()
                writer.write_table(
                    table_data, buffer, include_header=include_header, **kwargs
                )
                result[table_name] = buffer.getvalue()

            # Cache and return
            _conversion_cache[cache_key] = result
            return result

        # If writer registry not available, fall back to direct implementation
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
        from src.transmogrify.naming.conventions import sanitize_column_names

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
        from src.transmogrify.naming.conventions import sanitize_column_names

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
        # Get the writer registry using lazy import
        writer_registry = _get_writer_registry()

        if writer_registry is None:
            raise ImportError(
                "Writer registry not available. Make sure transmogrify[io] is installed."
            )

        # Check if the format is available
        if not writer_registry.is_format_available(format_name):
            available = writer_registry.list_available_formats()
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
        writer = writer_registry.create_writer(format_name)
        return writer.write_all_tables(
            main_table=self.main_table,
            child_tables=self.child_tables,
            base_path=base_path,
            entity_name=self.entity_name,
            **format_options,
        )

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
        # Try to use the writer registry
        writer_registry = _get_writer_registry()
        if writer_registry and writer_registry.is_format_available("csv"):
            writer = writer_registry.get_writer("csv")
            return writer.write_all_tables(
                self.main_table,
                self.child_tables,
                base_path,
                self.entity_name,
                include_header=include_header,
                **kwargs,
            )

        # Fall back to direct implementation if registry not available
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
