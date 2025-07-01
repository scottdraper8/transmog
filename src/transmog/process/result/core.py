"""Core ProcessingResult class for managing processing outputs."""

from enum import Enum
from typing import Any, Optional, Union

from ...types.base import JsonDict
from ...types.result_types import ConversionModeType, ResultInterface


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
        """Convert result to a dictionary representation.

        Returns:
            Dictionary containing main table and child tables
        """
        result = {
            "main_table": self.main_table,
            "child_tables": self.child_tables,
            "entity_name": self.entity_name,
            "source_info": self.source_info,
        }
        return result

    def count_records(self) -> dict[str, int]:
        """Count records in all tables."""
        counts = {"main_table": len(self.main_table)}
        for table_name, records in self.child_tables.items():
            counts[table_name] = len(records)
        return counts

    @classmethod
    def combine_results(
        cls,
        results: list["ProcessingResult"],
        entity_name: Optional[str] = None,
    ) -> "ProcessingResult":
        """Combine multiple ProcessingResults into one.

        Args:
            results: List of ProcessingResult objects to combine
            entity_name: Optional entity name for the combined result

        Returns:
            Combined ProcessingResult
        """
        if not results:
            return cls([], {}, entity_name or "combined")

        # Use the first result's entity name if not provided
        if entity_name is None:
            entity_name = results[0].entity_name

        # Combine main tables
        combined_main = []
        for result in results:
            combined_main.extend(result.main_table)

        # Combine child tables
        combined_child: dict[str, list[JsonDict]] = {}
        for result in results:
            for table_name, records in result.child_tables.items():
                if table_name in combined_child:
                    combined_child[table_name].extend(records)
                else:
                    combined_child[table_name] = records.copy()

        # Combine source info
        combined_source_info = {}
        for result in results:
            combined_source_info.update(result.source_info)

        return cls(
            main_table=combined_main,
            child_tables=combined_child,
            entity_name=entity_name,
            source_info=combined_source_info,
        )

    def with_conversion_mode(self, mode: ConversionMode) -> "ProcessingResult":
        """Create a copy with a different conversion mode.

        Args:
            mode: New conversion mode

        Returns:
            New ProcessingResult with the specified conversion mode
        """
        return ProcessingResult(
            main_table=self.main_table.copy(),
            child_tables={k: v.copy() for k, v in self.child_tables.items()},
            entity_name=self.entity_name,
            source_info=self.source_info.copy(),
            conversion_mode=mode,
        )

    def to_json(self, indent: Optional[int] = 2) -> str:
        """Convert result to JSON string.

        Args:
            indent: Indentation level for JSON formatting

        Returns:
            JSON string representation
        """
        import json

        return json.dumps(self.to_dict(), indent=indent)

    def to_json_objects(self) -> dict[str, list[dict[str, Any]]]:
        """Convert to JSON-serializable dictionary of tables.

        Returns:
            Dictionary of table names to lists of records
        """
        # Ensure all data is JSON-serializable
        main_serializable = self._ensure_json_serializable(self.main_table)
        child_serializable = {}

        for table_name, records in self.child_tables.items():
            child_serializable[table_name] = self._ensure_json_serializable(records)

        return {"main": main_serializable, **child_serializable}

    def _ensure_json_serializable(
        self, data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Ensure data is JSON serializable.

        Args:
            data: List of dictionaries to make serializable

        Returns:
            JSON-serializable list of dictionaries
        """
        import json

        # Try to serialize and deserialize to catch non-serializable objects
        try:
            json_str = json.dumps(data)
            result = json.loads(json_str)
            # Ensure we return the correct type
            if isinstance(result, list):
                return result
            else:
                return [result] if isinstance(result, dict) else []
        except (TypeError, ValueError):
            # Handle non-serializable objects by converting them to strings
            serializable_data = []
            for record in data:
                serializable_record = {}
                for key, value in record.items():
                    try:
                        json.dumps(value)
                        serializable_record[key] = value
                    except (TypeError, ValueError):
                        # Convert non-serializable values to strings
                        serializable_record[key] = str(value)
                serializable_data.append(serializable_record)
            return serializable_data

    def to_pyarrow_tables(self) -> dict[str, Any]:
        """Convert all tables to PyArrow tables.

        Returns:
            Dictionary of table names to PyArrow tables

        Raises:
            MissingDependencyError: If PyArrow is not available
        """
        from transmog.error.exceptions import MissingDependencyError

        from .utils import _check_pyarrow_available

        if not _check_pyarrow_available():
            raise MissingDependencyError(
                "PyArrow is required for PyArrow table conversion",
                package="pyarrow",
                feature="pyarrow_tables",
            )

        tables = {"main": self.main_table, **self.child_tables}
        arrow_tables = {}

        for table_name, records in tables.items():
            if records:  # Only convert non-empty tables
                arrow_tables[table_name] = self._dict_list_to_pyarrow(records)

        return arrow_tables

    def _dict_list_to_pyarrow(
        self, data: list[dict[str, Any]], force_string_types: Optional[bool] = None
    ) -> Any:
        """Convert list of dictionaries to PyArrow table.

        Args:
            data: List of dictionaries to convert
            force_string_types: Whether to force all types to strings

        Returns:
            PyArrow table
        """
        import pyarrow as pa

        if not data:
            return pa.table({})

        # Get all column names from the data
        all_columns: set[str] = set()
        for record in data:
            all_columns.update(record.keys())
        column_names = sorted(all_columns)

        # Convert to column-oriented format
        columns = {}
        for col_name in column_names:
            columns[col_name] = [record.get(col_name) for record in data]

        # Convert to PyArrow table
        try:
            table = pa.table(columns)
            return table
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # If conversion fails, try with string types
            if force_string_types is False:
                raise

            # Convert all values to strings for compatibility
            string_columns = {}
            for col_name in column_names:
                string_columns[col_name] = [
                    None if record.get(col_name) is None else str(record.get(col_name))
                    for record in data
                ]

            return pa.table(string_columns)

    def register_converter(self, format_name: str, converter_func: Any) -> None:
        """Register a custom converter function for a specific format.

        Args:
            format_name: Format name to register
            converter_func: Function that converts this result to the desired format
        """
        # Store the converter function
        if not hasattr(self, "_conversion_functions"):
            self._conversion_functions = {}
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
        # Check if a converter function exists for this format
        if (
            hasattr(self, "_conversion_functions")
            and format_name in self._conversion_functions
        ):
            # Call the converter function with this result
            converter = self._conversion_functions[format_name]
            return converter(self, options)

        # Format not found - try using built-in methods
        format_method_name = f"to_{format_name}"
        if hasattr(self, format_method_name):
            method = getattr(self, format_method_name)
            return method(**options)

        # No converter found
        raise ValueError(f"No converter registered for format: {format_name}")

    def __repr__(self) -> str:
        """String representation of the result."""
        main_count = len(self.main_table)
        child_count = sum(len(records) for records in self.child_tables.values())
        return (
            f"ProcessingResult(entity='{self.entity_name}', "
            f"main_records={main_count}, child_records={child_count}, "
            f"child_tables={len(self.child_tables)})"
        )

    def to_parquet_bytes(self, **kwargs: Any) -> dict[str, bytes]:
        """Convert to Parquet bytes."""
        from .converters import ResultConverters

        converter = ResultConverters(self)
        return converter.to_parquet_bytes(**kwargs)

    def to_csv_bytes(self, **kwargs: Any) -> dict[str, bytes]:
        """Convert to CSV bytes."""
        from .converters import ResultConverters

        converter = ResultConverters(self)
        return converter.to_csv_bytes(**kwargs)

    def to_json_bytes(self, **kwargs: Any) -> dict[str, bytes]:
        """Convert to JSON bytes."""
        from .converters import ResultConverters

        converter = ResultConverters(self)
        return converter.to_json_bytes(**kwargs)

    def write(self, format_name: str, base_path: str, **kwargs: Any) -> dict[str, str]:
        """Write data to files in the specified format."""
        from .writers import ResultWriters

        writer = ResultWriters(self)
        return writer.write(format_name, base_path, **kwargs)

    def write_all_parquet(self, base_path: str, **kwargs: Any) -> dict[str, str]:
        """Write data to Parquet files."""
        from .writers import ResultWriters

        writer = ResultWriters(self)
        return writer.write_all_parquet(base_path, **kwargs)

    def write_all_json(self, base_path: str, **kwargs: Any) -> dict[str, str]:
        """Write data to JSON files."""
        from .writers import ResultWriters

        writer = ResultWriters(self)
        return writer.write_all_json(base_path, **kwargs)

    def write_all_csv(self, base_path: str, **kwargs: Any) -> dict[str, str]:
        """Write data to CSV files."""
        from .writers import ResultWriters

        writer = ResultWriters(self)
        return writer.write_all_csv(base_path, **kwargs)
