"""Data format converters for ProcessingResult.

Contains conversion methods for transforming result data into various formats
like JSON, CSV, Parquet, and PyArrow tables.
"""

import io
import json
import logging
from typing import TYPE_CHECKING, Any, Optional, cast

from transmog.error.exceptions import MissingDependencyError, OutputError

from .utils import _check_orjson_available, _check_pyarrow_available, _get_cache_key

if TYPE_CHECKING:
    from .core import ProcessingResult

logger = logging.getLogger(__name__)

# Global cache for converted data
_conversion_cache: dict[tuple[int, str, str], Any] = {}


class ResultConverters:
    """Handles conversion of ProcessingResult data to various formats."""

    def __init__(self, result: "ProcessingResult"):
        """Initialize with a ProcessingResult instance.

        Args:
            result: ProcessingResult instance to convert
        """
        self.result = result

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
            self.result.to_dict(), "json_bytes", indent=indent, **kwargs
        )
        if cache_key in _conversion_cache:
            return cast(dict[str, bytes], _conversion_cache[cache_key])

        # Try orjson first if available
        if _check_orjson_available():
            try:
                result = self._to_json_bytes_orjson(indent, **kwargs)

                # Cache the result if using eager mode
                if self.result.conversion_mode.value == "eager":
                    _conversion_cache[cache_key] = result

                return result
            except Exception as e:
                logger.debug(f"orjson conversion failed, falling back to stdlib: {e}")
                # Fall back to stdlib if orjson fails

        # Use Python's standard library json module
        result = self._to_json_bytes_stdlib(indent, **kwargs)

        # Cache the result if using eager mode
        if self.result.conversion_mode.value == "eager":
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
            tables = self.result.to_json_objects()
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
        tables = self.result.to_json_objects()
        result: dict[str, bytes] = {}

        # Convert each table
        for table_name, records in tables.items():
            json_str = json.dumps(records, indent=indent, **kwargs)
            result[table_name] = json_str.encode("utf-8")

        return result

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
            self.result.to_dict(), "csv_bytes", include_header=include_header, **kwargs
        )
        if cache_key in _conversion_cache:
            return cast(dict[str, bytes], _conversion_cache[cache_key])

        # Try PyArrow first if available
        if _check_pyarrow_available():
            try:
                result = self._to_csv_bytes_pyarrow(include_header, **kwargs)

                # Cache the result if using eager mode
                if self.result.conversion_mode.value == "eager":
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
        if self.result.conversion_mode.value == "eager":
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
            arrow_tables = self.result.to_pyarrow_tables()
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
        tables = {"main": self.result.main_table, **self.result.child_tables}

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

    def to_parquet_bytes(
        self, compression: str = "snappy", **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert all tables to Parquet bytes.

        Args:
            compression: Compression algorithm to use
            **kwargs: Additional Parquet options

        Returns:
            Dictionary of table names to Parquet bytes

        Raises:
            MissingDependencyError: If PyArrow is not available
            OutputError: If conversion fails
        """
        if not _check_pyarrow_available():
            raise MissingDependencyError(
                "PyArrow is required for Parquet conversion",
                package="pyarrow",
                feature="parquet",
            )

        # Check cache first
        cache_key = _get_cache_key(
            self.result.to_dict(), "parquet_bytes", compression=compression, **kwargs
        )
        if cache_key in _conversion_cache:
            return cast(dict[str, bytes], _conversion_cache[cache_key])

        try:
            import pyarrow.parquet as pq

            # Convert to PyArrow tables first
            arrow_tables = self.result.to_pyarrow_tables()
            result: dict[str, bytes] = {}

            # Process each table
            for table_name, table in arrow_tables.items():
                # Create in-memory buffer
                buffer = io.BytesIO()

                # Write to Parquet format
                pq.write_table(table, buffer, compression=compression, **kwargs)

                # Get the bytes from the buffer
                buffer.seek(0)
                result[table_name] = buffer.getvalue()

            # Cache the result if using eager mode
            if self.result.conversion_mode.value == "eager":
                _conversion_cache[cache_key] = result

            return result
        except Exception as e:
            raise OutputError(
                f"Failed to convert to Parquet: {e}", output_format="parquet"
            ) from e
