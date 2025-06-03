"""CSV reader for Transmog input.

This module provides functions for reading CSV data with multiple implementations
optimized for different scenarios and data sizes.
"""

import bz2
import csv
import gzip
import logging
import lzma
import os
from collections.abc import Generator, Iterator
from typing import (
    Any,
    Optional,
    cast,
)

# Try to import PyArrow
try:
    import pyarrow as pa
    import pyarrow.csv as pa_csv

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

# Import Polars (required dependency)
import polars as pl

from transmog.config import settings
from transmog.error import FileError, ParsingError
from transmog.naming.conventions import sanitize_column_names

logger = logging.getLogger(__name__)

# Performance thresholds for adaptive selection
LARGE_FILE_THRESHOLD = 100_000  # rows
VERY_LARGE_FILE_THRESHOLD = 1_000_000  # rows


class CSVImplementation:
    """Enumeration of available CSV implementations."""

    NATIVE = "native"
    POLARS = "polars"
    PYARROW = "pyarrow"


def select_optimal_csv_reader(file_path: str, cast_to_string: bool = False) -> str:
    """Select the optimal CSV reader based on file characteristics.

    Strategy:
    - Small files (<100K rows): Native CSV (fastest for small data)
    - Large files (100K-1M rows): Polars if available, otherwise Native
    - Very large files (>1M rows): Polars for best performance
    - Never use PyArrow for row-oriented dictionary output (performance anti-pattern)

    Args:
        file_path: Path to the CSV file
        cast_to_string: Whether casting to string is needed

    Returns:
        CSVImplementation constant indicating best reader
    """
    try:
        # Quick file size estimation
        file_size = os.path.getsize(file_path)

        # Rough estimation: assume ~100 bytes per row average
        estimated_rows = file_size / 100

        # For very large files, use Polars if available
        if estimated_rows > VERY_LARGE_FILE_THRESHOLD:
            logger.info(
                f"Large file detected ({estimated_rows:.0f} estimated rows), "
                f"using Polars CSV reader"
            )
            return CSVImplementation.POLARS

        # For medium files, use Polars if available, otherwise native
        elif estimated_rows > LARGE_FILE_THRESHOLD:
            logger.info(
                f"Medium file detected ({estimated_rows:.0f} estimated rows), "
                f"using Polars CSV reader"
            )
            return CSVImplementation.POLARS

        # For small files, native CSV is fastest
        else:
            logger.info(
                f"Small file detected ({estimated_rows:.0f} estimated rows), "
                f"using native CSV reader"
            )
            return CSVImplementation.NATIVE

    except Exception as e:
        logger.warning(
            f"Could not estimate file size for {file_path}: {e}, "
            f"defaulting to native CSV"
        )
        return CSVImplementation.NATIVE


def read_csv_file(
    file_path: str,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[list[str]] = None,
    cast_to_string: Optional[bool] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
    date_format: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Read a CSV file and return its contents as a list of dictionaries.

    Args:
        file_path: Path to the CSV file
        delimiter: Delimiter character (default: auto-detect or comma)
        has_header: Whether the file has a header row
        null_values: List of strings to interpret as null values
        cast_to_string: Whether to cast all values to strings
        sanitize_column_names: Whether to sanitize column names
        infer_types: Whether to infer types (PyArrow only)
        skip_rows: Number of rows to skip at the beginning
        quote_char: Character used for quoting fields
        encoding: File encoding (fallback mode only)
        date_format: Optional format string for parsing dates

    Returns:
        List of dictionaries with column names as keys

    Raises:
        FileError: If the file doesn't exist
        ParsingError: If the file cannot be parsed
    """
    if not os.path.exists(file_path):
        raise FileError(f"File not found: {file_path}")

    reader = CSVReader(
        delimiter=delimiter,
        has_header=has_header,
        null_values=null_values,
        sanitize_column_names=sanitize_column_names,
        infer_types=infer_types,
        skip_rows=skip_rows,
        quote_char=quote_char,
        encoding=encoding,
        cast_to_string=cast_to_string,
        date_format=date_format,
    )

    return reader.read_all(file_path)


def read_csv_stream(
    file_path: str, chunk_size: int = 1000, **kwargs: Any
) -> Generator[list[dict[str, Any]], None, None]:
    """Read a CSV file in chunks.

    Args:
        file_path: Path to the CSV file
        chunk_size: Number of rows to read at a time
        **kwargs: Additional arguments passed to CSVReader

    Yields:
        Chunks of records as lists of dictionaries

    Raises:
        FileError: If the file doesn't exist
        ParsingError: If the file cannot be parsed
    """
    if not os.path.exists(file_path):
        raise FileError(f"File not found: {file_path}")

    reader = CSVReader(**kwargs)
    yield from reader.read_in_chunks(file_path, chunk_size)


class CSVReader:
    """Reader for CSV files with auto-selection of implementation.

    Implementation is selected based on available libraries and file characteristics.

    Performance Notes:
    ==================
    PyArrow vs Native CSV Performance:
    - Native CSV is ~20x faster for typical use cases (1K-50K records)
    - PyArrow overhead comes from:
      1. Double file reading when cast_to_string=True (sampling + main read)
      2. Row-by-row conversion from columnar to dict format (.as_py() calls)
      3. Complex type system overhead for simple string processing

    PyArrow is optimized for large-scale analytics (millions of rows) and columnar
    operations, not small-to-medium row-oriented dictionary processing.

    Potential Optimizations:
    - Eliminate double read by pre-defining string schema
    - Use batch conversion instead of row-by-row .as_py() calls
    - Consider using PyArrow only for large files (>100K records)
    """

    def __init__(
        self,
        delimiter: Optional[str] = None,
        has_header: bool = True,
        null_values: Optional[list[str]] = None,
        sanitize_column_names: bool = True,
        infer_types: bool = True,
        skip_rows: int = 0,
        quote_char: Optional[str] = None,
        encoding: str = "utf-8",
        cast_to_string: Optional[bool] = None,
        date_format: Optional[str] = None,
    ):
        """Initialize the CSV reader.

        Args:
            delimiter: Column delimiter
            has_header: Whether file has a header row
            null_values: List of values to interpret as NULL
            sanitize_column_names: Whether to sanitize column names
            infer_types: Whether to infer types from values
            skip_rows: Number of rows to skip
            quote_char: Quote character
            encoding: File encoding
            cast_to_string: Whether to cast all values to strings
            date_format: Optional format string for parsing dates
        """
        # Configuration values from settings or defaults
        self.delimiter = delimiter or settings.get_option("csv_delimiter", ",")
        self.has_header = has_header
        self.null_values = null_values or settings.get_option(
            "csv_null_values", ["", "NULL", "null", "NA", "na", "N/A", "n/a"]
        )
        self.sanitize_column_names = sanitize_column_names
        self.infer_types = infer_types
        self.skip_rows = skip_rows
        self.quote_char = quote_char or settings.get_option("csv_quote_char", '"')
        self.encoding = encoding
        self.cast_to_string = cast_to_string or settings.get_option(
            "cast_to_string", False
        )
        self.date_format = date_format

        # Separator for column name sanitization
        self.separator = settings.get_option("separator", "_")

        # If cast_to_string is True, disable type inference to prevent conversion issues
        if self.cast_to_string:
            self.infer_types = False

    def read_records(self, file_path: str) -> Iterator[dict[str, Any]]:
        """Read records one by one from a CSV file.

        Args:
            file_path: Path to the CSV file

        Returns:
            Iterator yielding records as dictionaries

        Raises:
            FileError: If the file cannot be read
        """
        if not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

        # Check for environment override to force native CSV
        if os.environ.get("TRANSMOG_FORCE_NATIVE_CSV", "").lower() in (
            "true",
            "1",
            "yes",
        ):
            logger.info("TRANSMOG_FORCE_NATIVE_CSV is set, using native CSV reader")
            yield from self._read_records_builtin(file_path)
            return

        # Use adaptive reader selection
        optimal_reader = select_optimal_csv_reader(file_path, self.cast_to_string)

        if optimal_reader == CSVImplementation.POLARS:
            try:
                yield from self._read_records_polars(file_path)
                return
            except Exception as e:
                logger.warning(
                    f"Error reading CSV with Polars: {str(e)}. "
                    f"Falling back to native CSV module."
                )
                yield from self._read_records_builtin(file_path)
                return
        elif optimal_reader == CSVImplementation.PYARROW and PYARROW_AVAILABLE:
            try:
                yield from self._read_records_pyarrow(file_path)
                return
            except Exception as e:
                logger.warning(
                    f"Error reading CSV with PyArrow: {str(e)}. "
                    f"Falling back to native CSV module."
                )
                yield from self._read_records_builtin(file_path)
                return
        else:
            # Use native CSV reader
            yield from self._read_records_builtin(file_path)

    def read_all(self, file_path: str) -> list[dict[str, Any]]:
        """Read all records from a CSV file.

        Args:
            file_path: Path to the CSV file

        Returns:
            List of dictionaries with column names as keys

        Raises:
            FileError: If the file cannot be read
        """
        return list(self.read_records(file_path))

    def read_in_chunks(
        self, file_path: str, chunk_size: int = 1000
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Read a CSV file in chunks.

        Args:
            file_path: Path to the CSV file
            chunk_size: Number of rows to read at a time

        Yields:
            Chunks of records as lists of dictionaries

        Raises:
            FileError: If the file cannot be read
        """
        if not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

        # Check for environment override to force native CSV
        if os.environ.get("TRANSMOG_FORCE_NATIVE_CSV", "").lower() in (
            "true",
            "1",
            "yes",
        ):
            logger.info(
                "TRANSMOG_FORCE_NATIVE_CSV is set, "
                "using native CSV reader for chunked reading"
            )
            yield from self._read_chunks_builtin(file_path, chunk_size)
            return

        # Use adaptive reader selection
        optimal_reader = select_optimal_csv_reader(file_path, self.cast_to_string)

        if optimal_reader == CSVImplementation.POLARS:
            try:
                yield from self._read_chunks_polars(file_path, chunk_size)
                return
            except Exception as e:
                logger.warning(
                    f"Error reading CSV chunks with Polars: {str(e)}. "
                    f"Falling back to native CSV module."
                )
                yield from self._read_chunks_builtin(file_path, chunk_size)
                return
        elif optimal_reader == CSVImplementation.PYARROW and PYARROW_AVAILABLE:
            try:
                yield from self._read_chunks_pyarrow(file_path, chunk_size)
                return
            except Exception as e:
                logger.warning(
                    f"Error reading CSV chunks with PyArrow: {str(e)}. "
                    f"Falling back to native CSV module."
                )
                yield from self._read_chunks_builtin(file_path, chunk_size)
                return
        else:
            # Use native CSV reader
            yield from self._read_chunks_builtin(file_path, chunk_size)

    def _read_records_pyarrow(self, file_path: str) -> Iterator[dict[str, Any]]:
        """Read records using PyArrow.

        Args:
            file_path: Path to the CSV file

        Returns:
            Iterator yielding records as dictionaries
        """
        table = self._read_table_pyarrow(file_path)
        column_names = self._get_column_names(table.column_names)

        # Batch conversion optimization: convert entire columns to Python lists
        column_data = {}
        for i, col_name in enumerate(column_names):
            if i < len(table.column_names):
                # Convert entire column to Python list at once
                column_data[col_name] = table.column(i).to_pylist()

        # Zip columns into row dictionaries
        num_rows = table.num_rows
        for row_idx in range(num_rows):
            record = {}
            for col_name in column_names:
                if col_name in column_data:
                    value = column_data[col_name][row_idx]
                    record[col_name] = self._process_value(value)
            yield record

    def _read_chunks_pyarrow(
        self, file_path: str, chunk_size: int
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Read CSV in chunks using PyArrow.

        Args:
            file_path: Path to the CSV file
            chunk_size: Number of rows per chunk

        Yields:
            Chunks of records as lists of dictionaries
        """
        # Read table in batches using PyArrow's streaming reader
        read_options = pa_csv.ReadOptions(
            skip_rows=self.skip_rows,
            autogenerate_column_names=not self.has_header,
            block_size=max(chunk_size * 512, 32768),  # Reasonable block size
        )

        parse_options = pa_csv.ParseOptions(
            delimiter=self.delimiter,
            quote_char=self.quote_char,
        )

        # Configure conversion options to handle dates as strings when needed
        convert_options = pa_csv.ConvertOptions(
            null_values=self.null_values,
            strings_can_be_null=True,
            # Force all columns to string type if cast_to_string is True
            column_types={} if not self.cast_to_string else None,
            timestamp_parsers=[]
            if self.cast_to_string
            else ([self.date_format] if self.date_format else []),
            check_utf8=True,
        )

        # Force string types if cast_to_string is enabled
        if self.cast_to_string:
            # Use predefined string schema instead of sampling
            convert_options = pa_csv.ConvertOptions(
                column_types=pa.string(),  # Force all columns to string
                auto_dict_encode=False,
                strings_can_be_null=True,
                null_values=self.null_values,
                timestamp_parsers=[],
                check_utf8=True,
            )

        # Stream the file in batches
        with pa_csv.open_csv(
            file_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        ) as reader:
            column_names = None
            chunk = []

            for batch in reader:
                if batch.num_rows == 0:
                    continue

                # Get column names from first batch
                if column_names is None:
                    column_names = self._get_column_names(batch.column_names)

                # Batch conversion: convert entire batch columns to Python lists
                batch_column_data = {}
                for i, col_name in enumerate(column_names):
                    if i < len(batch.column_names):
                        batch_column_data[col_name] = batch.column(i).to_pylist()

                # Process each row in the batch
                for row_idx in range(batch.num_rows):
                    record = {}
                    for col_name in column_names:
                        if col_name in batch_column_data:
                            value = batch_column_data[col_name][row_idx]
                            record[col_name] = self._process_value(value)

                    chunk.append(record)

                    # Yield chunk when it reaches desired size
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []

            # Yield any remaining records
            if chunk:
                yield chunk

    def _read_table_pyarrow(self, file_path: str) -> pa.Table:
        """Read entire CSV as PyArrow table.

        Args:
            file_path: Path to the CSV file

        Returns:
            PyArrow table
        """
        read_options = pa_csv.ReadOptions(
            skip_rows=self.skip_rows,
            autogenerate_column_names=not self.has_header,
        )

        parse_options = pa_csv.ParseOptions(
            delimiter=self.delimiter,
            quote_char=self.quote_char,
        )

        convert_options = pa_csv.ConvertOptions(
            null_values=self.null_values,
            strings_can_be_null=True,
            timestamp_parsers=[]
            if self.cast_to_string
            else ([self.date_format] if self.date_format else []),
            check_utf8=True,
        )

        # Force string types if cast_to_string is enabled
        if self.cast_to_string:
            # Use predefined string schema instead of sampling
            convert_options = pa_csv.ConvertOptions(
                column_types=pa.string(),  # Force all columns to string
                auto_dict_encode=False,
                strings_can_be_null=True,
                null_values=self.null_values,
                timestamp_parsers=[],
                check_utf8=True,
            )

        return pa_csv.read_csv(
            file_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )

    def _read_records_polars(self, file_path: str) -> Iterator[dict[str, Any]]:
        """Read records using Polars.

        Args:
            file_path: Path to the CSV file

        Returns:
            Iterator yielding records as dictionaries
        """
        # Prepare Polars read options
        read_options = {
            "separator": self.delimiter,
            "has_header": self.has_header,
            "skip_rows": self.skip_rows,
            "null_values": self.null_values,
            "quote_char": self.quote_char,
            "try_parse_dates": not self.cast_to_string,
        }

        # When cast_to_string is True, disable all type inference
        if self.cast_to_string:
            # Ensure all data is read as strings by forcing string schema
            read_options.update(
                {
                    "dtypes": None,  # No specific dtype mapping
                    "infer_schema_length": 0,  # Disable schema inference
                    "ignore_errors": True,  # Ignore parsing errors
                }
            )

        # Read the CSV file
        df = pl.read_csv(file_path, **read_options)

        # Cast all columns to string if requested
        if self.cast_to_string:
            df = df.select([pl.col(col).cast(pl.Utf8).alias(col) for col in df.columns])

        # Get column names and sanitize if needed
        column_names = self._get_column_names(df.columns)

        # Rename columns if sanitized
        if column_names != df.columns:
            df = df.rename(dict(zip(df.columns, column_names)))

        # Convert to row-oriented dicts
        for row in df.iter_rows(named=True):
            # Process values according to configuration
            processed_row = {}
            for key, value in row.items():
                processed_row[key] = self._process_value(value)
            yield processed_row

    def _read_chunks_polars(
        self, file_path: str, chunk_size: int
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Read CSV in chunks using Polars.

        Args:
            file_path: Path to the CSV file
            chunk_size: Number of rows per chunk

        Yields:
            Chunks of records as lists of dictionaries
        """
        # Prepare Polars read options
        read_options = {
            "separator": self.delimiter,
            "has_header": self.has_header,
            "skip_rows": self.skip_rows,
            "null_values": self.null_values,
            "quote_char": self.quote_char,
            "try_parse_dates": not self.cast_to_string,
        }

        # When cast_to_string is True, disable all type inference
        if self.cast_to_string:
            # Ensure all data is read as strings by forcing string schema
            read_options.update(
                {
                    "dtypes": None,  # No specific dtype mapping
                    "infer_schema_length": 0,  # Disable schema inference
                    "ignore_errors": True,  # Ignore parsing errors
                }
            )

        # Read the CSV file
        df = pl.read_csv(file_path, **read_options)

        # Cast all columns to string if requested
        if self.cast_to_string:
            df = df.select([pl.col(col).cast(pl.Utf8).alias(col) for col in df.columns])

        # Get column names and sanitize if needed
        column_names = self._get_column_names(df.columns)

        # Rename columns if sanitized
        if column_names != df.columns:
            df = df.rename(dict(zip(df.columns, column_names)))

        # Process in chunks
        total_rows = len(df)
        for i in range(0, total_rows, chunk_size):
            chunk_df = df[i : i + chunk_size]
            chunk = []

            for row in chunk_df.iter_rows(named=True):
                # Process values according to configuration
                processed_row = {}
                for key, value in row.items():
                    processed_row[key] = self._process_value(value)
                chunk.append(processed_row)

            yield chunk

    def _read_records_builtin(self, file_path: str) -> Iterator[dict[str, Any]]:
        """Read records using built-in CSV module.

        Args:
            file_path: Path to the CSV file

        Returns:
            Iterator yielding records as dictionaries
        """
        try:
            file_extension = os.path.splitext(file_path)[1].lower()

            # Get the appropriate opener based on file extension
            file_opener, mode = self._get_file_opener(file_extension)

            # Define file open function with parameters
            def open_file() -> Any:
                return file_opener(file_path, mode=mode, encoding=self.encoding)

            # Handle encoding for text mode
            if "t" in mode:
                with open_file() as file:
                    reader = csv.reader(
                        file, delimiter=self.delimiter, quotechar=self.quote_char
                    )

                    # Skip rows if needed
                    for _ in range(self.skip_rows):
                        next(reader, None)

                    # Read header row if present
                    header_row = self._get_header_row(reader, file)

                    # Process data rows
                    for row in reader:
                        if not row:  # Skip empty rows
                            continue

                        record = self._process_row(row, header_row)
                        if record:  # Only yield non-empty records
                            yield record
            else:
                # Binary mode for compressed files
                with open_file() as file:
                    reader = csv.reader(
                        file, delimiter=self.delimiter, quotechar=self.quote_char
                    )

                    # Skip rows if needed
                    for _ in range(self.skip_rows):
                        next(reader, None)

                    # Read header row if present
                    header_row = self._get_header_row(reader, file)

                    # Process data rows
                    for row in reader:
                        if not row:  # Skip empty rows
                            continue

                        record = self._process_row(row, header_row)
                        if record:  # Only yield non-empty records
                            yield record

        except Exception as e:
            if isinstance(e, (FileError, ParsingError)):
                raise
            raise ParsingError(f"Failed to parse CSV file: {str(e)}") from e

    def _read_chunks_builtin(
        self, file_path: str, chunk_size: int
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Read CSV in chunks using built-in CSV module.

        Args:
            file_path: Path to the CSV file
            chunk_size: Number of rows per chunk

        Yields:
            Chunks of records as lists of dictionaries
        """
        try:
            file_extension = os.path.splitext(file_path)[1].lower()

            # Get the appropriate opener based on file extension
            file_opener, mode = self._get_file_opener(file_extension)

            # Define file open function with parameters
            def open_file() -> Any:
                return file_opener(file_path, mode=mode, encoding=self.encoding)

            # Handle encoding for text mode
            if "t" in mode:
                with open_file() as file:
                    reader = csv.reader(
                        file, delimiter=self.delimiter, quotechar=self.quote_char
                    )

                    # Skip rows if needed
                    for _ in range(self.skip_rows):
                        next(reader, None)

                    # Read header row if present
                    header_row = self._get_header_row(reader, file)

                    chunk = []
                    for row in reader:
                        if not row:  # Skip empty rows
                            continue

                        record = self._process_row(row, header_row)
                        if record:  # Only add non-empty records
                            chunk.append(record)

                        # Yield when chunk is full
                        if len(chunk) >= chunk_size:
                            yield chunk
                            chunk = []

                    # Yield the final chunk if not empty
                    if chunk:
                        yield chunk
            else:
                # Binary mode for compressed files
                with open_file() as file:
                    reader = csv.reader(
                        file, delimiter=self.delimiter, quotechar=self.quote_char
                    )

                    # Skip rows if needed
                    for _ in range(self.skip_rows):
                        next(reader, None)

                    # Read header row if present
                    header_row = self._get_header_row(reader, file)

                    chunk = []
                    for row in reader:
                        if not row:  # Skip empty rows
                            continue

                        record = self._process_row(row, header_row)
                        if record:  # Only add non-empty records
                            chunk.append(record)

                        # Yield when chunk is full
                        if len(chunk) >= chunk_size:
                            yield chunk
                            chunk = []

                    # Yield the final chunk if not empty
                    if chunk:
                        yield chunk

        except Exception as e:
            if isinstance(e, (FileError, ParsingError)):
                raise
            raise ParsingError(f"Failed to parse CSV file in chunks: {str(e)}") from e

    def _get_header_row(self, reader: Any, file: Any) -> list[str]:
        """Get header row, either from file or generated.

        Args:
            reader: CSV reader object
            file: File object for potential rewinding

        Returns:
            List of column names
        """
        if self.has_header:
            try:
                header_row: list[str] = next(reader)
                if self.sanitize_column_names:
                    header_row = sanitize_column_names(
                        header_row, separator=self.separator, sql_safe=True
                    )
                return header_row
            except StopIteration:
                raise ParsingError("CSV file is empty or has no header row") from None
        else:
            # Generate column names if no header
            try:
                row: list[str] = next(reader)
                if row is None:
                    raise ParsingError("CSV file is empty")
                header_row = [f"column_{i + 1}" for i in range(len(row))]
                # Rewind to reprocess first row
                if hasattr(file, "seek"):
                    file.seek(0)
                    for _ in range(self.skip_rows):
                        next(reader, None)
                return header_row
            except StopIteration:
                raise ParsingError("CSV file is empty") from None

    def _process_row(self, row: list[str], header_row: list[str]) -> dict[str, Any]:
        """Process a single CSV row into a record.

        Args:
            row: List of string values from CSV
            header_row: List of column names

        Returns:
            Dictionary record
        """
        # Ensure row length matches header
        if len(row) < len(header_row):
            row.extend([""] * (len(header_row) - len(row)))
        elif len(row) > len(header_row):
            row = row[: len(header_row)]

        # Create record
        record = {}
        for i, value in enumerate(row):
            if i < len(header_row):  # Safety check
                column_name = header_row[i]
                record[column_name] = self._process_value(value)

        return record

    def _process_value(self, value: Any) -> Any:
        """Process a single value according to configuration.

        Args:
            value: Raw value to process

        Returns:
            Processed value
        """
        # Handle None values
        if value is None:
            return None

        # Convert to string first if needed
        str_value = str(value) if value is not None else ""

        # Process null values
        if str_value in self.null_values:
            return None

        # Cast to string if required
        if self.cast_to_string:
            return str_value

        # Infer types if requested and value is not empty
        if self.infer_types and str_value:
            return self._infer_type(str_value)

        return str_value

    def _infer_type(self, value: str) -> Any:
        """Infer the type of a value.

        Args:
            value: String value to infer type from

        Returns:
            Value converted to its inferred type
        """
        # Try to convert to integer
        try:
            if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
                return int(value)
        except (ValueError, AttributeError):
            pass

        # Try to convert to float
        try:
            return float(value)
        except (ValueError, TypeError):
            pass

        # Try to convert to boolean - only for explicit boolean values
        lower_value = value.lower()
        if lower_value in ("true", "yes", "1"):
            return True
        elif lower_value in ("false", "no", "0"):
            return False

        # Return as string if no other type matches
        return value

    def _get_column_names(self, raw_names: list[str]) -> list[str]:
        """Get processed column names.

        Args:
            raw_names: Raw column names from file

        Returns:
            Processed column names
        """
        if not self.has_header:
            return [f"column_{i + 1}" for i in range(len(raw_names))]

        if self.sanitize_column_names:
            return cast(
                list[str],
                sanitize_column_names(
                    raw_names, separator=self.separator, sql_safe=True
                ),
            )

        return raw_names

    def _get_file_opener(self, file_extension: str) -> tuple[Any, str]:
        """Get the appropriate file opener and mode based on file extension.

        Args:
            file_extension: File extension including the dot

        Returns:
            Tuple of (file_opener, mode)
        """
        opener: Any
        mode: str

        if file_extension == ".gz":
            opener, mode = gzip.open, "rt"
        elif file_extension == ".bz2":
            opener, mode = bz2.open, "rt"
        elif file_extension in (".xz", ".lzma"):
            opener, mode = lzma.open, "rt"
        else:
            opener, mode = open, "r"

        return (opener, mode)


def normalize_column_names(column_names: list[str], separator: str = "_") -> list[str]:
    """Normalize column names by sanitizing and deduplicating.

    Args:
        column_names: List of column names to normalize
        separator: Separator to use for sanitization

    Returns:
        List of normalized column names
    """
    # Sanitize column names
    sanitized = sanitize_column_names(column_names, separator=separator, sql_safe=True)

    # Handle duplicate column names by adding a suffix
    result: list[str] = []
    seen: dict[str, int] = {}

    for name in sanitized:
        if name in seen:
            seen[name] += 1
            result.append(f"{name}{separator}{seen[name]}")
        else:
            seen[name] = 0
            result.append(name)

    return result


def detect_delimiter(
    file_path: str, sample_size: int = 4096, possible_delimiters: str = ",;\t|"
) -> str:
    """Detect the delimiter used in a CSV file.

    Args:
        file_path: Path to the CSV file
        sample_size: Number of bytes to sample for detection
        possible_delimiters: String of possible delimiters to check

    Returns:
        Most likely delimiter character
    """
    if not os.path.exists(file_path):
        raise FileError(f"File not found: {file_path}", file_path=file_path)

    # Get file opener based on extension
    file_extension = os.path.splitext(file_path)[1].lower()

    # Define opener and mode based on file extension
    opener: Any
    mode: str

    if file_extension == ".gz":
        opener, mode = gzip.open, "rt"
    elif file_extension == ".bz2":
        opener, mode = bz2.open, "rt"
    elif file_extension in (".xz", ".lzma"):
        opener, mode = lzma.open, "rt"
    else:
        opener, mode = open, "r"

    try:
        with opener(file_path, mode) as f:
            sample = f.read(sample_size)

        # Count occurrences for each potential delimiter
        counts = {}
        for delim in possible_delimiters:
            if isinstance(sample, str):
                counts[delim] = sample.count(delim)
            else:
                counts[delim] = sample.count(bytes(delim, "utf-8"))

        # Return the delimiter with the highest count
        most_common = max(counts.items(), key=lambda x: x[1])

        # Default to comma if no delimiter found with significant count
        if most_common[1] <= 5:
            return ","

        return most_common[0]
    except Exception as e:
        logger.warning(f"Failed to detect delimiter: {e}")
        return ","
