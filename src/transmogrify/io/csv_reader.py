"""
CSV reader for Transmogrify input.

This module provides functions for reading CSV data with PyArrow and built-in CSV module
implementations, with support for type inference, null value handling, and column sanitization.
"""

import csv
import os
import io
import gzip
import bz2
import lzma
import logging
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    Iterator,
    Generator,
    Set,
    Tuple,
    Iterable,
)

# Try to import PyArrow
try:
    import pyarrow as pa
    import pyarrow.csv as pa_csv
    import pyarrow.compute as pc

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from ..naming.conventions import sanitize_name, sanitize_column_names
from ..exceptions import FileError, ParsingError, ProcessingError
from ..config.settings import settings

logger = logging.getLogger(__name__)


def read_csv_file(
    file_path: str,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[List[str]] = None,
    cast_to_string: Optional[bool] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
) -> List[Dict[str, Any]]:
    """
    Read a CSV file and return its contents as a list of dictionaries.

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
    )

    return reader.read_all(file_path)


def read_csv_stream(
    file_path: str, chunk_size: int = 1000, **kwargs
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Read a CSV file in chunks.

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
    """Reader for CSV files with auto-selection of implementation based on available libraries."""

    def __init__(
        self,
        delimiter: Optional[str] = None,
        has_header: bool = True,
        null_values: Optional[List[str]] = None,
        sanitize_column_names: bool = True,
        infer_types: bool = True,
        skip_rows: int = 0,
        quote_char: Optional[str] = None,
        encoding: str = "utf-8",
        cast_to_string: Optional[bool] = None,
    ):
        """
        Initialize the CSV reader.

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
        """
        # Get configuration values or use defaults
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

        # Use the same separator as the rest of the package for column name sanitization
        self.separator = settings.get_option("separator", "_")

        # Flag to track if PyArrow is used
        self._using_pyarrow = PYARROW_AVAILABLE

    def read_records(
        self, file_path: str, using_fallback: bool = False
    ) -> Iterator[Dict[str, Any]]:
        """
        Read records one by one from a CSV file.

        Args:
            file_path: Path to the CSV file
            using_fallback: Flag to prevent circular dependencies in fallback scenarios

        Returns:
            Iterator yielding records as dictionaries

        Raises:
            FileError: If the file cannot be read
        """
        if not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

        # Try PyArrow first if available and not already in fallback mode
        if PYARROW_AVAILABLE and not using_fallback:
            try:
                # PyArrow doesn't have a streaming API that yields one record at a time,
                # so we'll read in chunks and yield each record
                pyarrow_success = False
                for chunk in self.read_in_chunks(file_path, chunk_size=1000):
                    yield from chunk
                    pyarrow_success = True

                # If we successfully processed any chunks with PyArrow, we're done
                if pyarrow_success:
                    return
            except Exception as e:
                logger.warning(
                    f"Error reading CSV with PyArrow: {str(e)}. Falling back to built-in CSV module."
                )
                self._using_pyarrow = False
                # Continue to built-in reader below

        # If we get here, either PyArrow is not available, we're in fallback mode,
        # or PyArrow failed completely (pyarrow_success remained False)

        # Fallback to built-in CSV module
        try:
            # Determine file opening mode based on extension
            file_extension = os.path.splitext(file_path)[1].lower()
            open_func, mode = self._get_file_opener(file_extension)

            with open_func(file_path, mode) as file:
                reader = csv.reader(
                    file, delimiter=self.delimiter, quotechar=self.quote_char
                )

                # Skip rows if needed
                for _ in range(self.skip_rows):
                    next(reader, None)

                # Read header row if present
                if self.has_header:
                    try:
                        header_row = next(reader)
                        # Sanitize column names if requested
                        if self.sanitize_column_names:
                            header_row = sanitize_column_names(
                                header_row, separator=self.separator, sql_safe=True
                            )
                    except StopIteration:
                        raise ParsingError("CSV file is empty or has no header row")
                else:
                    # Generate column names if no header
                    row = next(reader, None)
                    if row is None:
                        raise ParsingError("CSV file is empty")
                    header_row = [f"column_{i + 1}" for i in range(len(row))]
                    # Rewind to reprocess first row
                    file.seek(0)
                    for _ in range(self.skip_rows):
                        next(reader, None)

                # Process data rows
                for row in reader:
                    # Skip empty rows
                    if not row:
                        continue

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

                            # Process null values
                            if value in self.null_values:
                                processed_value = None
                            else:
                                # Infer types if requested
                                if (
                                    self.infer_types
                                    and value
                                    and not self.cast_to_string
                                ):
                                    processed_value = self._infer_type(value)
                                else:
                                    processed_value = value

                            # Cast to string if required
                            if self.cast_to_string and processed_value is not None:
                                processed_value = str(processed_value)

                            record[column_name] = processed_value

                    yield record

        except Exception as e:
            if isinstance(e, (FileError, ParsingError)):
                raise
            raise ParsingError(f"Failed to parse CSV file: {str(e)}")

    def read_all(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Read all records from a CSV file.

        Args:
            file_path: Path to the CSV file

        Returns:
            List of dictionaries with column names as keys

        Raises:
            FileError: If the file cannot be read
        """
        return list(self.read_records(file_path, using_fallback=False))

    def read_in_chunks(
        self, file_path: str, chunk_size: int = 1000
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Read a CSV file in chunks.

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

        # Try PyArrow first if available
        if PYARROW_AVAILABLE:
            try:
                # Collect all chunks from PyArrow in a list to detect complete failure
                # This avoids yielding partial results that lead to double counting
                pyarrow_success = False
                for chunk in self._read_chunks_pyarrow(file_path, chunk_size):
                    yield chunk
                    pyarrow_success = True

                # If we successfully processed any chunks with PyArrow, we're done
                if pyarrow_success:
                    return
            except Exception as e:
                logger.warning(
                    f"Error reading CSV chunks with PyArrow: {str(e)}. Falling back to built-in CSV module."
                )
                self._using_pyarrow = False
                # Continue to built-in reader below

        # If we get here, either PyArrow is not available or PyArrow failed completely

        # Fallback to built-in CSV module - use direct implementation instead of calling read_records
        # to prevent circular dependencies
        try:
            file_extension = os.path.splitext(file_path)[1].lower()
            open_func, mode = self._get_file_opener(file_extension)

            with open_func(file_path, mode) as file:
                reader = csv.reader(
                    file, delimiter=self.delimiter, quotechar=self.quote_char
                )

                # Skip rows if needed
                for _ in range(self.skip_rows):
                    next(reader, None)

                # Read header row if present
                if self.has_header:
                    try:
                        header_row = next(reader)
                        # Sanitize column names if requested
                        if self.sanitize_column_names:
                            header_row = sanitize_column_names(
                                header_row, separator=self.separator, sql_safe=True
                            )
                    except StopIteration:
                        raise ParsingError("CSV file is empty or has no header row")
                else:
                    # Generate column names if no header
                    row = next(reader, None)
                    if row is None:
                        raise ParsingError("CSV file is empty")
                    header_row = [f"column_{i + 1}" for i in range(len(row))]
                    # Rewind to reprocess first row
                    file.seek(0)
                    for _ in range(self.skip_rows):
                        next(reader, None)

                chunk = []
                count = 0

                # Process data rows
                for row in reader:
                    # Skip empty rows
                    if not row:
                        continue

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

                            # Process null values
                            if value in self.null_values:
                                processed_value = None
                            else:
                                # Infer types if requested
                                if (
                                    self.infer_types
                                    and value
                                    and not self.cast_to_string
                                ):
                                    processed_value = self._infer_type(value)
                                else:
                                    processed_value = value

                            # Cast to string if required
                            if self.cast_to_string and processed_value is not None:
                                processed_value = str(processed_value)

                            record[column_name] = processed_value

                    chunk.append(record)
                    count += 1

                    # Yield when chunk is full
                    if count >= chunk_size:
                        yield chunk
                        chunk = []
                        count = 0

                # Yield the final chunk if not empty
                if chunk:
                    yield chunk

        except Exception as e:
            if isinstance(e, (FileError, ParsingError)):
                raise
            raise ParsingError(f"Failed to parse CSV file in chunks: {str(e)}")

    def _read_chunks_pyarrow(
        self, file_path: str, chunk_size: int
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Read CSV in chunks using PyArrow.

        Args:
            file_path: Path to the CSV file
            chunk_size: Number of rows per chunk

        Yields:
            Chunks of records as lists of dictionaries

        Raises:
            ParsingError: If PyArrow fails to parse the CSV file
        """
        # Get file size for sanity check
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            logger.warning(f"File {file_path} is empty")
            raise ParsingError("CSV file is empty")

        # For very large files, estimate the number of lines instead of counting exactly
        # This avoids memory issues and improves performance
        estimated_records = 0
        if file_size > 10 * 1024 * 1024:  # If file is larger than 10MB
            try:
                # Sample the first 1000 lines to estimate average line length
                sample_lines = 1000
                lines_read = 0
                total_bytes = 0

                with open(file_path, "r", encoding=self.encoding) as f:
                    for _ in range(sample_lines):
                        line = f.readline()
                        if not line:
                            break
                        lines_read += 1
                        total_bytes += len(line.encode(self.encoding))

                if lines_read > 0:
                    avg_line_size = total_bytes / lines_read
                    estimated_records = int(file_size / avg_line_size)
                    if self.has_header:
                        estimated_records = max(0, estimated_records - 1)

                    logger.debug(
                        f"Estimated {estimated_records} records in large file (avg line size: {avg_line_size:.1f} bytes)"
                    )
                else:
                    estimated_records = 0
            except Exception as e:
                logger.warning(f"Error estimating lines in file: {str(e)}")
                # Fall back to a reasonable default based on file size and chunk size
                estimated_records = max(1000, file_size // 100)  # Rough estimate
        else:
            # For smaller files, count lines exactly
            try:
                total_lines = 0
                with open(file_path, "r", encoding=self.encoding) as f:
                    for _ in f:
                        total_lines += 1

                # Account for header row if present
                estimated_records = total_lines - (1 if self.has_header else 0)
            except Exception as e:
                logger.warning(f"Error counting lines in file: {str(e)}")
                raise ParsingError(f"Could not read file: {str(e)}")

        # Safety check
        estimated_records = max(0, estimated_records)  # Ensure non-negative
        if estimated_records == 0:
            logger.warning(f"No data records in file {file_path}")
            return

        logger.debug(
            f"Reading CSV file: {file_path}, size: {file_size} bytes, expecting ~{estimated_records} records"
        )

        # Set up PyArrow read options
        read_options = pa_csv.ReadOptions(
            skip_rows=self.skip_rows,
            autogenerate_column_names=not self.has_header,
            # Use a reasonable block size based on file size - don't make it too large
            block_size=min(chunk_size * 1024, file_size + 1024),
        )

        parse_options = pa_csv.ParseOptions(
            delimiter=self.delimiter,
            quote_char=self.quote_char,
        )

        convert_options = pa_csv.ConvertOptions(
            null_values=self.null_values,
            strings_can_be_null=True,
            auto_dict_encode=True,
        )

        # Create a reader for the file
        record_count = 0
        records_yielded = 0
        batch_count = 0
        max_batches = max(
            100, (estimated_records // chunk_size) + 10
        )  # Reasonable upper limit

        # Collect all chunks in this list, and only yield them if the entire process succeeds
        all_chunks = []
        success = False

        try:
            with pa_csv.open_csv(
                file_path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
            ) as reader:
                # Process each batch
                while True:
                    batch_count += 1

                    # Hard limit on number of batches to prevent infinite loops
                    if batch_count > max_batches:
                        logger.warning(
                            f"Reached maximum batch count ({max_batches}), stopping PyArrow processing"
                        )
                        raise ParsingError(
                            f"Too many batches ({max_batches}) when processing CSV"
                        )

                    try:
                        batch = reader.read_next_batch()
                        if batch is None:
                            break

                        # Add safety check for empty batches that might cause infinite loops
                        if batch.num_rows == 0:
                            logger.warning(
                                "Empty batch detected, stopping PyArrow processing"
                            )
                            break

                        # Convert batch to table
                        table = pa.Table.from_batches([batch])

                        # Safety check - don't process more records than expected
                        if record_count + table.num_rows > estimated_records * 1.5:
                            logger.warning(
                                f"Record count exceeds expected by significant margin: {record_count + table.num_rows} > {estimated_records * 1.5}"
                            )
                            # Only take the number of rows we need
                            remaining = max(
                                0, int(estimated_records * 1.5) - record_count
                            )
                            if remaining <= 0:
                                break
                            # Slice the table to only get the remaining rows
                            if remaining < table.num_rows:
                                table = table.slice(0, remaining)

                        # Process column names
                        column_names = list(table.column_names)

                        # If no header, replace PyArrow's default 'f0', 'f1', etc. with 'column_1', 'column_2', etc.
                        if not self.has_header:
                            column_names = [
                                f"column_{i + 1}" for i in range(len(column_names))
                            ]
                        elif self.sanitize_column_names:
                            column_names = sanitize_column_names(
                                column_names, separator=self.separator, sql_safe=True
                            )

                        # Convert batch to list of dictionaries
                        records = []
                        for row_idx in range(table.num_rows):
                            record = {}
                            for col_idx, col_name in enumerate(column_names):
                                if col_idx < len(table.column_names):  # Safety check
                                    value = table.column(col_idx)[row_idx].as_py()

                                    # Cast to string if required
                                    if self.cast_to_string and value is not None:
                                        value = str(value)
                                    elif (
                                        not self.infer_types
                                        and value is not None
                                        and not self.cast_to_string
                                    ):
                                        # If not inferring types or casting to string, convert to string and then infer
                                        value = self._infer_type(str(value))

                                    record[col_name] = value
                            records.append(record)

                        # Update record count
                        record_count += len(records)

                        # Add chunk to the list instead of yielding immediately
                        if records:
                            current_chunk = []
                            for record in records:
                                current_chunk.append(record)
                                if len(current_chunk) >= chunk_size:
                                    all_chunks.append(current_chunk)
                                    current_chunk = []

                            # Add any remaining records to a final chunk
                            if current_chunk:
                                all_chunks.append(current_chunk)

                    except Exception as e:
                        logger.warning(
                            f"Error processing batch {batch_count}: {str(e)}"
                        )
                        # An error in a batch means we should abort PyArrow processing
                        raise

                # If we got here without exceptions, PyArrow processing succeeded
                success = True

            # Only yield results if the entire PyArrow processing succeeded
            if success:
                for chunk in all_chunks:
                    yield chunk
            else:
                # PyArrow processing failed, raise an error to trigger fallback
                raise ParsingError("PyArrow CSV reading failed")

        except Exception as e:
            error_msg = f"Error reading CSV with PyArrow: {str(e)}"
            logger.warning(error_msg)
            # Clear PyArrow flag and raise an exception to trigger fallback
            self._using_pyarrow = False
            raise ParsingError(f"PyArrow CSV reading failed: {str(e)}")

    def _infer_type(self, value: str) -> Any:
        """
        Infer the type of a value.

        Args:
            value: String value to infer type from

        Returns:
            Value converted to its inferred type
        """
        # Try to convert to integer
        try:
            if value.isdigit():
                return int(value)
        except (ValueError, AttributeError):
            pass

        # Try to convert to float
        try:
            return float(value)
        except (ValueError, TypeError):
            pass

        # Try to convert to boolean
        lower_value = value.lower()
        if lower_value in ("true", "yes", "1"):
            return True
        elif lower_value in ("false", "no", "0"):
            return False

        # Return as string if no other type matches
        return value

    def _get_file_opener(self, file_extension: str) -> Tuple[Any, str]:
        """
        Get the appropriate file opener and mode based on file extension.

        Args:
            file_extension: File extension including the dot

        Returns:
            Tuple of (file_opener, mode)
        """
        if file_extension == ".gz":
            return gzip.open, "rt"
        elif file_extension == ".bz2":
            return bz2.open, "rt"
        elif file_extension in (".xz", ".lzma"):
            return lzma.open, "rt"
        else:
            return open, f"r"
