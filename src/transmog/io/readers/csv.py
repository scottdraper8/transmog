"""CSV reader for Transmog input.

This module provides functions for reading CSV data with PyArrow and built-in CSV module
implementations, with support for type inference, null value handling, and column
    sanitization.
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
)

# Try to import PyArrow
try:
    import pyarrow as pa
    import pyarrow.csv as pa_csv

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from transmog.config import settings
from transmog.error import FileError, ParsingError
from transmog.naming.conventions import sanitize_column_names

logger = logging.getLogger(__name__)


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
    """Reader for CSV files with auto-selection of implementation based on.

    available libraries.
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

        # Separator for column name sanitization
        self.separator = settings.get_option("separator", "_")

        # Flag for PyArrow usage
        self._using_pyarrow = PYARROW_AVAILABLE

    def read_records(
        self, file_path: str, using_fallback: bool = False
    ) -> Iterator[dict[str, Any]]:
        """Read records one by one from a CSV file.

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

        # Try PyArrow first if available and not in fallback mode
        if PYARROW_AVAILABLE and not using_fallback:
            try:
                # Read in chunks and yield each record
                pyarrow_success = False
                for chunk in self.read_in_chunks(file_path, chunk_size=1000):
                    yield from chunk
                    pyarrow_success = True

                if pyarrow_success:
                    return
            except Exception as e:
                logger.warning(
                    f"Error reading CSV with PyArrow: {str(e)}. "
                    f"Falling back to built-in CSV module."
                )
                self._using_pyarrow = False

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
                        raise ParsingError(
                            "CSV file is empty or has no header row"
                        ) from None
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
            raise ParsingError(f"Failed to parse CSV file: {str(e)}") from e

    def read_all(self, file_path: str) -> list[dict[str, Any]]:
        """Read all records from a CSV file.

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

        # Try PyArrow first if available
        if PYARROW_AVAILABLE:
            try:
                # Collect all chunks to detect complete failure
                pyarrow_success = False
                for chunk in self._read_chunks_pyarrow(file_path, chunk_size):
                    yield chunk
                    pyarrow_success = True

                if pyarrow_success:
                    return
            except Exception as e:
                logger.warning(
                    f"Error reading CSV chunks with PyArrow: {str(e)}. "
                    f"Falling back to built-in CSV module."
                )
                self._using_pyarrow = False

        # Fallback to built-in CSV module
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
                        raise ParsingError(
                            "CSV file is empty or has no header row"
                        ) from None
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
            raise ParsingError(f"Failed to parse CSV file in chunks: {str(e)}") from e

    def _read_chunks_pyarrow(
        self, file_path: str, chunk_size: int
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Read CSV in chunks using PyArrow.

        Args:
            file_path: Path to the CSV file
            chunk_size: Number of rows per chunk

        Yields:
            Chunks of records as lists of dictionaries

        Raises:
            ParsingError: If PyArrow fails to parse the CSV file
        """
        # Check file size
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            logger.warning(f"File {file_path} is empty")
            raise ParsingError("CSV file is empty")

        # Estimate record count for large files
        estimated_records = 0
        if file_size > 10 * 1024 * 1024:  # If file is larger than 10MB
            try:
                # Sample first 1000 lines to estimate average line length
                sample_lines = 1000
                lines_read = 0
                total_bytes = 0

                with open(file_path, encoding=self.encoding) as f:
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
                        f"Estimated {estimated_records} records in large file "
                        f"(avg line size: {avg_line_size:.1f} bytes)"
                    )
                else:
                    estimated_records = 0
            except Exception as e:
                logger.warning(f"Error estimating lines in file: {str(e)}")
                # Estimate based on file size and chunk size
                estimated_records = max(1000, file_size // 100)
        else:
            # Count lines in smaller files
            try:
                total_lines = 0
                with open(file_path, encoding=self.encoding) as f:
                    for _ in f:
                        total_lines += 1

                estimated_records = total_lines - (1 if self.has_header else 0)
            except Exception as e:
                logger.warning(f"Error counting lines in file: {str(e)}")
                raise ParsingError(f"Could not read file: {str(e)}") from e

        # Ensure non-negative record count
        estimated_records = max(0, estimated_records)
        if estimated_records == 0:
            logger.warning(f"No data records in file {file_path}")
            return

        logger.debug(
            f"Reading CSV file: {file_path}, size: {file_size} bytes, "
            f"expecting ~{estimated_records} records"
        )

        # Configure PyArrow options
        read_options = pa_csv.ReadOptions(
            skip_rows=self.skip_rows,
            autogenerate_column_names=not self.has_header,
            # Reasonable block size based on file size
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

        # Setup batch processing
        record_count = 0
        batch_count = 0
        max_batches = max(
            100, (estimated_records // chunk_size) + 10
        )  # Reasonable upper limit

        # Collect chunks for processing
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

                    # Prevent infinite loops with batch limit
                    if batch_count > max_batches:
                        logger.warning(
                            f"Reached maximum batch count ({max_batches}), "
                            f"stopping PyArrow processing"
                        )
                        raise ParsingError(
                            f"Too many batches ({max_batches}) when processing CSV"
                        )

                    try:
                        batch = reader.read_next_batch()
                        if batch is None:
                            break

                        # Check for empty batches
                        if batch.num_rows == 0:
                            logger.warning(
                                "Empty batch detected, stopping PyArrow processing"
                            )
                            break

                        # Convert batch to table
                        table = pa.Table.from_batches([batch])

                        # Limit records to prevent processing excessive data
                        if record_count + table.num_rows > estimated_records * 1.5:
                            total_records = record_count + table.num_rows
                            expected_limit = estimated_records * 1.5
                            logger.warning(
                                f"Record count exceeds expected by significant margin: "
                                f"{total_records} > {expected_limit}"
                            )
                            # Take only needed rows
                            remaining = max(
                                0, int(estimated_records * 1.5) - record_count
                            )
                            if remaining <= 0:
                                break
                            # Slice table to get remaining rows
                            if remaining < table.num_rows:
                                table = table.slice(0, remaining)

                        # Process column names
                        column_names = list(table.column_names)

                        # Handle column naming based on settings
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
                                        # Convert to string then infer type
                                        value = self._infer_type(str(value))

                                    record[col_name] = value
                            records.append(record)

                        # Update record count
                        record_count += len(records)

                        # Process records into chunks
                        if records:
                            current_chunk = []
                            for record in records:
                                current_chunk.append(record)
                                if len(current_chunk) >= chunk_size:
                                    all_chunks.append(current_chunk)
                                    current_chunk = []

                            # Add remaining records
                            if current_chunk:
                                all_chunks.append(current_chunk)

                    except Exception as e:
                        logger.warning(
                            f"Error processing batch {batch_count}: {str(e)}"
                        )
                        # Abort PyArrow processing on batch error
                        raise

                # Mark success if completed without exceptions
                success = True

            # Yield results only if PyArrow processing succeeded
            if success:
                yield from all_chunks
            else:
                # Trigger fallback on failure
                raise ParsingError("PyArrow CSV reading failed")

        except Exception as e:
            error_msg = f"Error reading CSV with PyArrow: {str(e)}"
            logger.warning(error_msg)
            # Clear PyArrow flag and trigger fallback
            self._using_pyarrow = False
            raise ParsingError(f"PyArrow CSV reading failed: {str(e)}") from e

    def _infer_type(self, value: str) -> Any:
        """Infer the type of a value.

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

        # Try to convert to boolean - only for non-numeric looking values
        lower_value = value.lower()
        if not value.isdigit() and lower_value not in ("0", "1"):
            if lower_value in ("true", "yes"):
                return True
            elif lower_value in ("false", "no"):
                return False

        # Return as string if no other type matches
        return value

    def _get_file_opener(self, file_extension: str) -> tuple[Any, str]:
        """Get the appropriate file opener and mode based on file extension.

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
            return open, "r"


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
    opener, mode = (
        _get_file_opener(file_path)
        if hasattr(CSVReader, "_get_file_opener")
        else (open, "rt")
    )

    try:
        with opener(file_path, mode) as f:
            sample = f.read(sample_size)

        # Count occurrences of each delimiter
        counts = {delim: sample.count(delim) for delim in possible_delimiters}

        # Return the delimiter with the highest count
        most_common = max(counts.items(), key=lambda x: x[1])

        # Default to comma if no delimiter found with significant count
        if most_common[1] <= 5:
            return ","

        return most_common[0]
    except Exception as e:
        logger.warning(f"Failed to detect delimiter: {e}")
        return ","


def _get_file_opener(file_path: str) -> tuple[Any, str]:
    """Get the appropriate file opener and mode based on file extension.

    Args:
        file_path: Path to the file

    Returns:
        Tuple of (opener_function, mode)
    """
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()

    if ext == ".gz":
        return gzip.open, "rt"
    elif ext == ".bz2":
        return bz2.open, "rt"
    elif ext in (".xz", ".lzma"):
        return lzma.open, "rt"
    else:
        return open, "r"
