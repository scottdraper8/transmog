"""Data iteration functionality for Transmog package.

This module contains functions for creating iterators over different data sources
such as JSON files, JSONL files, CSV files, and in-memory data structures.
"""

import json
import os
from collections.abc import Iterator
from typing import (
    Any,
    Optional,
    Union,
)

from ..error import (
    FileError,
    ParsingError,
    ProcessingError,
    ValidationError,
    get_recovery_strategy,
    logger,
)


class DataIteratorUtils:
    """Utility class for common data iteration patterns.

    Consolidates repetitive file reading patterns, iterator creation logic,
    and error handling patterns used across different data format iterators.
    """

    @staticmethod
    def validate_file_exists(file_path: str) -> None:
        """Validate that a file exists.

        Args:
            file_path: Path to the file

        Raises:
            FileError: If file doesn't exist
        """
        if not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

    @staticmethod
    def get_json_parser() -> tuple[Any, tuple[type[Exception], ...]]:
        """Get the best available JSON parser.

        Returns:
            Tuple of (parser_module, decode_error_types)
        """
        try:
            import orjson

            return orjson, (json.JSONDecodeError,)
        except ImportError:
            return json, (json.JSONDecodeError,)

    @staticmethod
    def handle_json_error(
        error: Exception,
        processor: Any,
        context: str,
        line_number: Optional[int] = None,
    ) -> bool:
        """Handle JSON parsing errors with recovery strategy.

        Args:
            error: The JSON parsing error
            processor: Processor instance for recovery strategy
            context: Context description for error
            line_number: Optional line number for error

        Returns:
            True if error was handled and processing should continue,
            False if error should be re-raised
        """
        if line_number is not None:
            error_msg = f"Invalid JSON on line {line_number}: {str(error)}"
            entity_name = f"line_{line_number}"
        else:
            error_msg = f"Invalid JSON in {context}: {str(error)}"
            entity_name = context

        logger.warning(error_msg)

        # Handle error based on recovery strategy
        strategy = get_recovery_strategy(
            processor.config.error_handling.recovery_strategy
        )
        try:
            # Attempt recovery
            strategy.recover(
                error,
                entity_name=entity_name,
                entity_type="JSON data",
                source=context,
            )
            # Recovery successful, continue processing
            return True
        except Exception:
            # Recovery failed, re-raise original error
            return False

    @staticmethod
    def read_file_with_encoding(
        file_path: str, encoding: str = "utf-8", mode: str = "r"
    ) -> Any:
        """Open a file with proper encoding handling.

        Args:
            file_path: Path to the file
            encoding: File encoding
            mode: File open mode

        Returns:
            File object
        """
        try:
            return open(file_path, mode, encoding=encoding)
        except Exception as e:
            raise FileError(f"Error opening file {file_path}: {str(e)}") from e

    @staticmethod
    def process_jsonl_lines(
        lines: Iterator[str], processor: Any, context: str = "data"
    ) -> Iterator[dict[str, Any]]:
        """Process JSONL lines with error handling.

        Args:
            lines: Iterator of text lines
            processor: Processor instance for error handling
            context: Context description for errors

        Yields:
            Parsed JSON records
        """
        json_parser, json_decode_error = DataIteratorUtils.get_json_parser()

        for line_number, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json_parser.loads(line)
                yield record
            except json_decode_error as e:
                if not DataIteratorUtils.handle_json_error(
                    e, processor, context, line_number
                ):
                    # Re-raise if recovery failed
                    error_msg = (
                        f"Invalid JSON in {context} at line {line_number}: {str(e)}"
                    )
                    raise ParsingError(error_msg) from e

    @staticmethod
    def detect_file_format(file_path: str) -> str:
        """Detect file format from extension.

        Args:
            file_path: Path to the file

        Returns:
            Detected format ('json', 'jsonl', 'csv', etc.)
        """
        extension = os.path.splitext(file_path)[1].lower()
        if extension in (".jsonl", ".ndjson"):
            return "jsonl"
        elif extension == ".csv":
            return "csv"
        elif extension == ".json":
            return "json"
        else:
            return "unknown"

    @staticmethod
    def wrap_file_error(file_path: str, operation: str, error: Exception) -> None:
        """Wrap file operation errors with consistent messaging.

        Args:
            file_path: Path to the file
            operation: Description of the operation
            error: Original error

        Raises:
            FileError: Wrapped error with consistent messaging
        """
        if isinstance(error, (ProcessingError, FileError, ParsingError)):
            raise
        raise FileError(f"Error {operation} file {file_path}: {str(error)}") from error


def get_data_iterator(
    processor: Any,
    data: Union[
        dict[str, Any], list[dict[str, Any]], str, bytes, Iterator[dict[str, Any]]
    ],
    input_format: str = "auto",
) -> Iterator[dict[str, Any]]:
    """Get an iterator for the input data.

    Args:
        processor: Processor instance
        data: Input data (various formats)
        input_format: Format of the data or 'auto' for detection

    Returns:
        Iterator of data records
    """
    # Return existing iterators directly (but not strings, lists, or dicts)
    if (
        hasattr(data, "__iter__")
        and hasattr(data, "__next__")
        and not isinstance(data, (list, dict, str, bytes))
    ):
        return data

    # Handle file paths (only if it's a string that exists as a file)
    if isinstance(data, str) and os.path.exists(data):
        if input_format == "auto":
            # Use consolidated format detection
            detected_format = DataIteratorUtils.detect_file_format(data)
            if detected_format == "jsonl":
                return get_jsonl_file_iterator(processor, data)
            elif detected_format == "csv":
                return get_csv_file_iterator(processor, data)
            elif detected_format == "json":
                return get_json_file_iterator(data)
            else:
                # Default to JSON for unknown extensions
                return get_json_file_iterator(data)
        elif input_format == "json":
            return get_json_file_iterator(data)
        elif input_format in ("jsonl", "ndjson"):
            return get_jsonl_file_iterator(processor, data)
        elif input_format == "csv":
            return get_csv_file_iterator(processor, data)
        else:
            raise ValueError(f"Unsupported input format: {input_format}")

    # Handle in-memory data structures
    if isinstance(data, dict):
        return iter([data])  # Return iterator instead of yielding
    elif isinstance(data, list):
        return iter(data)  # Return iterator instead of yielding
    # Handle JSON strings and bytes (including format detection for auto mode)
    elif isinstance(data, (str, bytes)):
        if input_format == "auto":
            # Auto-detect format for string/bytes data
            if isinstance(data, bytes):
                sample = data[:1000].decode("utf-8", errors="ignore")
            else:
                sample = data[:1000]

            # Check for JSONL format (newlines with JSON objects)
            if "\n" in sample and any(
                line.strip().startswith("{")
                for line in sample.split("\n")
                if line.strip()
            ):
                return get_jsonl_data_iterator(processor, data)
            else:
                return get_json_data_iterator(data)
        elif input_format == "json":
            return get_json_data_iterator(data)
        elif input_format in ("jsonl", "ndjson"):
            return get_jsonl_data_iterator(processor, data)
        else:
            raise ValueError(f"Unsupported input format: {input_format}")
    else:
        raise ValidationError(f"Unsupported data type: {type(data)}")


def get_json_data_iterator(
    data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
) -> Iterator[dict[str, Any]]:
    """Create an iterator for JSON data.

    Args:
        data: JSON data as string, bytes, dict, or list

    Returns:
        Iterator that yields dictionaries

    Raises:
        ProcessingError: If input contains invalid JSON or unsupported data
        ValidationError: If input is not a supported type
    """
    # Handle dict/list inputs directly
    if isinstance(data, dict):
        yield data
        return
    elif isinstance(data, list):
        yield from data
        return

    # Handle string/bytes inputs
    if not isinstance(data, (str, bytes)):
        raise ValidationError("JSON data must be a string, bytes, dict, or list")

    try:
        # Use consolidated JSON parser selection
        json_parser, _ = DataIteratorUtils.get_json_parser()

        # Parse the data
        parsed_data = json_parser.loads(data)

        # Yield records based on structure
        if isinstance(parsed_data, dict):
            yield parsed_data
        elif isinstance(parsed_data, list):
            yield from parsed_data
        else:
            raise ProcessingError(
                f"Expected dict or list from JSON data, "
                f"got {type(parsed_data).__name__}"
            )

    except Exception as e:
        if isinstance(e, (ProcessingError, ValidationError)):
            raise
        raise ProcessingError(f"Error parsing JSON data: {str(e)}") from e


def get_json_file_iterator(file_path: str) -> Iterator[dict[str, Any]]:
    """Create an iterator for a JSON file.

    Args:
        file_path: Path to JSON file

    Returns:
        Iterator that yields dictionaries

    Raises:
        FileError: If file cannot be read
        ParsingError: If file contains invalid JSON
    """
    # Use consolidated file validation
    DataIteratorUtils.validate_file_exists(file_path)

    try:
        # Use consolidated JSON parser selection
        json_parser, _ = DataIteratorUtils.get_json_parser()

        # Read file with appropriate parser
        if hasattr(json_parser, "load"):
            # Standard json module
            with DataIteratorUtils.read_file_with_encoding(file_path) as f:
                data = json_parser.load(f)
        else:
            # orjson module (requires bytes)
            with open(file_path, "rb") as f:
                data = json_parser.loads(f.read())

        # Process single object or list
        if isinstance(data, dict):
            yield data
        elif isinstance(data, list):
            yield from data
        else:
            raise ParsingError(
                f"Expected dict or list from JSON file, got {type(data).__name__}"
            )

    except Exception as e:
        DataIteratorUtils.wrap_file_error(file_path, "reading", e)


def get_jsonl_file_iterator(processor: Any, file_path: str) -> Iterator[dict[str, Any]]:
    """Create an iterator for a JSONL file (one JSON object per line).

    Args:
        processor: Processor instance
        file_path: Path to JSONL file

    Returns:
        Iterator that yields dictionaries

    Raises:
        FileError: If file cannot be read
        ParsingError: If file contains invalid JSON
    """
    # Use consolidated file validation
    DataIteratorUtils.validate_file_exists(file_path)

    try:
        with DataIteratorUtils.read_file_with_encoding(file_path) as f:
            # Use consolidated JSONL processing
            yield from DataIteratorUtils.process_jsonl_lines(f, processor, file_path)
    except Exception as e:
        DataIteratorUtils.wrap_file_error(file_path, "processing JSONL", e)


def get_jsonl_data_iterator(
    processor: Any, data: Union[str, bytes]
) -> Iterator[dict[str, Any]]:
    """Create an iterator for JSONL data (one JSON object per line).

    Args:
        processor: Processor instance
        data: JSONL data as string or bytes

    Returns:
        Iterator that yields dictionaries

    Raises:
        ParsingError: If input contains invalid JSON
    """
    if not isinstance(data, (str, bytes)):
        raise ValidationError("JSONL data must be a string or bytes")

    # Convert bytes to string if needed
    if isinstance(data, bytes):
        data = data.decode("utf-8")

    try:
        # Split into lines and use consolidated processing
        lines = data.strip().split("\n")
        yield from DataIteratorUtils.process_jsonl_lines(
            iter(lines), processor, "JSONL data"
        )
    except Exception as e:
        if isinstance(e, (ProcessingError, FileError, ParsingError)):
            raise
        raise ParsingError(f"Error processing JSONL data: {str(e)}") from e


def get_csv_file_iterator(
    processor: Any,
    file_path: str,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[list[str]] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
    date_format: Optional[str] = None,
) -> Iterator[dict[str, Any]]:
    """Create an iterator for a CSV file.

    Args:
        processor: Processor instance
        file_path: Path to CSV file
        delimiter: Column delimiter
        has_header: Whether file has a header row
        null_values: Values to interpret as NULL
        sanitize_column_names: Whether to sanitize column names
        infer_types: Whether to infer types from values
        skip_rows: Number of rows to skip
        quote_char: Quote character
        encoding: File encoding
        date_format: Optional format string for parsing dates

    Returns:
        Iterator that yields dictionaries

    Raises:
        FileError: If file cannot be read
    """
    from ..io.readers.csv import CSVReader

    if not os.path.exists(file_path):
        raise FileError(f"File not found: {file_path}")

    try:
        # Configure CSV reader
        reader = CSVReader(
            delimiter=delimiter,
            has_header=has_header,
            null_values=null_values,
            sanitize_column_names=sanitize_column_names,
            infer_types=infer_types,
            skip_rows=skip_rows,
            quote_char=quote_char,
            encoding=encoding,
            cast_to_string=processor.config.processing.cast_to_string,
            date_format=date_format,
        )

        yield from reader.read_records(file_path)

    except Exception as e:
        if isinstance(e, (ProcessingError, FileError)):
            raise
        raise FileError(f"Error reading CSV file {file_path}: {str(e)}") from e
