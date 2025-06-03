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
    logger,
    safe_json_loads,
)


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
    # Return existing iterators directly
    if (
        hasattr(data, "__iter__")
        and hasattr(data, "__next__")
        and not isinstance(data, (list, dict, str, bytes))
    ):
        return data

    # Handle file paths
    if isinstance(data, str) and os.path.exists(data):
        if input_format == "auto":
            # Detect format from file extension
            extension = os.path.splitext(data)[1].lower()
            if extension in (".jsonl", ".ndjson"):
                return get_jsonl_file_iterator(processor, data)
            elif extension == ".csv":
                return get_csv_file_iterator(processor, data)
            else:
                return get_json_file_iterator(data)
        elif input_format == "json":
            return get_json_file_iterator(data)
        elif input_format in ("jsonl", "ndjson"):
            return get_jsonl_file_iterator(processor, data)
        elif input_format == "csv":
            return get_csv_file_iterator(processor, data)
        else:
            raise ValueError(f"Unsupported input format: {input_format}")

    # Handle other data types
    if input_format == "auto":
        # Auto-detect format
        if isinstance(data, (dict, list)):
            return get_json_data_iterator(data)  # type: ignore
        elif isinstance(data, (str, bytes)):
            # Detect if data is JSONL or JSON
            try:
                if isinstance(data, bytes):
                    sample = data[:1000].decode("utf-8", errors="ignore")
                else:
                    sample = data[:1000]

                # Check for newlines with JSON objects
                if "\n" in sample and any(
                    line.strip().startswith("{")
                    for line in sample.split("\n")
                    if line.strip()
                ):
                    return get_jsonl_data_iterator(processor, data)
                else:
                    return get_json_data_iterator(data)
            except Exception:
                # Default to JSON on detection failure
                return get_json_data_iterator(data)
        else:
            raise ValidationError(f"Unsupported data type: {type(data)}")
    elif input_format == "json":
        if isinstance(data, (dict, list, str, bytes)):
            return get_json_data_iterator(data)
        else:
            raise ValidationError(f"Unsupported data type for JSON: {type(data)}")
    elif input_format in ("jsonl", "ndjson"):
        if isinstance(data, (str, bytes)):
            return get_jsonl_data_iterator(processor, data)
        else:
            raise ValidationError("JSONL data must be a string or bytes")
    else:
        raise ValueError(f"Unsupported input format: {input_format}")


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
    if not os.path.exists(file_path):
        raise FileError(f"File not found: {file_path}")

    try:
        # Attempt to use orjson for performance
        try:
            import orjson

            with open(file_path, "rb") as f:
                data = orjson.loads(f.read())
        except ImportError:
            # Fall back to standard json
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)

        # Process single object or list
        if isinstance(data, dict):
            yield data
        elif isinstance(data, list):
            yield from data
        else:
            raise ParsingError(
                f"Expected dict or list from JSON file, got {type(data).__name__}"
            )

    except json.JSONDecodeError as e:
        raise ParsingError(f"Invalid JSON in file {file_path}: {str(e)}") from e
    except Exception as e:
        if isinstance(e, (ProcessingError, FileError, ParsingError)):
            raise
        raise FileError(f"Error reading file {file_path}: {str(e)}") from e


def get_json_data_iterator(
    data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
) -> Iterator[dict[str, Any]]:
    """Create an iterator for JSON data.

    Args:
        data: JSON data as string, bytes, dict, or list

    Returns:
        Iterator that yields dictionaries

    Raises:
        ParsingError: If input contains invalid JSON
    """
    # Parse string/bytes if needed
    if isinstance(data, (str, bytes)):
        try:
            parsed_data = safe_json_loads(data)
        except ParsingError as e:
            logger.error(f"Failed to parse JSON data: {str(e)}")
            raise ProcessingError("Failed to parse JSON data") from e

        data = parsed_data

    # Handle single object or list
    if isinstance(data, dict):
        yield data
    elif isinstance(data, list):
        yield from data
    else:
        raise ValidationError(
            "Data must be a dict, list of dicts, or valid JSON",
            errors={"data": f"got {type(data).__name__}, expected list or dict"},
        )


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
    if not os.path.exists(file_path):
        raise FileError(f"File not found: {file_path}")

    # Select JSON parser
    try:
        import orjson as json_parser

        json_decode_error = (json.JSONDecodeError,)
    except ImportError:
        json_parser = json
        json_decode_error = (json.JSONDecodeError,)

    line_number = 0
    try:
        with open(file_path, encoding="utf-8") as f:
            for line in f:
                line_number += 1
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json_parser.loads(line)
                    yield record
                except json_decode_error as e:
                    error_msg = f"Invalid JSON on line {line_number}: {str(e)}"
                    logger.warning(error_msg)

                    # Handle error based on recovery strategy
                    if (
                        processor.config.error_handling.recovery_strategy is None
                        or processor.config.error_handling.recovery_strategy.is_strict()
                    ):
                        error_details = str(e)
                        error_msg = (
                            f"Invalid JSON in file {file_path} "
                            f"at line {line_number}: {error_details}"
                        )
                        raise ParsingError(error_msg) from e
    except Exception as e:
        if isinstance(e, (ProcessingError, FileError, ParsingError)):
            raise
        raise FileError(f"Error processing JSONL file {file_path}: {str(e)}") from e


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

    # Split into lines
    lines = data.strip().split("\n")

    # Select JSON parser
    try:
        import orjson as json_parser

        json_decode_error = (json.JSONDecodeError,)
    except ImportError:
        import json as json_parser

        json_decode_error = (json.JSONDecodeError,)

    # Process each line
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        try:
            record = json_parser.loads(line)
            yield record
        except json_decode_error as e:
            error_msg = f"Invalid JSON on line {i + 1}: {str(e)}"
            logger.warning(error_msg)

            # Handle error based on recovery strategy
            if (
                processor.config.error_handling.recovery_strategy is None
                or processor.config.error_handling.recovery_strategy.is_strict()
            ):
                raise ParsingError(f"Invalid JSON at line {i + 1}: {str(e)}") from e
        except Exception as e:
            if isinstance(e, (ProcessingError, FileError, ParsingError)):
                raise
            raise ParsingError(f"Error processing JSON data: {str(e)}") from e


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
