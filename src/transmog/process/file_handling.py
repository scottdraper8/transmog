"""
File processing functionality for Transmog package.

This module contains functions for handling file-based processing
operations including file reading, format detection, and error handling.
"""

import os
import json
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    BinaryIO,
)

from ..error import (
    FileError,
    ParsingError,
    ProcessingError,
    error_context,
    logger,
)

from .utils import get_common_config_params, get_batch_size
from .strategy import FileStrategy, CSVStrategy
from .result import ProcessingResult


@error_context("Failed to process file", log_exceptions=True)
def process_file(
    processor,
    file_path: str,
    entity_name: str,
    extract_time: Optional[Any] = None,
) -> ProcessingResult:
    """
    Process a file with automatic format detection.

    Args:
        processor: Processor instance
        file_path: Path to the file to process
        entity_name: Name of the entity being processed
        extract_time: Optional extraction timestamp

    Returns:
        ProcessingResult containing processed data
    """
    # Determine file type based on extension
    file_ext = os.path.splitext(file_path)[1].lower()

    if file_ext == ".csv":
        strategy = CSVStrategy(processor.config)
    else:
        strategy = FileStrategy(processor.config)

    return strategy.process(
        file_path, entity_name=entity_name, extract_time=extract_time
    )


@error_context("Failed to process file", log_exceptions=True)
def process_file_to_format(
    processor,
    file_path: str,
    entity_name: str,
    output_format: str,
    output_path: Optional[str] = None,
    extract_time: Optional[Any] = None,
    **format_options,
) -> ProcessingResult:
    """
    Process a file and write directly to the specified output format.

    This is a convenience method that combines processing and writing in one step.

    Args:
        processor: Processor instance
        file_path: Path to the input file
        entity_name: Name of the entity
        output_format: Output format ("json", "csv", "parquet", etc)
        output_path: Path to write output files
        extract_time: Optional extraction timestamp
        **format_options: Format-specific options

    Returns:
        ProcessingResult object (also writes to output_path if specified)
    """
    # Determine if we should use streaming based on file size
    if os.path.getsize(file_path) > 100 * 1024 * 1024:  # 100 MB
        # For large files, stream directly to output format if possible
        try:
            # Create output directory if it doesn't exist
            if output_path:
                os.makedirs(output_path, exist_ok=True)

            from .streaming import stream_process_file_with_format

            stream_process_file_with_format(
                processor=processor,
                file_path=file_path,
                entity_name=entity_name,
                output_format=output_format,
                format_type=detect_input_format(file_path),
                output_destination=output_path,
                extract_time=extract_time,
                **format_options,
            )

            # Return a minimal ProcessingResult for API compatibility
            return ProcessingResult([], {}, entity_name)
        except Exception as e:
            # Fall back to normal processing if streaming fails
            logger.warning(
                f"Streaming processing failed, falling back to standard processing: {str(e)}"
            )

    # Normal processing
    result = process_file(processor, file_path, entity_name, extract_time)

    # Write to output format if path is specified
    if output_path:
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        # Write to the specified format
        result.write(output_format, output_path, **format_options)

    return result


def detect_input_format(file_path: str) -> str:
    """
    Detect the input format of a file based on its extension.

    Args:
        file_path: Path to the file

    Returns:
        Detected format type ('json', 'jsonl', 'csv')
    """
    extension = os.path.splitext(file_path)[1].lower()

    if extension in (".jsonl", ".ndjson"):
        return "jsonl"
    elif extension == ".csv":
        return "csv"
    else:
        return "json"


def handle_file_error(file_path: str, error: Exception, error_type: str = "file"):
    """
    Handle errors in file processing with consistent error messages.

    Args:
        file_path: Path to the file
        error: Exception that occurred
        error_type: Type of error for message customization

    Raises:
        FileError: If file cannot be read
        ParsingError: If file format is invalid
        ProcessingError: For other processing errors
    """
    if isinstance(error, (ProcessingError, FileError, ParsingError)):
        # Pass through known error types
        raise
    elif isinstance(error, json.JSONDecodeError):
        # JSON parsing errors
        raise ParsingError(f"Invalid JSON in {error_type} {file_path}: {str(error)}")
    else:
        # General file errors
        raise FileError(f"Error reading {error_type} {file_path}: {str(error)}")


@error_context("Failed to process CSV file", log_exceptions=True)
def process_csv(
    processor,
    file_path: str,
    entity_name: str,
    extract_time: Optional[Any] = None,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[List[str]] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
    chunk_size: Optional[int] = None,
) -> ProcessingResult:
    """
    Process a CSV file.

    Args:
        processor: Processor instance
        file_path: Path to the CSV file
        entity_name: Name of the entity being processed
        extract_time: Optional extraction timestamp
        delimiter: CSV delimiter character
        has_header: Whether the CSV has a header row
        null_values: List of strings to treat as null values
        sanitize_column_names: Whether to sanitize column names
        infer_types: Whether to infer data types
        skip_rows: Number of rows to skip at the beginning
        quote_char: Quote character for CSV fields
        encoding: File encoding
        chunk_size: Size of chunks to process

    Returns:
        ProcessingResult containing processed data
    """
    strategy = CSVStrategy(processor.config)
    return strategy.process(
        file_path,
        entity_name=entity_name,
        extract_time=extract_time,
        delimiter=delimiter,
        has_header=has_header,
        null_values=null_values,
        sanitize_column_names=sanitize_column_names,
        infer_types=infer_types,
        skip_rows=skip_rows,
        quote_char=quote_char,
        encoding=encoding,
        chunk_size=chunk_size,
    )


@error_context("Failed to process in chunks", log_exceptions=True)
def process_chunked(
    processor,
    data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
    entity_name: str,
    extract_time: Optional[Any] = None,
    chunk_size: Optional[int] = None,
    input_format: str = "auto",
    **format_options,
) -> ProcessingResult:
    """
    Process data in chunks for memory efficiency.

    Args:
        processor: Processor instance
        data: Input data (dict, list, string, bytes, or file path)
        entity_name: Name of the entity being processed
        extract_time: Optional extraction timestamp
        chunk_size: Size of chunks to process
        input_format: Format of the input data
        **format_options: Additional format options

    Returns:
        ProcessingResult containing processed data
    """
    from .strategy import ChunkedStrategy

    strategy = ChunkedStrategy(processor.config)
    return strategy.process(
        data,
        entity_name=entity_name,
        extract_time=extract_time,
        chunk_size=chunk_size,
        input_format=input_format,
        **format_options,
    )
