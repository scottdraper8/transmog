"""File processing functionality for Transmog package.

This module contains functions for handling file-based processing
operations including file reading, format detection, and error handling.
"""

import os
from collections.abc import Generator
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
    cast,
)

from ..error import error_context, logger
from .result import ProcessingResult
from .strategy import CSVStrategy, FileStrategy

# Define a return type variable for the decorator's generic type
R = TypeVar("R")

# Define a type for the decorator function
F = TypeVar("F", bound=Callable[..., Any])


@error_context("Failed to process file", log_exceptions=True)  # type: ignore
def process_file(
    processor: Any,
    file_path: str,
    entity_name: str,
    extract_time: Optional[Any] = None,
) -> ProcessingResult:
    """Process a file with automatic format detection.

    Args:
        processor: Processor instance
        file_path: Path to the file to process
        entity_name: Name of the entity being processed
        extract_time: Optional extraction timestamp

    Returns:
        ProcessingResult containing processed data
    """
    file_ext = os.path.splitext(file_path)[1].lower()

    if file_ext == ".csv":
        csv_strategy = CSVStrategy(processor.config)
        processed_result = csv_strategy.process(
            file_path, entity_name=entity_name, extract_time=extract_time
        )
    else:
        file_strategy = FileStrategy(processor.config)
        processed_result = file_strategy.process(
            file_path, entity_name=entity_name, extract_time=extract_time
        )

    return cast(ProcessingResult, processed_result)


@error_context("Failed to process file to format", log_exceptions=True)  # type: ignore
def process_file_to_format(
    processor: Any,
    file_path: str,
    entity_name: str,
    output_format: str,
    output_path: Optional[str] = None,
    extract_time: Optional[Any] = None,
    **format_options: Any,
) -> ProcessingResult:
    """Process a file and save to a specified format.

    This is a convenience method that combines processing and writing in one step.

    Args:
        processor: Processor instance
        file_path: Path to the input file
        entity_name: Name of the entity
        output_format: Output format (json, csv, parquet)
        output_path: Path to save output
        extract_time: Optional extraction timestamp
        **format_options: Format-specific options

    Returns:
        ProcessingResult containing processed data
    """
    # Use streaming for files larger than 100 MB
    if os.path.getsize(file_path) > 100 * 1024 * 1024:
        try:
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

            # Return minimal result object for API compatibility
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
                source_info={"file_path": file_path, "streaming": True},
            )
            return result
        except Exception as e:
            # Fall back to normal processing if streaming fails
            error_message = str(e)
            logger.warning(
                f"Streaming processing failed, falling back to standard processing: "
                f"{error_message}"
            )

    processed_result = process_file(processor, file_path, entity_name, extract_time)

    if output_path:
        os.makedirs(output_path, exist_ok=True)
        processed_result.write(output_format, output_path, **format_options)

    return cast(ProcessingResult, processed_result)


def detect_input_format(file_path: str) -> str:
    """Detect the input format of a file based on its extension.

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


@error_context("Failed to process CSV file", log_exceptions=True)  # type: ignore
def process_csv(
    processor: Any,
    file_path: str,
    entity_name: str,
    extract_time: Optional[Any] = None,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[list[str]] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
    chunk_size: Optional[int] = None,
    date_format: Optional[str] = None,
) -> ProcessingResult:
    """Process a CSV file.

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
        date_format: Optional format string for parsing dates

    Returns:
        ProcessingResult containing processed data
    """
    strategy = CSVStrategy(processor.config)
    # Initialize result object with empty structures
    result = ProcessingResult(
        main_table=[],
        child_tables={},
        entity_name=entity_name,
    )
    result.source_info["file_path"] = file_path

    processed_result = strategy.process(
        file_path,
        entity_name=entity_name,
        extract_time=extract_time,
        result=result,
        delimiter=delimiter,
        has_header=has_header,
        null_values=null_values,
        sanitize_column_names=sanitize_column_names,
        infer_types=infer_types,
        skip_rows=skip_rows,
        quote_char=quote_char,
        encoding=encoding,
        chunk_size=chunk_size,
        date_format=date_format,
    )

    return cast(ProcessingResult, processed_result)


@error_context("Failed to process in chunks", log_exceptions=True)  # type: ignore
def process_chunked(
    processor: Any,
    data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
    entity_name: str,
    extract_time: Optional[Any] = None,
    chunk_size: Optional[int] = None,
    input_format: str = "auto",
    **format_options: Any,
) -> ProcessingResult:
    """Process data in chunks for memory-efficient processing.

    Args:
        processor: Processor instance
        data: Data to process in chunks (list, dict, file path, or generator)
        entity_name: Name of the entity being processed
        extract_time: Optional extraction timestamp
        chunk_size: Size of chunks to process at once
        input_format: Format of input data ("auto", "json", "jsonl", etc.)
        **format_options: Format-specific options

    Returns:
        ProcessingResult with processed data
    """
    from .strategy import ChunkedStrategy

    chunked_strategy = ChunkedStrategy(processor.config)

    result = ProcessingResult(
        main_table=[],
        child_tables={},
        entity_name=entity_name,
    )

    # Convert input data to appropriate format for processing
    processed_data: Union[list[dict[str, Any]], Generator[dict[str, Any], None, None]]

    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        processed_data = data
    elif isinstance(data, dict):
        processed_data = [data]
    elif isinstance(data, (str, bytes)):
        # Process file path if data is a string and the file exists
        if isinstance(data, str) and os.path.exists(data):
            file_strategy = FileStrategy(processor.config)
            file_result = file_strategy.process(
                data, entity_name=entity_name, extract_time=extract_time
            )
            return cast(ProcessingResult, file_result)
        else:
            # Treat non-file path strings/bytes as raw data
            processed_data = [{"raw_data": str(data)}]
    else:
        # Generic fallback for unsupported data types
        processed_data = [{"raw_data": str(data)}]

    processed_result = chunked_strategy.process(
        processed_data,
        entity_name=entity_name,
        extract_time=extract_time,
        result=result,
        chunk_size=chunk_size,
        input_format=input_format,
        **format_options,
    )

    return cast(ProcessingResult, processed_result)
