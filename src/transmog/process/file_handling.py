"""File processing functionality for Transmog package.

This module contains functions for handling file-based processing
operations including file reading, format detection, and error handling.
"""

import os
from collections.abc import Generator
from typing import (
    Any,
    Optional,
    Union,
)

from ..error import error_context, logger
from .result import ProcessingResult
from .strategy import CSVStrategy, FileStrategy


@error_context("Failed to process file", log_exceptions=True)
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
    # Determine file type based on extension
    file_ext = os.path.splitext(file_path)[1].lower()

    # Create the appropriate strategy
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

    # Return the processed result
    return processed_result


@error_context("Failed to process file", log_exceptions=True)
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

    # Normal processing - this will always return a valid ProcessingResult
    processed_result = process_file(processor, file_path, entity_name, extract_time)

    # Write to output format if path is specified
    if output_path:
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        # Write to the specified format
        processed_result.write(output_format, output_path, **format_options)

    return processed_result


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


@error_context("Failed to process CSV file", log_exceptions=True)
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

    Returns:
        ProcessingResult containing processed data
    """
    strategy = CSVStrategy(processor.config)
    # Create a result object first to ensure we don't return None
    result = ProcessingResult(
        main_table=[],
        child_tables={},
        entity_name=entity_name,
    )
    result.source_info["file_path"] = file_path

    # Process the file
    return strategy.process(
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
    )


@error_context("Failed to process in chunks", log_exceptions=True)
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

    # Create a chunked strategy with the processor's config
    chunked_strategy = ChunkedStrategy(processor.config)

    # Create result object
    result = ProcessingResult(
        main_table=[],
        child_tables={},
        entity_name=entity_name,
    )

    # Convert data to expected format for ChunkedStrategy
    processed_data: Union[list[dict[str, Any]], Generator[dict[str, Any], None, None]]

    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        processed_data = data
    elif isinstance(data, dict):
        processed_data = [data]
    elif isinstance(data, (str, bytes)):
        # If it's a string, treat it as a file path if it exists
        if isinstance(data, str) and os.path.exists(data):
            # Use FileStrategy to properly process the file
            file_strategy = FileStrategy(processor.config)
            file_result = file_strategy.process(
                data, entity_name=entity_name, extract_time=extract_time
            )
            # Return the processed file directly
            return file_result
        else:
            # If not a file path, create a single record with the raw data
            processed_data = [{"raw_data": str(data)}]
    else:
        # Fallback for any other type
        processed_data = [{"raw_data": str(data)}]

    # Process with the chunked strategy
    return chunked_strategy.process(
        processed_data,
        entity_name=entity_name,
        extract_time=extract_time,
        result=result,
        chunk_size=chunk_size,
        input_format=input_format,
        **format_options,
    )
