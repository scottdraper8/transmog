"""
Streaming processing functionality for Transmog package.

This module contains functions for memory-efficient streaming processing
of data directly to output formats without storing everything in memory.
"""

import os
import json
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    Callable,
    Iterator,
    BinaryIO,
    Set,
    Generator,
    TYPE_CHECKING,
    TypeVar,
)

from ..error import (
    ProcessingError,
    FileError,
    ParsingError,
    error_context,
    logger,
)
from ..core.metadata import (
    get_current_timestamp,
)
from ..core.hierarchy import (
    stream_process_records,
)

from .utils import get_common_config_params, get_batch_size
from .file_handling import handle_file_error
from .data_iterators import (
    get_json_file_iterator,
    get_jsonl_file_iterator,
    get_csv_file_iterator,
)

# Type definitions
if TYPE_CHECKING:
    from ..io.writer_interface import StreamingWriter

# For better type hints without circular imports
StreamingWriterType = TypeVar("StreamingWriterType")


def _get_streaming_params(
    processor,
    extract_time: Optional[Any] = None,
    use_deterministic_ids: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Get parameters for streaming processing functions.

    Args:
        processor: Processor instance
        extract_time: Optional extraction timestamp
        use_deterministic_ids: Whether to use deterministic IDs

    Returns:
        Dictionary of parameters for streaming processing
    """
    # Get base parameters
    params = get_common_config_params(processor, extract_time)

    # Add streaming-specific params
    if use_deterministic_ids is not None:
        params["use_deterministic_ids"] = use_deterministic_ids

    return params


def _stream_process_batch(
    processor,
    batch_data: List[Dict[str, Any]],
    entity_name: str,
    writer: "StreamingWriterType",
    child_tables_registry: Dict[str, bool],
    extract_time: Optional[Any] = None,
    use_deterministic_ids: bool = False,
) -> None:
    """
    Stream process a batch of records and write directly to output.

    Args:
        processor: Processor instance
        batch_data: Batch of records to process
        entity_name: Name of the entity being processed
        writer: StreamingWriter to write output
        child_tables_registry: Registry of discovered child tables
        extract_time: Optional extraction timestamp
        use_deterministic_ids: Whether to use deterministic ID generation
    """
    # Get streaming parameters
    params = _get_streaming_params(processor, extract_time, use_deterministic_ids)

    # Process the batch of records with streaming
    main_records, child_tables_gen = stream_process_records(
        records=batch_data, entity_name=entity_name, **params
    )

    # Write main records to output
    writer.write_main_records(main_records)

    # Process child tables
    for table_name, records in child_tables_gen:
        # Initialize table if we haven't seen it before
        if table_name not in child_tables_registry:
            writer.initialize_child_table(table_name)
            child_tables_registry[table_name] = True

        # Write child records
        writer.write_child_records(table_name, records)


def _stream_process_in_batches(
    processor,
    data_iterator: Iterator[Dict[str, Any]],
    entity_name: str,
    writer: "StreamingWriterType",
    extract_time: Optional[Any] = None,
    batch_size: int = 1000,
    use_deterministic_ids: bool = False,
) -> None:
    """
    Stream process data in batches and write directly to output.

    Args:
        processor: Processor instance
        data_iterator: Iterator over input records
        entity_name: Name of the entity being processed
        writer: StreamingWriter to write output
        extract_time: Optional extraction timestamp
        batch_size: Size of batches to process
        use_deterministic_ids: Whether to use deterministic ID generation
    """
    # Process records in batches
    record_buffer = []
    child_tables_registry = {}  # Track discovered child tables

    for record in data_iterator:
        record_buffer.append(record)

        # Process a batch when buffer is full
        if len(record_buffer) >= batch_size:
            _stream_process_batch(
                processor=processor,
                batch_data=record_buffer,
                entity_name=entity_name,
                extract_time=extract_time,
                writer=writer,
                child_tables_registry=child_tables_registry,
                use_deterministic_ids=use_deterministic_ids,
            )
            record_buffer = []

    # Process any remaining records
    if record_buffer:
        _stream_process_batch(
            processor=processor,
            batch_data=record_buffer,
            entity_name=entity_name,
            extract_time=extract_time,
            writer=writer,
            child_tables_registry=child_tables_registry,
            use_deterministic_ids=use_deterministic_ids,
        )


def _create_streaming_writer(
    processor,
    output_format: str,
    output_destination: Optional[Union[str, BinaryIO]],
    entity_name: str,
    **format_options,
) -> "StreamingWriterType":
    """
    Create a streaming writer for the specified output format.

    Args:
        processor: Processor instance
        output_format: Output format ("json", "csv", "parquet", etc)
        output_destination: File path or file-like object to write to
        entity_name: Name of the entity being processed
        **format_options: Format-specific options for the writer

    Returns:
        StreamingWriter: A writer instance for the specified format
    """
    # Import here to avoid circular imports
    from ..io.writer_factory import create_streaming_writer

    return create_streaming_writer(
        format_name=output_format,
        destination=output_destination,
        entity_name=entity_name,
        **format_options,
    )


def stream_process_file_with_format(
    processor,
    file_path: str,
    entity_name: str,
    output_format: str,
    format_type: str,
    output_destination: Optional[Union[str, BinaryIO]] = None,
    extract_time: Optional[Any] = None,
    use_deterministic_ids: Optional[bool] = None,
    **format_options,
) -> None:
    """
    Stream process a file with known format.

    Args:
        processor: Processor instance
        file_path: Path to the file
        entity_name: Name of the entity
        output_format: Output format
        format_type: Input file format ('json', 'jsonl', 'csv')
        output_destination: Output destination
        extract_time: Optional extraction timestamp
        use_deterministic_ids: Whether to use deterministic IDs
        **format_options: Format-specific options
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileError(f"File not found: {file_path}")

    try:
        # Extract chunk_size before passing to iterators
        chunk_size = format_options.pop("chunk_size", get_batch_size(processor))

        # Create appropriate iterator based on format
        if format_type == "json":
            data_iterator = get_json_file_iterator(file_path)
        elif format_type in ("jsonl", "ndjson"):
            data_iterator = get_jsonl_file_iterator(processor, file_path)
        elif format_type == "csv":
            data_iterator = get_csv_file_iterator(
                processor, file_path, **format_options
            )
        else:
            raise ValueError(f"Unsupported file format: {format_type}")

        # Stream process the data
        stream_process(
            processor=processor,
            data=data_iterator,
            entity_name=entity_name,
            output_format=output_format,
            output_destination=output_destination,
            extract_time=extract_time,
            use_deterministic_ids=use_deterministic_ids,
            **format_options,
        )
    except Exception as e:
        handle_file_error(file_path, e, format_type)


@error_context("Failed to stream process file", log_exceptions=True)
def stream_process_file(
    processor,
    file_path: str,
    entity_name: str,
    output_format: str,
    output_destination: Optional[Union[str, BinaryIO]] = None,
    extract_time: Optional[Any] = None,
    **format_options,
) -> None:
    """
    Stream process a file directly to the specified output format.

    Args:
        processor: Processor instance
        file_path: Path to the input file
        entity_name: Name of the entity being processed
        output_format: Output format ("json", "csv", "parquet", etc)
        output_destination: File path or file-like object to write to
        extract_time: Optional extraction timestamp
        **format_options: Format-specific options for the writer

    Returns:
        None - data is written directly to output_destination
    """
    # Determine file format based on extension
    extension = os.path.splitext(file_path)[1].lower()

    if extension in (".jsonl", ".ndjson"):
        format_type = "jsonl"
    elif extension == ".csv":
        format_type = "csv"
    else:
        format_type = "json"

    return stream_process_file_with_format(
        processor=processor,
        file_path=file_path,
        entity_name=entity_name,
        output_format=output_format,
        format_type=format_type,
        output_destination=output_destination,
        extract_time=extract_time,
        **format_options,
    )


@error_context("Failed to stream process CSV file", log_exceptions=True)
def stream_process_csv(
    processor,
    file_path: str,
    entity_name: str,
    output_format: str,
    output_destination: Optional[Union[str, BinaryIO]] = None,
    extract_time: Optional[Any] = None,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[List[str]] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
    **format_options,
) -> None:
    """
    Stream process a CSV file directly to the specified output format.

    Args:
        processor: Processor instance
        file_path: Path to the CSV file
        entity_name: Name of the entity being processed
        output_format: Output format ("json", "csv", "parquet", etc)
        output_destination: File path or file-like object to write to
        extract_time: Optional extraction timestamp
        delimiter: Column delimiter
        has_header: Whether file has a header row
        null_values: Values to interpret as NULL
        sanitize_column_names: Whether to sanitize column names
        infer_types: Whether to infer types from values
        skip_rows: Number of rows to skip
        quote_char: Quote character for CSV
        encoding: File encoding
        **format_options: Format-specific options for the writer

    Returns:
        None - data is written directly to output_destination
    """
    # Gather CSV-specific options
    csv_options = {
        "delimiter": delimiter,
        "has_header": has_header,
        "null_values": null_values,
        "sanitize_column_names": sanitize_column_names,
        "infer_types": infer_types,
        "skip_rows": skip_rows,
        "quote_char": quote_char,
        "encoding": encoding,
    }

    # Merge with any other format options
    csv_options.update(format_options)

    # Use the helper method
    return stream_process_file_with_format(
        processor=processor,
        file_path=file_path,
        entity_name=entity_name,
        output_format=output_format,
        format_type="csv",
        output_destination=output_destination,
        extract_time=extract_time,
        **csv_options,
    )


@error_context("Failed to stream process data", log_exceptions=True)
def stream_process(
    processor,
    data: Union[
        Dict[str, Any], List[Dict[str, Any]], str, bytes, Iterator[Dict[str, Any]]
    ],
    entity_name: str,
    output_format: str,
    output_destination: Optional[Union[str, BinaryIO]] = None,
    extract_time: Optional[Any] = None,
    batch_size: Optional[int] = None,
    use_deterministic_ids: bool = None,
    **format_options,
) -> None:
    """
    Stream process data directly to the specified output format without storing everything in memory.

    This method processes data in chunks and directly writes the output to the specified
    destination, minimizing memory usage for large datasets.

    Args:
        processor: Processor instance
        data: Input data to process (dict, list, JSON string, file path, or iterator)
        entity_name: Name of the entity being processed
        output_format: Output format ("json", "csv", "parquet", etc)
        output_destination: File path or file-like object to write to
        extract_time: Optional extraction timestamp
        batch_size: Size of batches to process (None = use config value)
        use_deterministic_ids: Whether to use deterministic ID generation
        **format_options: Format-specific options for the writer

    Returns:
        None - data is written directly to output_destination
    """
    # Validate input format and destination
    if not output_format:
        raise ValueError("Output format must be specified for streaming processing")

    # Get the data iterator
    from .data_iterators import get_data_iterator

    data_iterator = get_data_iterator(processor, data)

    # Use batch size from config if not specified
    batch_size = get_batch_size(processor, batch_size)

    # Determine whether to use deterministic IDs if not specified
    if use_deterministic_ids is None:
        # Use deterministic IDs if configured
        use_deterministic_ids = bool(
            processor.config.metadata.deterministic_id_fields
        ) or (processor.config.metadata.id_generation_strategy is not None)

    # Get extract time if not provided
    if extract_time is None:
        extract_time = get_current_timestamp()

    # Create streaming writer
    writer = _create_streaming_writer(
        processor=processor,
        output_format=output_format,
        output_destination=output_destination,
        entity_name=entity_name,
        **format_options,
    )

    try:
        # Process and write the main table header
        writer.initialize_main_table()

        # Process data in batches to manage memory
        _stream_process_in_batches(
            processor=processor,
            data_iterator=data_iterator,
            entity_name=entity_name,
            extract_time=extract_time,
            batch_size=batch_size,
            writer=writer,
            use_deterministic_ids=use_deterministic_ids,
        )

        # Finalize the output
        writer.finalize()
    finally:
        writer.close()
