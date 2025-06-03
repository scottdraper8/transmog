"""Streaming process module for Transmog.

Provides functions for streaming processing of data directly to output.
"""

import os
from collections.abc import Iterator
from typing import (
    Any,
    BinaryIO,
    Optional,
    Union,
)

from ..core.metadata import get_current_timestamp
from ..error import (
    FileError,
    error_context,
)
from ..io.writer_factory import create_streaming_writer
from ..types.base import JsonDict
from ..types.io_types import StreamingWriterProtocol
from .data_iterators import (
    get_csv_file_iterator,
    get_data_iterator,
    get_json_file_iterator,
    get_jsonl_file_iterator,
)
from .utils import get_batch_size, get_common_config_params, handle_file_error


def _get_streaming_params(
    processor: Any,
    extract_time: Optional[Any] = None,
    use_deterministic_ids: Optional[bool] = None,
) -> dict[str, Any]:
    """Get parameters for streaming processing functions.

    Args:
        processor: Processor instance
        extract_time: Optional extraction timestamp
        use_deterministic_ids: Whether to use deterministic IDs

    Returns:
        Dictionary of parameters for streaming processing
    """
    # Get base parameters
    params: dict[str, Any] = get_common_config_params(processor, extract_time)

    # Add streaming-specific params
    if use_deterministic_ids is not None:
        params["use_deterministic_ids"] = use_deterministic_ids

    return params


def _stream_process_batch(
    processor: Any,
    batch_data: list[JsonDict],
    entity_name: str,
    writer: StreamingWriterProtocol,
    child_tables_registry: dict[str, bool],
    extract_time: Optional[Any] = None,
    use_deterministic_ids: bool = False,
) -> None:
    """Process a batch of records and write to output directly.

    Args:
        processor: Processor instance
        batch_data: Batch of records to process
        entity_name: Name of the entity being processed
        writer: StreamingWriter instance
        child_tables_registry: Registry of discovered child tables
        extract_time: Optional extraction timestamp
        use_deterministic_ids: Whether to use deterministic ID generation
    """
    # Get processing parameters
    params = _get_streaming_params(
        processor=processor,
        extract_time=extract_time,
        use_deterministic_ids=use_deterministic_ids,
    )

    # Import here to avoid circular imports
    from transmog.core.hierarchy import stream_process_records

    # Process batch with streaming
    main_records, child_tables_gen = stream_process_records(
        records=batch_data,
        entity_name=entity_name,
        **params,
    )

    # Write main records
    writer.write_main_records(main_records)

    # Buffer child records by table to handle both individual records and batches
    child_tables_buffer: dict[str, list[dict[str, Any]]] = {}

    # Process child tables
    for table_name, records in child_tables_gen:
        # Initialize table if we haven't seen it before
        if table_name not in child_tables_registry:
            writer.initialize_child_table(table_name)
            child_tables_registry[table_name] = True

        # Handle both individual records and batches of records
        if isinstance(records, dict):
            # Single record case
            if table_name not in child_tables_buffer:
                child_tables_buffer[table_name] = []
            child_tables_buffer[table_name].append(records)
        elif isinstance(records, list):
            # Batch case (for backward compatibility)
            writer.write_child_records(table_name, records)
        else:
            raise TypeError(f"Unexpected record type: {type(records)}")

    # Write buffered child records
    for table_name, table_records in child_tables_buffer.items():
        if table_records:
            writer.write_child_records(table_name, table_records)


def _stream_process_in_batches(
    processor: Any,
    data_iterator: Iterator[JsonDict],
    entity_name: str,
    writer: StreamingWriterProtocol,
    extract_time: Optional[Any] = None,
    batch_size: Optional[int] = 1000,
    use_deterministic_ids: bool = False,
) -> None:
    """Stream process data in batches and write directly to output.

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
    child_tables_registry: dict[str, bool] = {}  # Track discovered child tables

    for record in data_iterator:
        record_buffer.append(record)

        # Process a batch when buffer is full
        if len(record_buffer) >= (batch_size or 1000):
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
    processor: Any,
    output_format: str,
    output_destination: Optional[Union[str, BinaryIO]],
    entity_name: str,
    **format_options: Any,
) -> StreamingWriterProtocol:
    """Create a streaming writer for the specified output format.

    Args:
        processor: Processor instance
        output_format: Output format ("json", "csv", "parquet", etc)
        output_destination: File path or file-like object to write to
        entity_name: Name of the entity being processed
        **format_options: Format-specific options for the writer

    Returns:
        StreamingWriter: A writer instance for the specified format
    """
    return create_streaming_writer(
        format_name=output_format,
        destination=output_destination,
        entity_name=entity_name,
        **format_options,
    )


def stream_process_file_with_format(
    processor: Any,
    file_path: str,
    entity_name: str,
    output_format: str,
    format_type: str,
    output_destination: Optional[Union[str, BinaryIO]] = None,
    extract_time: Optional[Any] = None,
    use_deterministic_ids: bool = False,
    **format_options: Any,
) -> None:
    """Stream process a file with known format.

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
        format_options.pop("chunk_size", get_batch_size(processor))

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


@error_context("Failed to stream process file", log_exceptions=True)  # type: ignore
def stream_process_file(
    processor: Any,
    file_path: str,
    entity_name: str,
    output_format: str,
    output_destination: Optional[Union[str, BinaryIO]] = None,
    extract_time: Optional[Any] = None,
    **format_options: Any,
) -> None:
    """Stream process a file directly to the specified output format.

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


@error_context("Failed to stream process CSV file", log_exceptions=True)  # type: ignore
def stream_process_csv(
    processor: Any,
    file_path: str,
    entity_name: str,
    output_format: str,
    output_destination: Optional[Union[str, BinaryIO]] = None,
    extract_time: Optional[Any] = None,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[list[str]] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
    date_format: Optional[str] = None,
    **format_options: Any,
) -> None:
    """Stream process a CSV file directly to the specified output format.

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
        date_format: Optional format string for parsing dates
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
        "date_format": date_format,
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
        use_deterministic_ids=False,
        **csv_options,
    )


@error_context("Failed to stream process data", log_exceptions=True)  # type: ignore
def stream_process(
    processor: Any,
    data: Union[
        dict[str, Any], list[dict[str, Any]], str, bytes, Iterator[dict[str, Any]]
    ],
    entity_name: str,
    output_format: str,
    output_destination: Optional[Union[str, BinaryIO]] = None,
    extract_time: Optional[Any] = None,
    batch_size: Optional[int] = None,
    use_deterministic_ids: bool = False,
    **format_options: Any,
) -> None:
    """Stream process data directly to output format.

    Args:
        processor: Processor instance
        data: Input data (dict, list, string, bytes, or iterator)
        entity_name: Name of the entity
        output_format: Output format
        output_destination: Output destination (file path or file-like object)
        extract_time: Optional extraction timestamp
        batch_size: Optional batch size for processing
        use_deterministic_ids: Whether to use deterministic IDs
        **format_options: Format-specific options
    """
    # Validate input format and destination
    if not output_format:
        raise ValueError("Output format must be specified for streaming processing")

    # Get the data iterator
    data_iterator = get_data_iterator(processor, data)

    # Use batch size from config if not specified
    batch_size = get_batch_size(processor, batch_size)

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
            batch_size=batch_size if batch_size is not None else 1000,
            writer=writer,
            use_deterministic_ids=use_deterministic_ids,
        )

        # Finalize the output
        writer.finalize()
    finally:
        writer.close()
