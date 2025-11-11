"""Streaming process module for Transmog."""

from collections.abc import Iterator
from typing import (
    Any,
    BinaryIO,
    Optional,
    Union,
)

from transmog.core.hierarchy import stream_process_records
from transmog.core.metadata import get_current_timestamp
from transmog.error import ProcessingError, logger
from transmog.io.writer_factory import create_streaming_writer
from transmog.io.writer_interface import StreamingWriter
from transmog.types import JsonDict, ProcessingContext

from .data_iterators import get_data_iterator


def _stream_process_batch(
    processor: Any,
    batch_data: list[JsonDict],
    entity_name: str,
    writer: StreamingWriter,
    extract_time: str,
) -> None:
    """Process a batch of records and write to output directly.

    Args:
        processor: Processor instance
        batch_data: Batch of records to process
        entity_name: Name of the entity being processed
        writer: StreamingWriter instance
        extract_time: Extraction timestamp
    """
    config = processor.config
    context = ProcessingContext(extract_time=extract_time)

    main_records, child_tables_gen = stream_process_records(
        records=batch_data,
        entity_name=entity_name,
        config=config,
        context=context,
    )

    # Write main records
    writer.write_main_records(main_records)

    child_tables_buffer: dict[str, list[JsonDict]] = {}

    for table_name, record in child_tables_gen:
        child_tables_buffer.setdefault(table_name, []).append(record)

    for table_name, table_records in child_tables_buffer.items():
        writer.write_child_records(table_name, table_records)


def _stream_process_in_batches(
    processor: Any,
    data_iterator: Iterator[JsonDict],
    entity_name: str,
    writer: StreamingWriter,
    extract_time: str,
    batch_size: int,
) -> None:
    """Stream process data in batches and write directly to output.

    Args:
        processor: Processor instance
        data_iterator: Iterator over input records
        entity_name: Name of the entity being processed
        writer: StreamingWriter to write output
        extract_time: Extraction timestamp
        batch_size: Size of batches to process
    """
    record_buffer = []
    for record in data_iterator:
        record_buffer.append(record)

        if len(record_buffer) >= batch_size:
            _stream_process_batch(
                processor=processor,
                batch_data=record_buffer,
                entity_name=entity_name,
                extract_time=extract_time,
                writer=writer,
            )
            record_buffer = []

    if record_buffer:
        _stream_process_batch(
            processor=processor,
            batch_data=record_buffer,
            entity_name=entity_name,
            extract_time=extract_time,
            writer=writer,
        )


def stream_process(
    processor: Any,
    data: Union[
        dict[str, Any], list[dict[str, Any]], str, bytes, Iterator[dict[str, Any]]
    ],
    entity_name: str,
    output_format: str,
    output_destination: Optional[Union[str, BinaryIO]] = None,
    extract_time: Optional[str] = None,
    batch_size: Optional[int] = None,
    **format_options: Any,
) -> None:
    """Stream process data and write directly to output.

    Args:
        processor: Processor instance
        data: Input data (dict, list, string, bytes, or iterator)
        entity_name: Name of the entity being processed
        output_format: Output format ("json", "csv", "parquet", etc)
        output_destination: File path or file-like object to write to
        extract_time: Optional extraction timestamp
        batch_size: Size of batches to process
        **format_options: Format-specific options for the writer
    """
    try:
        writer = create_streaming_writer(
            format_name=output_format,
            destination=output_destination,
            entity_name=entity_name,
            **format_options,
        )

        data_iterator = get_data_iterator(processor, data)
        actual_batch_size = batch_size or processor.config.batch_size
        timestamp = extract_time if extract_time else get_current_timestamp()

        try:
            _stream_process_in_batches(
                processor=processor,
                data_iterator=data_iterator,
                entity_name=entity_name,
                writer=writer,
                extract_time=timestamp,
                batch_size=actual_batch_size,
            )
        finally:
            writer.close()
    except Exception as e:
        logger.error(f"Failed to stream process data: {e}")
        if not isinstance(e, ProcessingError):
            raise ProcessingError(f"Failed to stream process data: {e}") from e
        raise
