"""Streaming processing and result containers."""

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from transmog.flattening import get_current_timestamp, process_record_batch
from transmog.iterators import get_data_iterator
from transmog.types import ProcessingContext, ProgressCallback
from transmog.writers import create_streaming_writer

logger = logging.getLogger(__name__)


def stream_process(
    config: Any,
    data: (
        dict[str, Any]
        | list[dict[str, Any]]
        | str
        | Path
        | bytes
        | Iterator[dict[str, Any]]
    ),
    entity_name: str,
    output_format: str,
    output_destination: str | None = None,
    extract_time: str | None = None,
    progress_callback: ProgressCallback | None = None,
    total_records: int | None = None,
    **format_options: Any,
) -> list[Path]:
    """Stream process data and write directly to output.

    Args:
        config: TransmogConfig instance
        data: Input data (dict, list, string, Path, bytes, or iterator)
        entity_name: Name of the entity being processed
        output_format: Output format ("csv", "parquet", "orc", "avro")
        output_destination: Directory path to write to
        extract_time: Optional extraction timestamp
        progress_callback: Optional callable invoked after each batch flush
        total_records: Total input record count (None when unknown)
        **format_options: Format-specific options for the writer

    Returns:
        List of file paths written by the writer.
    """
    writer_options = dict(format_options)
    if config.stringify_values:
        writer_options["stringify_values"] = True

    writer = create_streaming_writer(
        format_name=output_format,
        destination=output_destination,
        entity_name=entity_name,
        batch_size=config.batch_size,
        coerce_schema=config.coerce_schema,
        **writer_options,
    )

    logger.info("stream started, entity=%s, format=%s", entity_name, output_format)

    batch_count = 0
    total_records_processed = 0
    files_written: list[Path] = []

    try:
        data_iterator = get_data_iterator(data, streaming=True)
        actual_batch_size = config.batch_size
        timestamp = extract_time if extract_time else get_current_timestamp()
        context = ProcessingContext(extract_time=timestamp)

        def flush_batch(buffer: list[dict[str, Any]]) -> None:
            nonlocal batch_count, total_records_processed
            main_records, child_tables = process_record_batch(
                records=buffer,
                entity_name=entity_name,
                config=config,
                _context=context,
            )
            writer.write_main_records(main_records)
            for table_name, table_records in child_tables.items():
                writer.write_child_records(table_name, table_records)
            batch_count += 1
            total_records_processed += len(buffer)
            logger.info(
                "stream batch %d processed, records_in_batch=%d, total_records=%d",
                batch_count,
                len(buffer),
                total_records_processed,
            )
            if progress_callback is not None:
                progress_callback(total_records_processed, total_records)

        record_buffer: list[dict[str, Any]] = []
        for record in data_iterator:
            record_buffer.append(record)

            if len(record_buffer) >= actual_batch_size:
                flush_batch(record_buffer)
                record_buffer.clear()

        if record_buffer:
            flush_batch(record_buffer)

        logger.info(
            "stream completed, entity=%s, total_batches=%d, total_records=%d",
            entity_name,
            batch_count,
            total_records_processed,
        )
    finally:
        files_written = writer.close()
    return files_written


__all__ = ["stream_process"]
