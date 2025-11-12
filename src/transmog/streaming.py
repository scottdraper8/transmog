"""Streaming processing and result containers."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any, BinaryIO

from transmog.flattening import get_current_timestamp, process_record_batch
from transmog.iterators import get_data_iterator
from transmog.types import ProcessingContext
from transmog.writers import create_streaming_writer


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
    output_destination: str | BinaryIO | None = None,
    extract_time: str | None = None,
    batch_size: int | None = None,
    **format_options: Any,
) -> None:
    """Stream process data and write directly to output.

    Args:
        config: TransmogConfig instance
        data: Input data (dict, list, string, Path, bytes, or iterator)
        entity_name: Name of the entity being processed
        output_format: Output format ("json", "csv", "parquet", "orc", etc)
        output_destination: File path or file-like object to write to
        extract_time: Optional extraction timestamp
        batch_size: Size of batches to process
        **format_options: Format-specific options for the writer
    """
    writer = create_streaming_writer(
        format_name=output_format,
        destination=output_destination,
        entity_name=entity_name,
        **format_options,
    )

    try:
        data_iterator = get_data_iterator(data)
        actual_batch_size = batch_size or config.batch_size
        timestamp = extract_time if extract_time else get_current_timestamp()
        context = ProcessingContext(extract_time=timestamp)

        record_buffer = []
        for record in data_iterator:
            record_buffer.append(record)

            if len(record_buffer) >= actual_batch_size:
                main_records, child_tables = process_record_batch(
                    records=record_buffer,
                    entity_name=entity_name,
                    config=config,
                    _context=context,
                )

                writer.write_main_records(main_records)

                for table_name, table_records in child_tables.items():
                    writer.write_child_records(table_name, table_records)

                record_buffer = []

        if record_buffer:
            main_records, child_tables = process_record_batch(
                records=record_buffer,
                entity_name=entity_name,
                config=config,
                _context=context,
            )

            writer.write_main_records(main_records)

            for table_name, table_records in child_tables.items():
                writer.write_child_records(table_name, table_records)
    finally:
        writer.close()


__all__ = ["stream_process"]
