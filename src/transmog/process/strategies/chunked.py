"""Chunked processing strategy for memory-efficient processing of large datasets."""

import os
from collections.abc import Generator
from typing import Any, Optional, Union

import orjson

from transmog.core.hierarchy import process_records_in_single_pass
from transmog.core.metadata import get_current_timestamp
from transmog.error import ConfigurationError
from transmog.process.result import ProcessingResult
from transmog.types import ProcessingContext

from .base import ProcessingStrategy


class ChunkedStrategy(ProcessingStrategy):
    """Strategy for processing data in chunks for memory optimization."""

    def process(
        self,
        data: Union[list[dict[str, Any]], Generator[dict[str, Any], None, None], str],
        entity_name: str,
        extract_time: Optional[Any] = None,
        result: Optional[ProcessingResult] = None,
        **kwargs: Any,
    ) -> ProcessingResult:
        """Process data in memory-efficient chunks.

        Args:
            data: List of dictionaries, data generator, or file path
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            result: Optional existing result to append to
            **kwargs: Additional parameters including:
                - input_format: Format of input data when data is a file path
                - chunk_size: Size of chunks to process

        Returns:
            ProcessingResult containing the processed data

        Raises:
            ConfigurationError: If data is not a valid type
        """
        if result is None:
            result = ProcessingResult(
                main_table=[], child_tables={}, entity_name=entity_name
            )

        input_format = kwargs.get("input_format", None)
        if isinstance(data, str) and os.path.exists(data) and input_format is not None:
            from .file import FileStrategy

            file_strategy = FileStrategy(self.config)
            return file_strategy.process(
                data, entity_name=entity_name, extract_time=extract_time, result=result
            )

        extract_time = extract_time or get_current_timestamp()
        context = ProcessingContext(extract_time=extract_time)

        if isinstance(data, str):
            try:
                parsed_data = orjson.loads(data)
                if isinstance(parsed_data, dict):
                    data_iter = iter([parsed_data])
                elif isinstance(parsed_data, list):
                    data_iter = iter(parsed_data)
                else:
                    raise ConfigurationError(
                        f"Expected dict or list, got {type(parsed_data).__name__}"
                    )
            except orjson.JSONDecodeError as e:
                raise ConfigurationError(f"Invalid JSON: {e}") from e
        elif isinstance(data, list):
            data_iter = iter(data)
        elif isinstance(data, Generator):
            data_iter = data
        else:
            raise ConfigurationError(f"Unsupported data type: {type(data).__name__}")

        chunk_size = self.config.batch_size
        chunk = []

        for record in data_iter:
            chunk.append(record)

            if len(chunk) >= chunk_size:
                self._process_chunk(chunk, entity_name, context, result)
                chunk = []

        if chunk:
            self._process_chunk(chunk, entity_name, context, result)

        return result

    def _process_chunk(
        self,
        chunk: list[dict[str, Any]],
        entity_name: str,
        context: ProcessingContext,
        result: ProcessingResult,
    ) -> None:
        """Process a single chunk of records.

        Args:
            chunk: Chunk of records to process
            entity_name: Entity name
            context: Processing context
            result: Result object to append to
        """
        processed_records, child_arrays = process_records_in_single_pass(
            chunk,
            entity_name=entity_name,
            config=self.config,
            context=context,
        )

        if child_arrays:
            result.add_child_tables(child_arrays)

        for record in processed_records:
            result.add_main_record(record)
