"""Chunked processing strategy for memory-efficient processing of large datasets."""

import os
from collections.abc import Generator, Iterator
from typing import Any, Callable, Optional, Union

import orjson

from ...core.hierarchy import process_records_in_single_pass, process_structure
from ...core.metadata import get_current_timestamp
from ...error import ConfigurationError, logger
from ..result import ProcessingResult
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
            ConfigurationError: If data is not a valid type (list, generator,
                JSON string, or file path)
        """
        # Create result if not provided
        if result is None:
            result = ProcessingResult(
                main_table=[], child_tables={}, entity_name=entity_name
            )

        # Handle file paths when input_format is specified
        input_format = kwargs.get("input_format", None)
        if isinstance(data, str) and os.path.exists(data) and input_format is not None:
            # Import here to avoid circular imports
            from .file import FileStrategy

            # Use FileStrategy to properly process the file
            file_strategy = FileStrategy(self.config)
            return file_strategy.process(
                data, entity_name=entity_name, extract_time=extract_time, result=result
            )

        # Check if the string input might be JSON
        if isinstance(data, str):
            try:
                # Try to parse as JSON
                if data.strip().startswith(("{", "[")):
                    parsed_data = orjson.loads(data)
                    # Convert to list if it's a single object
                    if isinstance(parsed_data, dict):
                        data = [parsed_data]
                    elif isinstance(parsed_data, list):
                        data = parsed_data
                    else:
                        raise ConfigurationError(
                            f"Invalid JSON data type: {type(parsed_data)}"
                        )
                else:
                    # Not valid JSON, treat as error
                    raise ConfigurationError(
                        "Invalid input: String provided is not a valid JSON "
                        "object or array"
                    )
            except orjson.JSONDecodeError:
                # Not valid JSON
                raise ConfigurationError(
                    "Invalid input: String provided is not valid JSON or a file path"
                ) from None

        # Check for unsupported data types
        if not isinstance(data, (list, Iterator)):
            raise ConfigurationError(
                f"Unsupported data type: {type(data)}. "
                f"Expected list, generator, JSON string, or file path."
            )

        # Extract common parameters from kwargs or config
        params = self._get_common_parameters(**kwargs)

        # Get extraction timestamp
        extract_time = extract_time or get_current_timestamp()

        # Get chunk size
        chunk_size = self._get_batch_size(kwargs.get("chunk_size"))

        # Get ID fields
        id_field = params.get("id_field", "__transmog_id")
        parent_field = params.get("parent_field", "__parent_transmog_id")
        time_field = params.get("time_field", "__transmog_datetime")
        default_id_field = params.get("default_id_field")
        id_generation_strategy = params.get("id_generation_strategy")

        # Process data in chunks
        accumulated_chunk: list[dict[str, Any]] = []

        # Iterate through records
        for record in data:
            # Skip any non-dict records
            if not isinstance(record, dict):
                continue

            # Add to chunk
            accumulated_chunk.append(record)

            # Process chunk when it reaches the desired size
            if len(accumulated_chunk) >= chunk_size:
                self._process_chunk(
                    accumulated_chunk,
                    entity_name,
                    extract_time,
                    result,
                    id_field,
                    parent_field,
                    time_field,
                    default_id_field,
                    id_generation_strategy,
                    params,
                )
                # Reset chunk
                accumulated_chunk = []

        # Process any remaining records
        if accumulated_chunk:
            self._process_chunk(
                accumulated_chunk,
                entity_name,
                extract_time,
                result,
                id_field,
                parent_field,
                time_field,
                default_id_field,
                id_generation_strategy,
                params,
            )

        return result

    def _process_chunk(
        self,
        chunk: list[dict[str, Any]],
        entity_name: str,
        extract_time: Optional[Any],
        result: ProcessingResult,
        id_field: str,
        parent_field: str,
        time_field: str,
        default_id_field: Optional[Union[str, dict[str, str]]],
        id_generation_strategy: Optional[Callable[[dict[str, Any]], str]],
        params: dict[str, Any],
    ) -> ProcessingResult:
        """Process a chunk of records with arrays."""
        if not chunk:
            return result

        # Process the main records
        flattened_records, table_arrays = process_records_in_single_pass(
            chunk,
            entity_name=entity_name,
            transmog_time=extract_time,
            separator=params.get("separator", "_"),
            cast_to_string=params.get("cast_to_string", True),
            include_empty=params.get("include_empty", False),
            skip_null=params.get("skip_null", True),
            id_field=id_field,
            parent_field=parent_field,
            time_field=time_field,
            visit_arrays=params.get("visit_arrays", True),
            nested_threshold=params.get("nested_threshold", 4),
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            recovery_strategy=params.get("recovery_strategy"),
            max_depth=params.get("max_depth", 100),
            keep_arrays=params.get("keep_arrays", False),
            id_field_patterns=params.get("id_field_patterns"),
            id_field_mapping=params.get("id_field_mapping"),
            force_transmog_id=params.get("force_transmog_id", False),
        )

        # Add records to results
        for record in flattened_records:
            result.add_main_record(record)

        # Add array tables
        for table_name, records in table_arrays.items():
            for record in records:
                result.add_child_record(table_name, record)

        return result

    def _process_batch_arrays(
        self,
        batch: list[dict[str, Any]],
        entity_name: str,
        extract_time: Any,
        result: ProcessingResult,
        id_field: str,
        main_ids: list[str],
        default_id_field: Optional[Union[str, dict[str, str]]],
        id_generation_strategy: Optional[Callable[[dict[str, Any]], str]],
        params: dict[str, Any],
    ) -> None:
        """Process arrays from a batch of records.

        Args:
            batch: Batch of records to process
            entity_name: Name of the entity being processed
            extract_time: Extraction timestamp
            result: Result object to update
            id_field: ID field name
            main_ids: List of IDs for the parent records
            default_id_field: Field name or mapping for deterministic IDs
            id_generation_strategy: Custom function for ID generation
            params: Processing parameters
        """
        # Ensure valid parameters
        if len(batch) != len(main_ids):
            logger.warning(
                f"Batch size mismatch: records={len(batch)}, ids={len(main_ids)}"
            )
            # Use only as many IDs as available records
            main_ids = main_ids[: len(batch)]

        # Get processing parameters
        visit_arrays = params.get("visit_arrays", True)
        keep_arrays = params.get("keep_arrays", False)

        # Only process arrays if enabled
        if not visit_arrays:
            logger.debug("Array processing disabled, skipping")
            return

        # Process arrays for each record in the batch
        for i, (record, parent_id) in enumerate(zip(batch, main_ids)):
            # Skip if we don't have a valid parent ID
            if parent_id is None:
                logger.warning(
                    f"Null parent ID for record at index {i}, skipping arrays"
                )
                continue

            # Process arrays for this record
            _, arrays = process_structure(
                data=record,
                entity_name=entity_name,
                parent_id=parent_id,
                separator=params.get("separator", "_"),
                cast_to_string=params.get("cast_to_string", True),
                include_empty=params.get("include_empty", False),
                skip_null=params.get("skip_null", True),
                transmog_time=extract_time,
                nested_threshold=params.get("nested_threshold", 4),
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=params.get("recovery_strategy"),
                keep_arrays=keep_arrays,
                id_field=id_field,
                parent_field=params.get("parent_field", "__parent_transmog_id"),
                time_field=params.get("time_field", "__transmog_datetime"),
                id_field_patterns=params.get("id_field_patterns"),
                id_field_mapping=params.get("id_field_mapping"),
                force_transmog_id=params.get("force_transmog_id", False),
            )

            # Add arrays to result for this record - ensure arrays is a dict
            if isinstance(arrays, dict):
                for table_name, records in arrays.items():
                    if not records:
                        logger.debug(f"Empty records list for table {table_name}")
                        continue

                    logger.debug(
                        f"Adding {len(records)} records to child table {table_name}"
                    )
                    for child in records:
                        result.add_child_record(table_name, child)

                # Only remove array fields if keep_arrays is False
                if not keep_arrays:
                    # Remove array fields from the original record after they've been
                    # processed
                    # This ensures arrays don't exist in both the main table
                    # and child tables
                    self._remove_array_fields_from_record(record)

                    # Also update the main record in the result if it exists
                    if i < len(result.main_table):
                        self._remove_array_fields_from_record(result.main_table[i])
            else:
                logger.warning(f"No arrays found for record at index {i}")
