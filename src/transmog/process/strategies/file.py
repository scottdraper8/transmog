"""File processing strategy for handling JSON and JSONL files."""

import os
from collections.abc import Generator, Iterator
from typing import Any, Callable, Optional, Union

import orjson

from ...error import FileError, logger
from ..result import ProcessingResult
from ..utils import handle_file_error
from .base import ProcessingStrategy
from .shared import process_batch_arrays, process_batch_main_records


class FileStrategy(ProcessingStrategy):
    """Strategy for processing files."""

    def process(
        self,
        data: Any,
        entity_name: str,
        extract_time: Optional[Any] = None,
        result: Optional[ProcessingResult] = None,
        **kwargs: Any,
    ) -> ProcessingResult:
        """Process a file.

        Args:
            data: Path to the file (str)
            entity_name: Name of the entity to process
            extract_time: Optional extraction timestamp
            result: Optional existing result to append to
            **kwargs: Additional parameters

        Returns:
            ProcessingResult containing the processed data
        """
        # Convert data to file path if it's a string
        if not isinstance(data, str):
            raise TypeError(f"Expected string file path, got {type(data).__name__}")

        file_path = data

        # Check file exists
        if not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

        # Initialize result if not provided
        if result is None:
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
            )
            result.source_info["file_path"] = file_path

        # Determine file type based on extension
        file_ext = os.path.splitext(file_path)[1].lower()

        try:
            # Process different file types
            if file_ext in (".json", ".geojson"):
                return self._process_json_file(
                    file_path, entity_name, extract_time, result
                )
            elif file_ext in (".jsonl", ".ndjson", ".ldjson"):
                return self._process_jsonl_file(
                    file_path,
                    entity_name,
                    extract_time,
                    kwargs.get("chunk_size"),
                    result,
                )
            elif file_ext == ".csv":
                # Import here to avoid circular imports
                from .csv import CSVStrategy

                # CSVStrategy, but provide a fallback to avoid errors
                logger.warning(
                    f"CSV file {file_path} being processed by FileStrategy "
                    f"instead of CSVStrategy"
                )

                # Create a CSV strategy for this file
                csv_strategy = CSVStrategy(self.config)
                return csv_strategy.process(
                    file_path,
                    entity_name=entity_name,
                    extract_time=extract_time,
                    result=result,
                )
            else:
                # Default to JSON (it will raise appropriate errors if it's
                # not valid JSON)
                return self._process_json_file(
                    file_path, entity_name, extract_time, result
                )
        except Exception as e:
            handle_file_error(file_path, e)
            # This line will never be reached as handle_file_error always raises
            return result  # Just to make type checker happy

    def _process_json_file(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
        result: Optional[ProcessingResult] = None,
    ) -> ProcessingResult:
        """Process a JSON file.

        Args:
            file_path: Path to the JSON file
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            result: Optional existing result to append to

        Returns:
            ProcessingResult containing the processed data
        """
        # Create an empty result if not provided
        if result is None:
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
            )
            result.source_info["file_path"] = file_path

        try:
            # Load JSON file
            with open(file_path, encoding="utf-8") as f:
                data = orjson.loads(f.read())

            # Import here to avoid circular imports
            from .memory import InMemoryStrategy

            # Create in-memory strategy for processing the loaded data
            in_memory = InMemoryStrategy(self.config)

            # Process the data
            return in_memory.process(
                data, entity_name=entity_name, extract_time=extract_time, result=result
            )
        except Exception as e:
            handle_file_error(file_path, e)
            # This line will never be reached as handle_file_error always raises
            return result  # Just to make type checker happy

    def _process_jsonl_file(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
        chunk_size: Optional[int] = None,
        result: Optional[ProcessingResult] = None,
    ) -> ProcessingResult:
        """Process a JSONL file.

        Args:
            file_path: Path to the JSONL file
            entity_name: Name of the entity to process
            extract_time: Optional extraction timestamp
            chunk_size: Optional chunk size for batch processing
            result: Optional existing result to append to

        Returns:
            ProcessingResult containing the processed data
        """
        # Create an empty result if not provided
        if result is None:
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
            )
            result.source_info["file_path"] = file_path

        # Get common params
        params = self._get_common_config_params(extract_time)

        # Get id fields
        id_field = params.get("id_field", "__transmog_id")
        parent_field = params.get("parent_field", "__parent_transmog_id")
        time_field = params.get("time_field", "__transmog_datetime")
        default_id_field = params.get("default_id_field")
        id_generation_strategy = params.get("id_generation_strategy")

        # Determine batch size
        batch_size = self._get_batch_size(chunk_size)

        try:
            # Define JSONL iterator for memory-efficient processing
            def jsonl_iterator() -> Generator[dict[str, Any], None, None]:
                with open(file_path, encoding="utf-8") as f:
                    for line in f:
                        if line.strip():  # Skip empty lines
                            try:
                                record = orjson.loads(line)
                                if isinstance(record, dict):
                                    yield record
                                else:
                                    logger.warning(
                                        f"Skipping non-dict JSON in {file_path}: "
                                        f"{type(record).__name__}"
                                    )
                            except orjson.JSONDecodeError:
                                logger.warning(
                                    f"Invalid JSON in {file_path}: {line[:100]}..."
                                )

            # Process the file in batches
            return self._process_data_batches(
                data_iterator=jsonl_iterator(),
                entity_name=entity_name,
                extract_time=extract_time,
                result=result,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                params=params,
                batch_size=batch_size,
            )
        except Exception as e:
            handle_file_error(file_path, e)
            # This line will never be reached as handle_file_error always raises
            return result  # Just to make type checker happy

    def _process_data_batches(
        self,
        data_iterator: Iterator[dict[str, Any]],
        entity_name: str,
        extract_time: Any,
        result: ProcessingResult,
        id_field: str,
        parent_field: str,
        time_field: str,
        default_id_field: Optional[Union[str, dict[str, str]]],
        id_generation_strategy: Optional[Callable[[dict[str, Any]], str]],
        params: dict[str, Any],
        batch_size: int,
    ) -> ProcessingResult:
        """Process data in batches from an iterator.

        Args:
            data_iterator: Iterator yielding data records
            entity_name: Name of the entity
            extract_time: Extraction timestamp
            result: ProcessingResult to update
            id_field: ID field name
            parent_field: Parent ID field name
            time_field: Timestamp field name
            default_id_field: Field name or dict mapping paths to field names
                for deterministic IDs
            id_generation_strategy: Custom function for ID generation
            params: Processing parameters
            batch_size: Size of batches to process

        Returns:
            Updated ProcessingResult
        """
        batch = []
        batch_size_counter = 0
        all_main_ids = []

        for record in data_iterator:
            # Skip non-dict records
            if not isinstance(record, dict):
                continue

            batch.append(record)
            batch_size_counter += 1

            if batch_size_counter >= batch_size:
                # Process main records for this batch
                main_ids = process_batch_main_records(
                    self,
                    batch,
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
                all_main_ids.extend(main_ids)

                # Process arrays for the batch
                process_batch_arrays(
                    self,
                    batch,
                    entity_name,
                    extract_time,
                    result,
                    id_field,
                    main_ids,
                    default_id_field,
                    id_generation_strategy,
                    params,
                )

                # Reset batch
                batch = []
                batch_size_counter = 0

        # Process remaining records if any
        if batch:
            # Process main records for final batch
            main_ids = process_batch_main_records(
                self,
                batch,
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
            all_main_ids.extend(main_ids)

            # Process arrays for the final batch
            process_batch_arrays(
                self,
                batch,
                entity_name,
                extract_time,
                result,
                id_field,
                main_ids,
                default_id_field,
                id_generation_strategy,
                params,
            )

        return result
