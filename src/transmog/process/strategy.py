"""Processing strategies for Transmog.

This module provides various strategies for processing data
with different memory/performance tradeoffs.
"""

import json
import os
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterator
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
)

import orjson

from ..config import (
    TransmogConfig,
)
from ..core.extractor import extract_arrays
from ..core.hierarchy import (
    process_records_in_single_pass,
)
from ..core.metadata import (
    annotate_with_metadata,
    get_current_timestamp,
)
from ..error import (
    ConfigurationError,
    FileError,
    error_context,
    logger,
)
from ..naming.conventions import sanitize_name
from .result import ProcessingResult
from .utils import handle_file_error

T = TypeVar("T")


class ProcessingStrategy(ABC):
    """Abstract base class for processing strategies."""

    def __init__(self, config: TransmogConfig):
        """Initialize with configuration.

        Args:
            config: Processing configuration
        """
        self.config = config

    @abstractmethod
    def process(
        self,
        data: Any,
        entity_name: str,
        extract_time: Optional[Any] = None,
        **kwargs: Any,
    ) -> ProcessingResult:
        """Process the data.

        Args:
            data: Data to process
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            **kwargs: Additional parameters

        Returns:
            ProcessingResult containing processed data
        """
        pass

    def _get_common_config_params(
        self, extract_time: Optional[Any] = None
    ) -> dict[str, Any]:
        """Get common configuration parameters.

        Args:
            extract_time: Optional extraction timestamp

        Returns:
            Dictionary of common parameters
        """
        # Extract extraction time
        if extract_time is None:
            extract_time = datetime.now(timezone.utc)

        # Get naming parameters
        naming_config = getattr(self.config, "naming", None)
        if naming_config:
            separator = naming_config.separator
            deeply_nested_threshold = naming_config.deeply_nested_threshold
        else:
            separator = "_"
            deeply_nested_threshold = 4

        # Get processing parameters
        processing_config = getattr(self.config, "processing", None)
        if processing_config:
            cast_to_string = processing_config.cast_to_string
            include_empty = processing_config.include_empty
            skip_null = processing_config.skip_null
            path_parts_optimization = processing_config.path_parts_optimization
            visit_arrays = processing_config.visit_arrays
            keep_arrays = processing_config.keep_arrays
            max_depth = processing_config.max_depth
        else:
            cast_to_string = True
            include_empty = False
            skip_null = True
            path_parts_optimization = True
            visit_arrays = True
            keep_arrays = False
            max_depth = 100

        # Get metadata parameters
        metadata_config = getattr(self.config, "metadata", None)
        if metadata_config:
            id_field = metadata_config.id_field
            parent_field = metadata_config.parent_field
            time_field = metadata_config.time_field
            default_id_field = metadata_config.default_id_field
            id_generation_strategy = metadata_config.id_generation_strategy
        else:
            id_field = "__extract_id"
            parent_field = "__parent_extract_id"
            time_field = "__extract_datetime"
            default_id_field = None
            id_generation_strategy = None

        # Get error handling parameters
        error_config = getattr(self.config, "error_handling", None)
        if error_config:
            recovery_strategy = error_config.recovery_strategy
        else:
            recovery_strategy = "strict"

        # Return consolidated parameters
        return {
            "separator": separator,
            "deeply_nested_threshold": deeply_nested_threshold,
            "cast_to_string": cast_to_string,
            "include_empty": include_empty,
            "skip_null": skip_null,
            "path_parts_optimization": path_parts_optimization,
            "visit_arrays": visit_arrays,
            "keep_arrays": keep_arrays,
            "id_field": id_field,
            "parent_field": parent_field,
            "time_field": time_field,
            "default_id_field": default_id_field,
            "id_generation_strategy": id_generation_strategy,
            "extract_time": extract_time,
            "recovery_strategy": recovery_strategy,
            "max_depth": max_depth,
        }

    def _get_batch_size(self, chunk_size: Optional[int] = None) -> int:
        """Get batch size for processing.

        Args:
            chunk_size: Optional override for batch size

        Returns:
            Batch size to use
        """
        # Use provided chunk size if specified
        if chunk_size is not None:
            return int(chunk_size)

        # Otherwise use config batch size
        processing_config = getattr(self.config, "processing", None)
        if processing_config and hasattr(processing_config, "batch_size"):
            return processing_config.batch_size
        return 1000  # Default batch size

    def _get_common_parameters(self, **kwargs: Any) -> dict[str, Any]:
        """Get common parameters for processing.

        Args:
            **kwargs: Override parameters

        Returns:
            Dictionary of parameters
        """
        # Get parameters from config
        params = self._get_common_config_params(
            extract_time=kwargs.get("extract_time", None)
        )

        # Override with any provided parameters
        for key, value in kwargs.items():
            if key in params:
                params[key] = value

        return params

    def _remove_array_fields_from_record(self, record: dict[str, Any]) -> None:
        """Remove array and object fields from a record.

        Args:
            record: The record to remove array fields from
        """
        # Don't process None or non-dict records
        if record is None or not isinstance(record, dict):
            return

        # Identify keys to remove (complex types that would be extracted to
        # child tables)
        keys_to_remove = []
        for key, value in record.items():
            # Skip metadata fields which start with __
            if key.startswith("__"):
                continue

            # Remove lists and dictionaries that would be extracted to child tables
            if isinstance(value, (list, dict)):
                keys_to_remove.append(key)

        # Remove the identified keys
        for key in keys_to_remove:
            if key in record:  # Extra safety check
                del record[key]


class InMemoryStrategy(ProcessingStrategy):
    """Strategy for processing in-memory data structures."""

    @error_context("Failed to process data", log_exceptions=True)  # type: ignore
    def process(
        self,
        data: Any,
        entity_name: str,
        extract_time: Optional[Any] = None,
        **kwargs: Any,
    ) -> ProcessingResult:
        """Process in-memory data (dictionary or list of dictionaries).

        Args:
            data: Input data (dict or list of dicts)
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            **kwargs: Additional processing parameters

        Returns:
            ProcessingResult object containing processed data
        """
        # Get result from kwargs if it exists
        result = kwargs.pop("result", None)

        # Create a ProcessingResult if not provided
        if result is None:
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
            )

        # If data is a single dict, convert to list for uniform processing
        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            raise TypeError(
                f"Expected dict or list of dicts, got {type(data).__name__}"
            )

        # Get parameters from configuration and kwargs
        params = self._get_common_parameters(**kwargs)
        extract_time = extract_time or get_current_timestamp()

        # Process the data with parameters
        return self._process_in_memory(data_list, entity_name, params, result)

    def _process_in_memory(
        self,
        data_list: list[dict[str, Any]],
        entity_name: str,
        params: dict[str, Any],
        result: ProcessingResult,
    ) -> ProcessingResult:
        """Process a list of dictionaries in memory.

        Args:
            data_list: List of dictionaries to process
            entity_name: Name of the entity
            params: Processing parameters
            result: Existing result object or None

        Returns:
            ProcessingResult with processed data
        """
        # Extract recovery strategy if present
        recovery_strategy = params.get("recovery_strategy")
        keep_arrays = params.get("keep_arrays", False)

        # Get common parameters
        extract_time = params.get("extract_time")
        separator = params.get("separator", "_")
        cast_to_string = params.get("cast_to_string", True)
        include_empty = params.get("include_empty", False)
        skip_null = params.get("skip_null", True)
        id_field = params.get("id_field", "__extract_id")
        parent_field = params.get("parent_field", "__parent_extract_id")
        time_field = params.get("time_field", "__extract_datetime")
        visit_arrays = params.get("visit_arrays", True)
        deeply_nested_threshold = params.get("deeply_nested_threshold", 4)
        default_id_field = params.get("default_id_field")
        id_generation_strategy = params.get("id_generation_strategy")
        max_depth = params.get("max_depth", 100)

        # Process all records in a single pass
        main_records, child_tables = process_records_in_single_pass(
            records=data_list,
            entity_name=entity_name,
            extract_time=extract_time,
            separator=separator,
            cast_to_string=cast_to_string,
            include_empty=include_empty,
            skip_null=skip_null,
            id_field=id_field,
            parent_field=parent_field,
            time_field=time_field,
            visit_arrays=visit_arrays,
            deeply_nested_threshold=deeply_nested_threshold,
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            recovery_strategy=recovery_strategy,
            max_depth=max_depth,
            keep_arrays=keep_arrays,
        )

        # Update result
        for record in main_records:
            result.add_main_record(record)

        result.add_child_tables(child_tables)
        result.source_info["record_count"] = len(data_list)

        return result


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
                # This shouldn't typically happen as CSV files should be handled by
                # CSVStrategy, but we'll provide a fallback to avoid errors
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
        id_field = params.get("id_field", "__extract_id")
        parent_field = params.get("parent_field", "__parent_extract_id")
        time_field = params.get("time_field", "__extract_datetime")
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
                                record = json.loads(line)
                                if isinstance(record, dict):
                                    yield record
                                else:
                                    logger.warning(
                                        f"Skipping non-dict JSON in {file_path}: "
                                        f"{type(record).__name__}"
                                    )
                            except json.JSONDecodeError:
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

        # Create arrays for batch processing results
        main_ids = []

        for record in data_iterator:
            # Skip non-dict records
            if not isinstance(record, dict):
                continue

            batch.append(record)
            batch_size_counter += 1

            if batch_size_counter >= batch_size:
                # Process main records for this batch
                main_ids = self._process_batch_main_records(
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
                self._process_batch_arrays(
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
            main_ids = self._process_batch_main_records(
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
            self._process_batch_arrays(
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

    def _process_batch_main_records(
        self,
        batch: list[dict[str, Any]],
        entity_name: str,
        extract_time: Any,
        result: ProcessingResult,
        id_field: str,
        parent_field: str,
        time_field: str,
        default_id_field: Optional[Union[str, dict[str, str]]],
        id_generation_strategy: Optional[Callable[[dict[str, Any]], str]],
        params: dict[str, Any],
    ) -> list[str]:
        """Process a batch of main records and extract IDs.

        Args:
            batch: Batch of records to process
            entity_name: Name of the entity being processed
            extract_time: Extraction timestamp
            result: Result object to update
            id_field: ID field name
            parent_field: Parent ID field name
            time_field: Timestamp field name
            default_id_field: Field name or dict mapping paths to field names
                for deterministic IDs
            id_generation_strategy: Custom ID generation function
            params: Processing parameters

        Returns:
            List of extract IDs for the processed records
        """
        if not params:
            params = {}

        main_ids = []
        for record in batch:
            # Skip None or empty records
            if record is None or (isinstance(record, dict) and not record):
                continue

            # Resolve source field for deterministic ID
            source_field_str = None
            if default_id_field:
                if isinstance(default_id_field, str):
                    source_field_str = default_id_field
                elif isinstance(default_id_field, dict):
                    # First try root path (empty string)
                    if "" in default_id_field:
                        source_field_str = default_id_field[""]
                    # Then try wildcard match
                    elif "*" in default_id_field:
                        source_field_str = default_id_field["*"]
                    # Finally try entity name
                    elif entity_name in default_id_field:
                        source_field_str = default_id_field[entity_name]

            # Process the record
            main_record = {}
            if isinstance(record, dict):
                main_record = record.copy()

                # Add metadata with in-place optimization
                annotated = annotate_with_metadata(
                    main_record,
                    parent_id=None,
                    extract_time=extract_time,
                    id_field=id_field,
                    parent_field=parent_field,
                    time_field=time_field,
                    source_field=source_field_str,
                    id_generation_strategy=id_generation_strategy,
                    in_place=True,
                )

                # Add extract ID to track for child relationships
                if id_field in annotated:
                    main_ids.append(annotated[id_field])
                else:
                    # Fallback if ID field not present for some reason
                    main_ids.append(None)

                # Add to result's main table
                result.add_main_record(annotated)

        return main_ids

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
        """Process arrays for a batch of records efficiently."""
        # Get processing parameters
        visit_arrays = params.get("visit_arrays", True)
        keep_arrays = params.get("keep_arrays", False)

        # Only process arrays if enabled
        if not visit_arrays:
            logger.debug("Array processing disabled, skipping")
            return

        # Process arrays for each record in batch
        for i, record in enumerate(batch):
            if i >= len(main_ids):
                # Shouldn't happen, but safety check
                logger.warning(
                    f"Record index {i} exceeds main_ids length {len(main_ids)}"
                )
                continue

            # Skip null or empty records
            if record is None:
                logger.debug(f"Skipping null record at index {i}")
                continue

            # Get parent ID for this record
            parent_id = main_ids[i]
            if parent_id is None:
                logger.warning(
                    f"Null parent ID for record at index {i}, skipping arrays"
                )
                continue

            # Deep copy the record to prevent modification
            record_copy = record.copy() if isinstance(record, dict) else record

            # Extract arrays for this record
            arrays = extract_arrays(
                record_copy,
                parent_id=parent_id,
                entity_name=entity_name,
                separator=params["separator"],
                cast_to_string=params["cast_to_string"],
                include_empty=params["include_empty"],
                skip_null=params["skip_null"],
                extract_time=extract_time,
                deeply_nested_threshold=params.get("deeply_nested_threshold", 4),
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=params.get("recovery_strategy"),
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


class BatchStrategy(ProcessingStrategy):
    """Strategy for processing data in batches."""

    def process(
        self,
        data: Union[list[dict[str, Any]], Generator[dict[str, Any], None, None]],
        entity_name: str,
        extract_time: Optional[Any] = None,
        result: Optional[ProcessingResult] = None,
        **kwargs: Any,
    ) -> ProcessingResult:
        """Process data in batches.

        Args:
            data: List of dictionaries or data generator
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            result: Optional existing result to append to
            **kwargs: Additional parameters

        Returns:
            ProcessingResult containing the processed data
        """
        # Create result if not provided
        if result is None:
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
            )

        # Extract common parameters from kwargs or config
        params = self._get_common_parameters(**kwargs)

        # Get extraction timestamp
        extract_time = extract_time or get_current_timestamp()

        # Get batch size
        batch_size = self._get_batch_size(kwargs.get("chunk_size"))

        # Get id fields
        id_field = params.get("id_field", "__extract_id")
        parent_field = params.get("parent_field", "__parent_extract_id")
        time_field = params.get("time_field", "__extract_datetime")
        default_id_field = params.get("default_id_field")
        id_generation_strategy = params.get("id_generation_strategy")

        # Process data in batches
        batch: list[dict[str, Any]] = []
        for record in data:
            batch.append(record)

            # Process batch when it reaches the desired size
            if len(batch) >= batch_size:
                main_ids = self._process_batch_main_records(
                    batch=batch,
                    entity_name=entity_name,
                    extract_time=extract_time,
                    result=result,
                    id_field=id_field,
                    parent_field=parent_field,
                    time_field=time_field,
                    default_id_field=default_id_field,
                    id_generation_strategy=id_generation_strategy,
                    params=params,
                )

                # Process arrays for the batch
                self._process_batch_arrays(
                    batch=batch,
                    entity_name=entity_name,
                    extract_time=extract_time,
                    result=result,
                    id_field=id_field,
                    main_ids=main_ids,
                    default_id_field=default_id_field,
                    id_generation_strategy=id_generation_strategy,
                    params=params,
                )

                batch = []

        # Process any remaining records
        if batch:
            main_ids = self._process_batch_main_records(
                batch=batch,
                entity_name=entity_name,
                extract_time=extract_time,
                result=result,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                params=params,
            )

            # Process arrays for remaining records
            self._process_batch_arrays(
                batch=batch,
                entity_name=entity_name,
                extract_time=extract_time,
                result=result,
                id_field=id_field,
                main_ids=main_ids,
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                params=params,
            )

        return result

    def _process_batch_main_records(
        self,
        batch: list[dict[str, Any]],
        entity_name: str,
        extract_time: Any,
        result: ProcessingResult,
        id_field: str,
        parent_field: str,
        time_field: str,
        default_id_field: Optional[Union[str, dict[str, str]]],
        id_generation_strategy: Optional[Callable[[dict[str, Any]], str]],
        params: dict[str, Any],
    ) -> list[str]:
        """Process a batch of main records and extract IDs.

        Args:
            batch: Batch of records to process
            entity_name: Name of the entity being processed
            extract_time: Extraction timestamp
            result: Result object to update
            id_field: ID field name
            parent_field: Parent ID field name
            time_field: Timestamp field name
            default_id_field: Field name or dict mapping paths to field names
                for deterministic IDs
            id_generation_strategy: Custom ID generation function
            params: Processing parameters

        Returns:
            List of extract IDs for the processed records
        """
        if not params:
            params = {}

        main_ids = []
        for record in batch:
            # Skip None or empty records
            if record is None or (isinstance(record, dict) and not record):
                continue

            # Resolve source field for deterministic ID
            source_field_str = None
            if default_id_field:
                if isinstance(default_id_field, str):
                    source_field_str = default_id_field
                elif isinstance(default_id_field, dict):
                    # First try root path (empty string)
                    if "" in default_id_field:
                        source_field_str = default_id_field[""]
                    # Then try wildcard match
                    elif "*" in default_id_field:
                        source_field_str = default_id_field["*"]
                    # Finally try entity name
                    elif entity_name in default_id_field:
                        source_field_str = default_id_field[entity_name]

            # Process the record
            main_record = {}
            if isinstance(record, dict):
                main_record = record.copy()

                # Add metadata with in-place optimization
                annotated = annotate_with_metadata(
                    main_record,
                    parent_id=None,
                    extract_time=extract_time,
                    id_field=id_field,
                    parent_field=parent_field,
                    time_field=time_field,
                    source_field=source_field_str,
                    id_generation_strategy=id_generation_strategy,
                    in_place=True,
                )

                # Add extract ID to track for child relationships
                if id_field in annotated:
                    main_ids.append(annotated[id_field])
                else:
                    # Fallback if ID field not present for some reason
                    main_ids.append(None)

                # Add to result's main table
                result.add_main_record(annotated)

        return main_ids

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
        """Process arrays for a batch of records efficiently."""
        # Get processing parameters
        visit_arrays = params.get("visit_arrays", True)
        keep_arrays = params.get("keep_arrays", False)

        # Only process arrays if enabled
        if not visit_arrays:
            logger.debug("Array processing disabled, skipping")
            return

        # Process arrays for each record in batch
        for i, record in enumerate(batch):
            if i >= len(main_ids):
                # Shouldn't happen, but safety check
                logger.warning(
                    f"Record index {i} exceeds main_ids length {len(main_ids)}"
                )
                continue

            # Skip null or empty records
            if record is None:
                logger.debug(f"Skipping null record at index {i}")
                continue

            # Get parent ID for this record
            parent_id = main_ids[i]
            if parent_id is None:
                logger.warning(
                    f"Null parent ID for record at index {i}, skipping arrays"
                )
                continue

            # Deep copy the record to prevent modification
            record_copy = record.copy() if isinstance(record, dict) else record

            # Extract arrays for this record
            arrays = extract_arrays(
                record_copy,
                parent_id=parent_id,
                entity_name=entity_name,
                separator=params["separator"],
                cast_to_string=params["cast_to_string"],
                include_empty=params["include_empty"],
                skip_null=params["skip_null"],
                extract_time=extract_time,
                deeply_nested_threshold=params.get("deeply_nested_threshold", 4),
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=params.get("recovery_strategy"),
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
            # Use FileStrategy to properly process the file
            from . import FileStrategy

            file_strategy = FileStrategy(self.config)
            return file_strategy.process(
                data, entity_name=entity_name, extract_time=extract_time, result=result
            )

        # Check if the string input might be JSON
        if isinstance(data, str):
            try:
                # Try to parse as JSON
                if data.strip().startswith(("{", "[")):
                    parsed_data = json.loads(data)
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
            except json.JSONDecodeError:
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
        id_field = params.get("id_field", "__extract_id")
        parent_field = params.get("parent_field", "__parent_extract_id")
        time_field = params.get("time_field", "__extract_datetime")
        default_id_field = params.get("default_id_field")
        id_generation_strategy = params.get("id_generation_strategy")

        # Process data in chunks
        accumulated_chunk: list[dict[str, Any]] = []

        # Iterate through records
        for record in data:
            # Skip any non-dict records
            if not isinstance(record, dict):
                continue

            # Add to current chunk
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
            extract_time=extract_time,
            separator=params.get("separator", "_"),
            cast_to_string=params.get("cast_to_string", True),
            include_empty=params.get("include_empty", False),
            skip_null=params.get("skip_null", True),
            id_field=id_field,
            parent_field=parent_field,
            time_field=time_field,
            visit_arrays=params.get("visit_arrays", True),
            deeply_nested_threshold=params.get("deeply_nested_threshold", 4),
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            recovery_strategy=params.get("recovery_strategy"),
            max_depth=params.get("max_depth", 100),
            keep_arrays=params.get("keep_arrays", False),
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
        from ..core.hierarchy import process_structure

        # Ensure we have valid parameters
        if len(batch) != len(main_ids):
            logger.warning(
                f"Batch size mismatch: records={len(batch)}, ids={len(main_ids)}"
            )
            # Use only as many IDs as we have records
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
                extract_time=extract_time,
                deeply_nested_threshold=params.get("deeply_nested_threshold", 4),
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=params.get("recovery_strategy"),
                keep_arrays=keep_arrays,
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


class CSVStrategy(ProcessingStrategy):
    """Strategy for processing CSV files."""

    @error_context("Failed to process CSV file", log_exceptions=True)  # type: ignore
    def process(
        self,
        data: Any,
        entity_name: str,
        extract_time: Optional[Any] = None,
        **kwargs: Any,
    ) -> ProcessingResult:
        """Process a CSV file.

        Args:
            data: Path to CSV file or file-like object
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            **kwargs: Additional parameters including:
                - result: Optional result object to add results to
                - delimiter: CSV delimiter character
                - has_header: Whether the CSV has a header row
                - null_values: List of strings to interpret as null values
                - sanitize_column_names: Whether to sanitize column names
                - infer_types: Whether to infer data types from values
                - skip_rows: Number of rows to skip at beginning of file
                - quote_char: Quote character for CSV parsing
                - encoding: File encoding
                - chunk_size: Size of processing chunks
                - date_format: Date format for date columns

        Returns:
            ProcessingResult containing processed data
        """
        # Extract parameters from kwargs
        result = kwargs.pop("result", None)
        delimiter = kwargs.pop("delimiter", None)
        has_header = kwargs.pop("has_header", True)
        null_values = kwargs.pop("null_values", None)
        sanitize_column_names = kwargs.pop("sanitize_column_names", True)
        infer_types = kwargs.pop("infer_types", True)
        skip_rows = kwargs.pop("skip_rows", 0)
        quote_char = kwargs.pop("quote_char", None)
        encoding = kwargs.pop("encoding", "utf-8")
        chunk_size = kwargs.pop("chunk_size", None)
        date_format = kwargs.pop("date_format", None)

        # Convert data to file path
        if not isinstance(data, str):
            raise TypeError(f"Expected string file path, got {type(data).__name__}")

        file_path = data

        # Create result if not provided
        if result is None:
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
            )
            result.source_info["file_path"] = file_path

        # Get extraction timestamp
        extract_time = extract_time or get_current_timestamp()

        # Extract common parameters from kwargs or config
        params = self._get_common_parameters(**kwargs)

        # Set delimiter if not provided
        delimiter = delimiter or ","

        # Get batch size
        batch_size = self._get_batch_size(chunk_size)

        try:
            # Check file exists
            if not os.path.exists(file_path):
                raise FileError(f"File not found: {file_path}")

            # Process the CSV file in chunks if needed
            from ..io.readers.csv import CSVReader

            # Create CSV reader
            csv_reader = CSVReader(
                delimiter=delimiter,
                has_header=has_header,
                null_values=null_values,
                skip_rows=skip_rows,
                quote_char=quote_char,
                encoding=encoding,
                sanitize_column_names=sanitize_column_names,
                infer_types=infer_types,
                cast_to_string=self.config.processing.cast_to_string,
                date_format=date_format,
            ).read_records(file_path)

            # Get ID fields
            id_field = params.get("id_field", "__extract_id")
            parent_field = params.get("parent_field", "__parent_extract_id")
            time_field = params.get("time_field", "__extract_datetime")
            default_id_field = params.get("default_id_field")
            id_generation_strategy = params.get("id_generation_strategy")

            # Process in batches
            batch: list[dict[str, Any]] = []
            for record in csv_reader:
                # Add to batch
                batch.append(record)

                # Process when batch is full
                if len(batch) >= batch_size:
                    # Process batch
                    self._process_csv_chunk(
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
                        sanitize_column_names,
                    )
                    # Reset batch
                    batch = []

            # Process remaining records
            if batch:
                self._process_csv_chunk(
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
                    sanitize_column_names,
                )

            return result
        except Exception as e:
            # Handle file errors
            handle_file_error(file_path, e, "CSV")
            # This line is never reached because handle_file_error always raises
            # But we include it to satisfy type checking
            return result

    def _process_csv_chunk(
        self,
        records: list[dict[str, Any]],
        entity_name: str,
        extract_time: Any,
        result: ProcessingResult,
        id_field: str,
        parent_field: str,
        time_field: str,
        default_id_field: Optional[Union[str, dict[str, str]]],
        id_generation_strategy: Optional[Callable[[dict[str, Any]], str]],
        params: dict[str, Any],
        sanitize_column_names: bool,
    ) -> ProcessingResult:
        """Process a chunk of CSV records.

        Args:
            records: Chunk of records to process
            entity_name: Name of the entity being processed
            extract_time: Extraction timestamp
            result: Result object to update
            id_field: ID field name
            parent_field: Parent ID field name
            time_field: Timestamp field name
            default_id_field: Field name or dict mapping paths to field names
                for deterministic IDs
            id_generation_strategy: Custom function for ID generation
            params: Processing parameters
            sanitize_column_names: Whether to sanitize column names

        Returns:
            Updated ProcessingResult
        """
        # Skip if no records
        if not records:
            return result

        # Resolve source field for deterministic ID
        source_field_str = None
        if default_id_field:
            if isinstance(default_id_field, str):
                source_field_str = default_id_field
            elif isinstance(default_id_field, dict):
                # First try root path (empty string)
                if "" in default_id_field:
                    source_field_str = default_id_field[""]
                # Then try wildcard match
                elif "*" in default_id_field:
                    source_field_str = default_id_field["*"]
                # Finally try entity name
                elif entity_name in default_id_field:
                    source_field_str = default_id_field[entity_name]

        # Process each record
        for record in records:
            try:
                # Skip empty records
                if record is None or (isinstance(record, dict) and not record):
                    continue

                # Apply data type inference if needed
                if params.get("infer_types", False):
                    record = self._infer_record_types(record)

                # Sanitize column names if requested
                if sanitize_column_names:
                    sanitized_record = {}
                    for key, value in record.items():
                        sanitized_key = sanitize_name(key, params.get("separator", "_"))
                        sanitized_record[sanitized_key] = value
                    record = sanitized_record

                # Add metadata
                annotated = annotate_with_metadata(
                    record,
                    parent_id=None,
                    extract_time=extract_time,
                    id_field=id_field,
                    parent_field=parent_field,
                    time_field=time_field,
                    source_field=source_field_str,
                    id_generation_strategy=id_generation_strategy,
                    in_place=False,
                )

                # Add to result
                result.add_main_record(annotated)
            except Exception as e:
                logger.warning(f"Error processing CSV record: {str(e)}")
                # Skip the problematic record based on recovery strategy
                if params.get("recovery_strategy") != "skip":
                    raise

        return result

    def _infer_record_types(self, record: dict[str, Any]) -> dict[str, Any]:
        """Infer data types in a CSV record.

        Args:
            record: Dictionary representing a CSV row

        Returns:
            Dictionary with inferred data types
        """
        result: dict[str, Any] = {}
        for key, value in record.items():
            if value is None or value == "":
                result[key] = None
                continue

            # Handle non-string values
            if not isinstance(value, str):
                result[key] = value
                continue

            # Try converting to different types
            # Try as integer
            try:
                int_val = int(value)
                if str(int_val) == value:  # Ensure no information loss
                    result[key] = int_val
                    continue
            except (ValueError, TypeError):
                pass

            # Try as float
            try:
                float_val = float(value)
                result[key] = float_val
                continue
            except (ValueError, TypeError):
                pass

            # Try as boolean
            if value.lower() in ("true", "yes", "1", "t", "y"):
                result[key] = True
                continue
            elif value.lower() in ("false", "no", "0", "f", "n"):
                result[key] = False
                continue

            # Keep as string
            result[key] = value

        return result
