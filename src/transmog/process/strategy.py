"""Processing strategies for Transmog.

This module provides various strategies for processing data
with different memory/performance tradeoffs.
"""

import json
import os
import uuid
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterator
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
from ..core.flattener import flatten_json
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
from .result import ProcessingResult
from .utils import handle_file_error

T = TypeVar("T")


class ProcessingStrategy(ABC):
    """Base abstract class for processing strategies."""

    def __init__(self, config: TransmogConfig):
        """Initialize the strategy with configuration.

        Args:
            config: Configuration for processing
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
        """Process data according to the strategy.

        Args:
            data: Input data in a format appropriate for the strategy
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            **kwargs: Additional strategy-specific parameters

        Returns:
            ProcessingResult object containing processed data
        """
        pass

    def _get_common_config_params(
        self, extract_time: Optional[Any] = None
    ) -> dict[str, Any]:
        """Get common configuration parameters for processing.

        Args:
            extract_time: Optional extraction timestamp

        Returns:
            Dictionary of common configuration parameters
        """
        # Default to current timestamp if not provided
        if extract_time is None:
            extract_time = get_current_timestamp()

        # Get configuration sections
        naming_config = self.config.naming
        processing_config = self.config.processing
        metadata_config = self.config.metadata
        error_config = self.config.error_handling

        # Combine parameters
        params = {
            # Naming parameters
            "separator": naming_config.separator,
            "abbreviate_table_names": naming_config.abbreviate_table_names,
            "abbreviate_field_names": naming_config.abbreviate_field_names,
            "max_table_component_length": naming_config.max_table_component_length,
            "max_field_component_length": naming_config.max_field_component_length,
            "preserve_root_component": naming_config.preserve_root_component,
            "preserve_leaf_component": naming_config.preserve_leaf_component,
            "custom_abbreviations": naming_config.custom_abbreviations,
            # Processing parameters
            "cast_to_string": processing_config.cast_to_string,
            "include_empty": processing_config.include_empty,
            "skip_null": processing_config.skip_null,
            "visit_arrays": processing_config.visit_arrays,
            # Metadata parameters
            "id_field": metadata_config.id_field,
            "parent_field": metadata_config.parent_field,
            "time_field": metadata_config.time_field,
            "extract_time": extract_time,
            "default_id_field": metadata_config.default_id_field,
            "id_generation_strategy": metadata_config.id_generation_strategy,
        }

        # Apply recovery strategy if configured
        if error_config.recovery_strategy:
            from ..error.recovery import (
                DEFAULT,
                LENIENT,
                STRICT,
                PartialProcessingRecovery,
                SkipAndLogRecovery,
                StrictRecovery,
            )

            # Select appropriate recovery strategy
            strategy_name = error_config.recovery_strategy
            recovery_strategy: Optional[
                Union[StrictRecovery, SkipAndLogRecovery, PartialProcessingRecovery]
            ] = None

            if strategy_name == "strict":
                recovery_strategy = STRICT
            elif strategy_name == "skip":
                recovery_strategy = DEFAULT
            elif strategy_name == "partial":
                recovery_strategy = LENIENT
            elif isinstance(
                strategy_name,
                (StrictRecovery, SkipAndLogRecovery, PartialProcessingRecovery),
            ):
                recovery_strategy = strategy_name

            # Add valid strategy to params
            if recovery_strategy:
                params["recovery_strategy"] = recovery_strategy

        return params

    def _get_batch_size(self, chunk_size: Optional[int] = None) -> int:
        """Determine batch size for processing.

        Args:
            chunk_size: Optional override for batch size

        Returns:
            Batch size to use
        """
        if chunk_size is not None and chunk_size > 0:
            return chunk_size
        return self.config.processing.batch_size

    def _get_common_parameters(self, **kwargs: Any) -> dict[str, Any]:
        """Extract common parameters from kwargs or config.

        This method combines user-provided parameters with defaults from config.

        Args:
            **kwargs: User-provided parameters that override defaults

        Returns:
            Dictionary of parameters for processing
        """
        # Get config sections
        naming_config = self.config.naming
        processing_config = self.config.processing
        metadata_config = self.config.metadata

        # Combine parameters from config and kwargs
        params = {
            # Naming parameters
            "separator": kwargs.get("separator", naming_config.separator),
            "abbreviate_table_names": kwargs.get(
                "abbreviate_table_names", naming_config.abbreviate_table_names
            ),
            "abbreviate_field_names": kwargs.get(
                "abbreviate_field_names", naming_config.abbreviate_field_names
            ),
            "max_table_component_length": kwargs.get(
                "max_table_component_length", naming_config.max_table_component_length
            ),
            "max_field_component_length": kwargs.get(
                "max_field_component_length", naming_config.max_field_component_length
            ),
            "preserve_root_component": kwargs.get(
                "preserve_root_component", naming_config.preserve_root_component
            ),
            "preserve_leaf_component": kwargs.get(
                "preserve_leaf_component", naming_config.preserve_leaf_component
            ),
            "custom_abbreviations": kwargs.get(
                "custom_abbreviations", naming_config.custom_abbreviations
            ),
            # Processing parameters
            "cast_to_string": kwargs.get(
                "cast_to_string", processing_config.cast_to_string
            ),
            "include_empty": kwargs.get(
                "include_empty", processing_config.include_empty
            ),
            "skip_null": kwargs.get("skip_null", processing_config.skip_null),
            "max_depth": kwargs.get("max_depth", processing_config.max_nesting_depth),
            "visit_arrays": kwargs.get("visit_arrays", processing_config.visit_arrays),
            # Metadata parameters
            "id_field": kwargs.get("id_field", metadata_config.id_field),
            "parent_field": kwargs.get("parent_field", metadata_config.parent_field),
            "time_field": kwargs.get("time_field", metadata_config.time_field),
            "default_id_field": kwargs.get(
                "default_id_field", metadata_config.default_id_field
            ),
            "id_generation_strategy": kwargs.get(
                "id_generation_strategy", metadata_config.id_generation_strategy
            ),
        }

        return params


class InMemoryStrategy(ProcessingStrategy):
    """Strategy for processing in-memory data structures."""

    @error_context("Failed to process data", log_exceptions=True)
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

        # Remove from params to avoid duplicate argument
        params_copy = params.copy()
        if "recovery_strategy" in params_copy:
            del params_copy["recovery_strategy"]

        # Process all records in a single pass
        main_records, child_tables = process_records_in_single_pass(
            records=data_list,
            entity_name=entity_name,
            recovery_strategy=recovery_strategy,
            **params_copy,
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

        NOTE: This method does NOT return ProcessingResult. It returns a list of IDs
        that are needed for processing child records. It updates the result object
        in-place. Callers should NOT expect this to return a ProcessingResult.
        """
        if not params:
            params = {}

        main_ids = []
        for record in batch:
            # Add metadata and track
            source_field_str = None
            if isinstance(default_id_field, str):
                source_field_str = default_id_field

            annotated = annotate_with_metadata(
                record,
                parent_id=None,
                extract_time=extract_time,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                source_field=source_field_str,
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
        # Process arrays for each record in batch
        for i, record in enumerate(batch):
            if i >= len(main_ids):
                # Shouldn't happen, but safety check
                continue

            # Extract arrays for this record
            arrays = extract_arrays(
                record,
                parent_id=main_ids[i],
                entity_name=entity_name,
                separator=params["separator"],
                cast_to_string=params["cast_to_string"],
                include_empty=params["include_empty"],
                skip_null=params["skip_null"],
                extract_time=extract_time,
                abbreviate_enabled=params["abbreviate_table_names"],
                max_component_length=params["max_table_component_length"],
                preserve_leaf=params["preserve_leaf_component"],
                custom_abbreviations=params["custom_abbreviations"],
                recovery_strategy=params.get("recovery_strategy"),
            )

            # Add arrays to result for this record - ensure arrays is a dict
            if isinstance(arrays, dict):
                for table_name, records in arrays.items():
                    for child in records:
                        result.add_child_record(table_name, child)


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
            # Add metadata and track
            source_field_str = None
            if isinstance(default_id_field, str):
                source_field_str = default_id_field

            annotated = annotate_with_metadata(
                record,
                parent_id=None,
                extract_time=extract_time,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                source_field=source_field_str,
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
        # Process arrays for each record in batch
        for i, record in enumerate(batch):
            if i >= len(main_ids):
                # Shouldn't happen, but safety check
                continue

            # Extract arrays for this record
            arrays = extract_arrays(
                record,
                parent_id=main_ids[i],
                entity_name=entity_name,
                separator=params["separator"],
                cast_to_string=params["cast_to_string"],
                include_empty=params["include_empty"],
                skip_null=params["skip_null"],
                extract_time=extract_time,
                abbreviate_enabled=params["abbreviate_table_names"],
                max_component_length=params["max_table_component_length"],
                preserve_leaf=params["preserve_leaf_component"],
                custom_abbreviations=params["custom_abbreviations"],
                recovery_strategy=params.get("recovery_strategy"),
            )

            # Add arrays to result for this record - ensure arrays is a dict
            if isinstance(arrays, dict):
                for table_name, records in arrays.items():
                    for child in records:
                        result.add_child_record(table_name, child)


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
        """Process a chunk of data.

        Args:
            chunk: Chunk of data to process
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

        Returns:
            Updated ProcessingResult
        """
        main_ids = []

        # Process main records first
        for record in chunk:
            try:
                # Skip non-dict records
                if not isinstance(record, dict):
                    continue

                # Generate ID
                if id_generation_strategy:
                    record_id = id_generation_strategy(record)
                elif default_id_field:
                    if isinstance(default_id_field, dict):
                        id_source = default_id_field.get(entity_name)
                        record_id = (
                            str(record.get(id_source, ""))
                            if id_source
                            else str(uuid.uuid4())
                        )
                    else:
                        record_id = str(record.get(default_id_field, ""))
                else:
                    record_id = str(uuid.uuid4())

                # Store ID for array processing
                main_ids.append(record_id)

                # Add metadata
                record[id_field] = record_id
                record[parent_field] = None  # Main records have no parent
                record[time_field] = extract_time

                # Flatten
                flattened = flatten_json(
                    record,
                    separator=params.get("separator", "_"),
                    cast_to_string=params.get("cast_to_string", True),
                    include_empty=params.get("include_empty", False),
                    skip_null=params.get("skip_null", True),
                    skip_arrays=True,
                    abbreviate_field_names=params.get("abbreviate_field_names", False),
                    max_field_component_length=params.get("max_field_component_length"),
                    preserve_root_component=params.get("preserve_root_component", True),
                    preserve_leaf_component=params.get("preserve_leaf_component", True),
                    custom_abbreviations=params.get("custom_abbreviations"),
                    recovery_strategy=params.get("recovery_strategy"),
                )

                # Add to result - ensure flattened is not None
                if flattened is not None:
                    result.add_main_record(flattened)
            except Exception as e:
                # Log and continue with other records
                logger.warning(f"Error processing record: {str(e)}")

        # Process arrays if enabled
        if params.get("visit_arrays", True):
            self._process_batch_arrays(
                chunk,
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
        parent_field = params.get("parent_field", "__parent_extract_id")
        time_field = params.get("time_field", "__extract_datetime")

        # Only process arrays if enabled
        if not visit_arrays:
            return

        # Process arrays for each record in the batch
        for record, parent_id in zip(batch, main_ids):
            # Skip if we don't have a valid parent ID
            if parent_id is None:
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
                abbreviate_table_names=params.get("abbreviate_table_names", True),
                abbreviate_field_names=params.get("abbreviate_field_names", False),
                max_table_component_length=params.get("max_table_component_length"),
                max_field_component_length=params.get("max_field_component_length"),
                preserve_leaf_component=params.get("preserve_leaf_component", True),
                custom_abbreviations=params.get("custom_abbreviations"),
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                visit_arrays=visit_arrays,
                recovery_strategy=params.get("recovery_strategy"),
                streaming=False,
            )

            # Add arrays to result for this record - ensure arrays is a dict
            if isinstance(arrays, dict):
                for table_name, records in arrays.items():
                    for child in records:
                        result.add_child_record(table_name, child)


class CSVStrategy(ProcessingStrategy):
    """Strategy for processing CSV files."""

    @error_context("Failed to process CSV file", log_exceptions=True)
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
            records: List of records to process
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
        # Handle column name sanitization
        if sanitize_column_names and records:
            from ..naming.conventions import sanitize_name

            separator = params.get("separator", "_")

            # Create a mapping of original to sanitized column names
            col_mapping = {}
            for record in records:
                for col in list(record.keys()):
                    if col not in col_mapping:
                        sanitized = sanitize_name(col, separator, "_")
                        col_mapping[col] = sanitized

            # Update records with sanitized column names
            sanitized_records = []
            for record in records:
                sanitized_record = {}
                for col, value in record.items():
                    sanitized_record[col_mapping.get(col, col)] = value
                sanitized_records.append(sanitized_record)

            records = sanitized_records

        # Apply cast_to_string if enabled
        cast_to_string = params.get("cast_to_string", True)
        if cast_to_string:
            for record in records:
                for col, value in list(record.items()):
                    if value is not None and not isinstance(value, str):
                        if isinstance(value, bool):
                            record[col] = "true" if value else "false"
                        else:
                            record[col] = str(value)

        # Process flattened records
        for record in records:
            # Add metadata
            source_field_str = None
            if isinstance(default_id_field, str):
                source_field_str = default_id_field

            annotated = annotate_with_metadata(
                record,
                parent_id=None,
                extract_time=extract_time,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                source_field=source_field_str,
                in_place=True,
            )

            # Add to main table
            result.add_main_record(annotated)

        # Explicitly return the ProcessingResult object to help type checking
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
