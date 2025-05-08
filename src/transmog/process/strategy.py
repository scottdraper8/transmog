"""
Processing strategies for Transmog.

This module provides various strategies for processing data
with different memory/performance tradeoffs.
"""

from abc import ABC, abstractmethod
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    Callable,
    Iterator,
    BinaryIO,
    cast,
    Set,
    TypeVar,
    TextIO,
    Generic,
    Protocol,
)
import json
import os
import itertools
import logging
from datetime import datetime

from ..error import (
    ConfigurationError,
    FileError,
    ParsingError,
    ProcessingError,
    ValidationError,
    error_context,
    with_recovery,
    logger,
    safe_json_loads,
    validate_input,
)
from ..config import (
    TransmogConfig,
    ProcessingMode,
)
from ..core.hierarchy import (
    process_structure,
    process_records_in_single_pass,
    stream_process_records,
)
from ..core.metadata import (
    generate_extract_id,
    get_current_timestamp,
    create_batch_metadata,
    annotate_with_metadata,
)
from .result import ProcessingResult, ConversionMode
from .data_iterators import get_data_iterator
from .utils import get_common_config_params, get_batch_size
from ..core.flattener import flatten_json
from ..core.extractor import extract_arrays

T = TypeVar("T")


class ProcessingStrategy(ABC):
    """Base abstract class for processing strategies."""

    def __init__(self, config: TransmogConfig):
        """
        Initialize the strategy with configuration.

        Args:
            config: Configuration for processing
        """
        self.config = config

    @abstractmethod
    def process(
        self, data: Any, entity_name: str, extract_time: Optional[Any] = None, **kwargs
    ) -> ProcessingResult:
        """
        Process data according to the strategy.

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
    ) -> Dict[str, Any]:
        """
        Get common configuration parameters for processing.

        Args:
            extract_time: Optional extraction timestamp

        Returns:
            Dictionary of common configuration parameters
        """
        # Use current timestamp if not provided
        if extract_time is None:
            extract_time = get_current_timestamp()

        # Get configuration parameters
        naming_config = self.config.naming
        processing_config = self.config.processing
        metadata_config = self.config.metadata
        error_config = self.config.error_handling

        # Create parameters dictionary
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

        # Add recovery strategy if configured
        if error_config.recovery_strategy:
            from ..error.recovery import (
                STRICT,
                DEFAULT,
                LENIENT,
                StrictRecovery,
                SkipAndLogRecovery,
                PartialProcessingRecovery,
            )

            # Create the recovery strategy instance
            strategy_name = error_config.recovery_strategy
            recovery_strategy = None

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

            # Add to params if we have a valid strategy
            if recovery_strategy:
                params["recovery_strategy"] = recovery_strategy

        return params

    def _get_batch_size(self, chunk_size: Optional[int] = None) -> int:
        """
        Determine batch size for processing.

        Args:
            chunk_size: Optional override for batch size

        Returns:
            Batch size to use
        """
        if chunk_size is not None and chunk_size > 0:
            return chunk_size
        return self.config.processing.batch_size

    def _get_common_parameters(self, **kwargs):
        """
        Extract common parameters from kwargs or config.

        This method combines user-provided parameters with defaults from config.

        Args:
            **kwargs: User-provided parameters that override defaults

        Returns:
            Dictionary of parameters for processing
        """
        # Get config objects
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
            "preserve_root": kwargs.get(
                "preserve_root_component", naming_config.preserve_root_component
            ),
            "preserve_leaf": kwargs.get(
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
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        entity_name: str,
        extract_time: Optional[Any] = None,
        **kwargs,
    ) -> ProcessingResult:
        """
        Process in-memory data (dictionary or list of dictionaries).

        Args:
            data: Input data (dict or list of dicts)
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            **kwargs: Additional parameters

        Returns:
            ProcessingResult containing processed data
        """
        # Convert single object to list for consistent handling
        if isinstance(data, dict):
            data_list = [data]
        else:
            data_list = data

        # Validate input data
        validate_input(
            data_list, expected_type=list, param_name="data", allow_none=False
        )

        # Get configuration parameters
        params = self._get_common_config_params(extract_time)
        params.update({k: v for k, v in kwargs.items() if v is not None})

        # Get batch size from configuration or override
        batch_size = self._get_batch_size(kwargs.get("chunk_size"))

        # Extract recovery strategy if present
        recovery_strategy = params.get("recovery_strategy")

        # Remove from params to avoid duplicate argument
        params_copy = params.copy()
        if "recovery_strategy" in params_copy:
            del params_copy["recovery_strategy"]

        # Handle small data sets in one go
        if len(data_list) <= batch_size:
            # Process all records in a single pass
            main_records, child_tables = process_records_in_single_pass(
                records=data_list,
                entity_name=entity_name,
                recovery_strategy=recovery_strategy,
                **params_copy,
            )

            return ProcessingResult(
                main_table=main_records,
                child_tables=child_tables,
                entity_name=entity_name,
                source_info={"record_count": len(data_list)},
            )

        # For larger datasets, process in batches
        data_iterator = iter(data_list)
        total_records = 0
        result = None

        # Process data in chunks
        while True:
            # Get next batch
            batch = list(itertools.islice(data_iterator, batch_size))
            if not batch:
                break

            # Process batch
            main_records, child_tables = process_records_in_single_pass(
                records=batch,
                entity_name=entity_name,
                recovery_strategy=recovery_strategy,
                **params_copy,
            )

            # Update record count
            total_records += len(batch)

            # Initialize or update result
            if result is None:
                result = ProcessingResult(
                    main_table=main_records,
                    child_tables=child_tables,
                    entity_name=entity_name,
                )
            else:
                # Add main records
                result.add_main_records(main_records)

                # Add child records
                for table_name, records in child_tables.items():
                    result.add_records(table_name, records)

        # Update source info
        if result:
            result.source_info["record_count"] = total_records
            return result

        # Handle empty data case
        return ProcessingResult(
            main_table=[],
            child_tables={},
            entity_name=entity_name,
            source_info={"record_count": 0},
        )


class FileStrategy(ProcessingStrategy):
    """Strategy for processing files."""

    def process(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
        result: Optional[ProcessingResult] = None,
        **kwargs,
    ) -> ProcessingResult:
        """
        Process a file.

        Args:
            file_path: Path to the file to process
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            result: Optional existing result to append to
            **kwargs: Additional parameters

        Returns:
            ProcessingResult containing processed data
        """
        # Check file exists
        if not isinstance(file_path, str) or not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

        # Extract common parameters
        params = self._get_common_parameters(**kwargs)
        extract_time = extract_time or get_current_timestamp()

        # Initialize result object
        result = result or ProcessingResult(
            main_table=[], child_tables={}, entity_name=entity_name
        )

        # Get batch size
        batch_size = get_batch_size(self.config, kwargs)

        # Detect file format and get an appropriate data iterator
        data_iterator = get_data_iterator(self, file_path, "auto")

        # Get unique ID parameters
        id_field = params.get("id_field", self.config.metadata.id_field)
        parent_field = params.get("parent_field", self.config.metadata.parent_field)
        time_field = params.get("time_field", self.config.metadata.time_field)
        default_id_field = params.get(
            "default_id_field", self.config.metadata.default_id_field
        )
        id_generation_strategy = params.get(
            "id_generation_strategy", self.config.metadata.id_generation_strategy
        )

        # Process data in batches
        self._process_data_batches(
            data_iterator,
            entity_name,
            extract_time,
            result,
            id_field,
            parent_field,
            time_field,
            default_id_field,
            id_generation_strategy,
            params,
            batch_size,
        )

        return result

    def _process_json_file(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """
        Process a JSON file.

        Args:
            file_path: Path to the JSON file
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp

        Returns:
            ProcessingResult containing processed data
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Handle both single objects and arrays
            if isinstance(data, dict):
                data_list = [data]
            elif isinstance(data, list):
                data_list = data
            else:
                raise ParsingError(f"Invalid JSON data in {file_path}")

            # Create in-memory strategy for further processing
            in_memory_strategy = InMemoryStrategy(self.config)

            # Process using in-memory strategy
            return in_memory_strategy.process(data_list, entity_name, extract_time)

        except json.JSONDecodeError as e:
            raise ParsingError(f"Failed to parse JSON file {file_path}: {str(e)}")
        except IOError as e:
            raise FileError(f"Failed to read file {file_path}: {str(e)}")

    def _process_jsonl_file(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
        chunk_size: Optional[int] = None,
    ) -> ProcessingResult:
        """
        Process a JSONL file (line-delimited JSON).

        Args:
            file_path: Path to the JSONL file
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            chunk_size: Size of chunks to process

        Returns:
            ProcessingResult containing processed data
        """
        # Get batch size from configuration or override
        batch_size = self._get_batch_size(chunk_size)

        try:
            # Create JSONL iterator
            def jsonl_iterator():
                with open(file_path, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue  # Skip empty lines and comments
                        try:
                            yield safe_json_loads(line)
                        except json.JSONDecodeError as e:
                            logger.warning(
                                f"Error parsing line {line_num} in {file_path}: {str(e)}"
                            )
                            # Skip invalid lines rather than failing the whole process
                            continue

            # Create in-memory strategy for further processing
            in_memory_strategy = InMemoryStrategy(self.config)

            # Process using in-memory strategy with chunking
            return in_memory_strategy._process_in_chunks(
                jsonl_iterator(), entity_name, extract_time, batch_size
            )

        except IOError as e:
            raise FileError(f"Failed to read file {file_path}: {str(e)}")

    def _process_data_batches(
        self,
        data_iterator,
        entity_name,
        extract_time,
        result,
        id_field,
        parent_field,
        time_field,
        default_id_field,
        id_generation_strategy,
        params,
        batch_size,
    ):
        """Process data in batches efficiently."""
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

        return all_main_ids

    def _process_batch_main_records(
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
    ):
        """Process main records in a batch."""
        # Get recovery strategy if available, but don't remove it from params
        # so it can be used in subsequent processing
        recovery_strategy = params.get("recovery_strategy")

        # Create arrays for batch processing results
        main_table = []
        main_ids = []

        # Process each record
        for record in batch:
            # Flatten the record
            flattened = flatten_json(
                record,
                separator=params.get("separator", "_"),
                cast_to_string=params.get("cast_to_string", True),
                include_empty=params.get("include_empty", False),
                skip_null=params.get("skip_null", True),
                max_depth=params.get("max_depth"),
                abbreviate_field_names=params.get("abbreviate_field_names", False),
                max_field_component_length=params.get("max_field_component_length"),
                preserve_root_component=params.get("preserve_root", True),
                preserve_leaf_component=params.get("preserve_leaf", True),
                custom_abbreviations=params.get("custom_abbreviations", {}),
                recovery_strategy=recovery_strategy,
            )

            # Add metadata and track
            annotated = annotate_with_metadata(
                flattened,
                parent_id=None,  # Main records have no parent
                extract_time=extract_time,
                source_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
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
        batch,
        entity_name,
        extract_time,
        result,
        id_field,
        main_ids,
        default_id_field,
        id_generation_strategy,
        params,
    ):
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
                preserve_leaf=params["preserve_leaf"],
                custom_abbreviations=params["custom_abbreviations"],
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=params.get("recovery_strategy"),
            )

            # Add arrays to result
            result.add_child_tables(arrays)


class BatchStrategy(ProcessingStrategy):
    """Strategy for optimized batch processing of data."""

    def process(
        self,
        data,
        entity_name: str,
        extract_time: Optional[Any] = None,
        result: Optional[ProcessingResult] = None,
        **kwargs,
    ) -> ProcessingResult:
        """
        Process data in optimized batches.

        Args:
            data: Input data
            entity_name: Name of the entity
            extract_time: Optional extraction timestamp
            result: Optional existing result to append to
            **kwargs: Additional processing parameters

        Returns:
            ProcessingResult with processed data
        """
        # Extract common parameters from kwargs or config
        params = self._get_common_parameters(**kwargs)
        extract_time = extract_time or get_current_timestamp()

        # Initialize result if needed
        result = result or ProcessingResult(
            main_table=[], child_tables={}, entity_name=entity_name
        )

        # Get batch size with fallback to config
        batch_size = kwargs.get("batch_size", self.config.processing.batch_size)

        # Get unique ID parameters
        id_field = params.get("id_field", self.config.metadata.id_field)
        parent_field = params.get("parent_field", self.config.metadata.parent_field)
        time_field = params.get("time_field", self.config.metadata.time_field)
        default_id_field = params.get(
            "default_id_field", self.config.metadata.default_id_field
        )
        id_generation_strategy = params.get(
            "id_generation_strategy", self.config.metadata.id_generation_strategy
        )

        # Get data iterator
        data_iterator = get_data_iterator(None, data)

        # Process in batches
        batch = []
        batch_size_counter = 0
        main_ids = []  # Keep track of main IDs for array extraction

        for record in data_iterator:
            if not isinstance(record, dict):
                continue

            batch.append(record)
            batch_size_counter += 1

            if batch_size_counter >= batch_size:
                # Process batch of main records
                new_main_ids = self._process_batch_main_records(
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
                main_ids.extend(new_main_ids)

                # Process batch of arrays
                self._process_batch_arrays(
                    batch,
                    entity_name,
                    extract_time,
                    result,
                    id_field,
                    new_main_ids,
                    default_id_field,
                    id_generation_strategy,
                    params,
                )

                # Reset batch
                batch = []
                batch_size_counter = 0

        # Process any remaining records
        if batch:
            # Process batch of main records
            new_main_ids = self._process_batch_main_records(
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
            main_ids.extend(new_main_ids)

            # Process batch of arrays
            self._process_batch_arrays(
                batch,
                entity_name,
                extract_time,
                result,
                id_field,
                new_main_ids,
                default_id_field,
                id_generation_strategy,
                params,
            )

        return result

    def _process_batch_main_records(
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
    ):
        """
        Process a batch of main records without array extraction.

        This handles the flattening and metadata annotation of the
        main records without extracting arrays, which is done separately.

        Args:
            batch: The batch of records to process
            entity_name: The name of the entity for the records
            extract_time: Extraction timestamp
            result: ProcessingResult to update
            id_field: Extract ID field name
            parent_field: Field name for parent ID
            time_field: Field name for timestamp
            default_id_field: Field for deterministic IDs
            id_generation_strategy: Custom ID generation function
            params: Processing parameters

        Returns:
            List of extract IDs for the processed records
        """
        if not params:
            params = {}

        # Default parameters
        recovery_strategy = params.get("recovery_strategy")

        main_ids = []
        for record in batch:
            # Flatten the main record
            flattened = flatten_json(
                record,
                separator=params.get("separator", "_"),
                cast_to_string=params.get("cast_to_string", True),
                include_empty=params.get("include_empty", False),
                skip_null=params.get("skip_null", True),
                skip_arrays=True,  # Always skip arrays - they're extracted separately
                visit_arrays=params.get("visit_arrays", False),
                abbreviate_field_names=params.get("abbreviate_field_names", False),
                max_field_component_length=params.get("max_field_component_length"),
                preserve_root_component=params.get("preserve_root_component", True),
                preserve_leaf_component=params.get("preserve_leaf_component", True),
                custom_abbreviations=params.get("custom_abbreviations"),
                recovery_strategy=recovery_strategy,
            )

            # Add metadata and track
            annotated = annotate_with_metadata(
                flattened,
                parent_id=None,  # Main records have no parent
                extract_time=extract_time,
                source_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
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
        batch,
        entity_name,
        extract_time,
        result,
        id_field,
        main_ids,
        default_id_field,
        id_generation_strategy,
        params,
    ):
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
                preserve_leaf=params["preserve_leaf"],
                custom_abbreviations=params["custom_abbreviations"],
                default_id_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                recovery_strategy=params.get("recovery_strategy"),
            )

            # Add arrays to result for this record
            for table_name, child_records in arrays.items():
                for child in child_records:
                    result.add_child_record(table_name, child)


class ChunkedStrategy(ProcessingStrategy):
    """Strategy for processing data in chunks to reduce memory usage."""

    def process(
        self,
        data,
        entity_name: str,
        extract_time: Optional[Any] = None,
        result: Optional[ProcessingResult] = None,
        **kwargs,
    ) -> ProcessingResult:
        """
        Process data in chunks.

        Args:
            data: Input data (iterator or data source)
            entity_name: Name of the entity
            extract_time: Optional extraction timestamp
            result: Optional existing result to append to
            **kwargs: Additional processing parameters

        Returns:
            ProcessingResult with processed data
        """
        # Extract common parameters from kwargs or config
        params = self._get_common_parameters(**kwargs)
        extract_time = extract_time or get_current_timestamp()

        # Initialize result if needed
        result = result or ProcessingResult(
            main_table=[], child_tables={}, entity_name=entity_name
        )

        # Get batch size with fallback to config
        batch_size = kwargs.get("batch_size", self.config.processing.batch_size)

        # Get unique ID parameters
        id_field = params.get("id_field", self.config.metadata.id_field)
        parent_field = params.get("parent_field", self.config.metadata.parent_field)
        time_field = params.get("time_field", self.config.metadata.time_field)
        default_id_field = params.get(
            "default_id_field", self.config.metadata.default_id_field
        )
        id_generation_strategy = params.get(
            "id_generation_strategy", self.config.metadata.id_generation_strategy
        )

        # Get data iterator
        data_iterator = get_data_iterator(None, data)

        # Process in chunks
        chunk = []
        chunk_size = 0
        for record in data_iterator:
            if not isinstance(record, dict):
                continue

            chunk.append(record)
            chunk_size += 1

            if chunk_size >= batch_size:
                # Process this chunk
                self._process_chunk(
                    chunk,
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
                chunk = []
                chunk_size = 0

        # Process any remaining records
        if chunk:
            self._process_chunk(
                chunk,
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
        chunk,
        entity_name,
        extract_time,
        result,
        id_field,
        parent_field,
        time_field,
        default_id_field,
        id_generation_strategy,
        params,
    ):
        """Process a chunk of records."""
        main_ids = []

        # Process each record in the chunk
        for record in chunk:
            # Flatten the main record
            flattened = flatten_json(
                record,
                separator=params.get("separator", "_"),
                cast_to_string=params.get("cast_to_string", True),
                include_empty=params.get("include_empty", False),
                skip_null=params.get("skip_null", True),
                skip_arrays=True,  # Skip arrays - they're extracted separately
                visit_arrays=params.get("visit_arrays", False),
                abbreviate_field_names=params.get("abbreviate_field_names", False),
                max_field_component_length=params.get("max_field_component_length"),
                preserve_root_component=params.get("preserve_root_component", True),
                preserve_leaf_component=params.get("preserve_leaf_component", True),
                custom_abbreviations=params.get("custom_abbreviations"),
            )

            # Add metadata
            annotated = annotate_with_metadata(
                flattened,
                parent_id=None,  # Main records have no parent
                extract_time=extract_time,
                source_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
            )

            # Extract and process arrays for this record
            # Track extract ID and add to main table
            if id_field in annotated:
                main_ids.append(annotated[id_field])
            else:
                main_ids.append(None)

            # Add to the result
            result.add_main_record(annotated)

            # Extract and process arrays for this record
            if id_field in annotated:
                extract_id = annotated[id_field]
                arrays = extract_arrays(
                    record,
                    parent_id=extract_id,
                    entity_name=entity_name,
                    separator=params.get("separator", "_"),
                    cast_to_string=params.get("cast_to_string", True),
                    include_empty=params.get("include_empty", False),
                    skip_null=params.get("skip_null", True),
                    extract_time=extract_time,
                    abbreviate_enabled=params.get("abbreviate_table_names", True),
                    max_component_length=params.get("max_table_component_length"),
                    preserve_leaf=params.get("preserve_leaf_component", True),
                    custom_abbreviations=params.get("custom_abbreviations"),
                    default_id_field=default_id_field,
                    id_generation_strategy=id_generation_strategy,
                    recovery_strategy=params.get("recovery_strategy"),
                )

                # Add arrays to result for this record
                for table_name, records in arrays.items():
                    for child in records:
                        result.add_child_record(table_name, child)


class CSVStrategy(ProcessingStrategy):
    """Strategy for processing CSV files."""

    @error_context("Failed to process CSV file", log_exceptions=True)
    def process(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
        result: Optional[ProcessingResult] = None,
        delimiter: Optional[str] = None,
        has_header: bool = True,
        null_values: Optional[List[str]] = None,
        sanitize_column_names: bool = True,
        infer_types: bool = True,
        skip_rows: int = 0,
        quote_char: Optional[str] = None,
        encoding: str = "utf-8",
        chunk_size: Optional[int] = None,
        **kwargs,
    ) -> ProcessingResult:
        """
        Process data from a CSV file.

        Args:
            file_path: Path to the CSV file
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            result: Optional existing result to append to
            delimiter: CSV delimiter character
            has_header: Whether the CSV has a header row
            null_values: List of strings to treat as null values
            sanitize_column_names: Whether to sanitize column names
            infer_types: Whether to infer data types
            skip_rows: Number of rows to skip at the beginning
            quote_char: Quote character for CSV fields
            encoding: File encoding
            chunk_size: Size of chunks to process
            **kwargs: Additional parameters

        Returns:
            ProcessingResult containing processed data
        """
        # Check file exists
        if not isinstance(file_path, str) or not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

        # Get extraction timestamp
        extract_time = extract_time or get_current_timestamp()

        # Initialize result object
        result = result or ProcessingResult(
            main_table=[], child_tables={}, entity_name=entity_name
        )

        # Extract common parameters from kwargs or config
        params = self._get_common_parameters(**kwargs)

        # Get unique ID parameters
        id_field = params.get("id_field", self.config.metadata.id_field)
        parent_field = params.get("parent_field", self.config.metadata.parent_field)
        time_field = params.get("time_field", self.config.metadata.time_field)
        default_id_field = params.get(
            "default_id_field", self.config.metadata.default_id_field
        )
        id_generation_strategy = params.get(
            "id_generation_strategy", self.config.metadata.id_generation_strategy
        )

        # Process CSV using PyArrow if available (preferred), otherwise use native CSV module
        # We deliberately avoid using pandas to reduce dependencies
        try:
            import pyarrow as pa
            import pyarrow.csv as csv

            # Set CSV reading options
            parse_options = csv.ParseOptions(
                delimiter=delimiter or ",",
                quote_char=quote_char or '"',
                escape_char="\\",
                newlines_in_values=True,
            )

            read_options = csv.ReadOptions(
                skip_rows=skip_rows, encoding=encoding, use_threads=True
            )

            convert_options = csv.ConvertOptions(
                null_values=null_values
                or ["", "NULL", "null", "NA", "na", "N/A", "n/a"],
                strings_can_be_null=True,
            )

            # Read the CSV file
            table = csv.read_csv(
                file_path,
                parse_options=parse_options,
                read_options=read_options,
                convert_options=convert_options,
            )

            # Convert to records
            records = []
            # Convert PyArrow table to Python dict list
            records = table.to_pylist()

            # Process the records in chunks if needed
            if chunk_size and chunk_size > 0:
                for i in range(0, len(records), chunk_size):
                    chunk = records[i : i + chunk_size]
                    self._process_csv_chunk(
                        chunk,
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
            else:
                # Process all records at once
                self._process_csv_chunk(
                    records,
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

        except ImportError:
            # Fall back to built-in CSV module
            import csv as csv_stdlib

            # Use the csv module
            records = []
            try:
                with open(file_path, "r", encoding=encoding) as f:
                    # Skip initial rows if needed
                    for _ in range(skip_rows):
                        next(f, None)

                    # Set up CSV reader
                    csv_reader = csv_stdlib.reader(
                        f, delimiter=delimiter or ",", quotechar=quote_char or '"'
                    )

                    # Read headers if present
                    headers = next(csv_reader) if has_header else None

                    # If no headers, create positional ones
                    if not headers:
                        # Read first row to determine number of columns
                        first_row = next(csv_reader, None)
                        if first_row:
                            num_cols = len(first_row)
                            headers = [f"col{i}" for i in range(num_cols)]

                            # Initialize batch and count
                            batch = []
                            count = 0

                            # Go back to the beginning to re-read all rows including the first one
                            f.seek(0)
                            for _ in range(skip_rows):
                                next(f, None)
                            csv_reader = csv_stdlib.reader(
                                f,
                                delimiter=delimiter or ",",
                                quotechar=quote_char or '"',
                            )
                        else:
                            headers = []  # No data
                            batch = []
                            count = 0
                    else:
                        batch = []
                        count = 0

                    # Process rows
                    for row in csv_reader:
                        record = {}
                        for i, value in enumerate(row):
                            if i < len(headers):
                                col_name = headers[i]

                                # Handle null values
                                if null_values and value in null_values:
                                    record[col_name] = None
                                else:
                                    # Infer types if requested
                                    if infer_types and value:
                                        try:
                                            # Try as int
                                            int_val = int(value)
                                            if str(int_val) == value:
                                                record[col_name] = int_val
                                                continue
                                        except ValueError:
                                            pass

                                        try:
                                            # Try as float
                                            float_val = float(value)
                                            record[col_name] = float_val
                                            continue
                                        except ValueError:
                                            pass

                                        # Check for boolean
                                        if value.lower() in ("true", "yes", "1"):
                                            record[col_name] = True
                                            continue
                                        elif value.lower() in ("false", "no", "0"):
                                            record[col_name] = False
                                            continue

                                        # Default to string
                                        record[col_name] = value

                        batch.append(record)
                        count += 1

                        # Process in chunks if requested
                        if chunk_size and count >= chunk_size:
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
                            batch = []
                            count = 0

                    # Process any remaining records
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

            except Exception as e:
                raise FileError(f"Failed to read CSV file {file_path}: {str(e)}")

        return result

    def _process_csv_chunk(
        self,
        records,
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
    ):
        """Process a chunk of CSV records."""
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

        # Process each record
        for record in records:
            # Add metadata to record
            annotated_record = annotate_with_metadata(
                record,
                id_field=id_field,
                parent_field=parent_field,
                time_field=time_field,
                extract_time=extract_time,
                source_field=default_id_field,
                id_generation_strategy=id_generation_strategy,
                in_place=True,
            )

            # Add to result
            result.main_table.append(annotated_record)
