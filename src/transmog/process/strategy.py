"""
Processing strategies module for Transmog.

This module implements the Strategy pattern for different processing methods,
centralizing common functionality while allowing specialization for different data sources.
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
    stream_process_structure,
)
from ..core.metadata import (
    generate_extract_id,
    get_current_timestamp,
    create_batch_metadata,
)
from .result import ProcessingResult, ConversionMode

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

        return {
            # Naming parameters
            "separator": naming_config.separator,
            "abbreviate_table_names": naming_config.abbreviate_table_names,
            "abbreviate_field_names": naming_config.abbreviate_field_names,
            "max_table_component_length": naming_config.max_table_component_length,
            "max_field_component_length": naming_config.max_field_component_length,
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
            "deterministic_id_fields": metadata_config.deterministic_id_fields,
            "id_generation_strategy": metadata_config.id_generation_strategy,
        }

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


class InMemoryStrategy(ProcessingStrategy):
    """Strategy for processing in-memory data structures."""

    @error_context("Failed to process data", log_exceptions=True)
    def process(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        entity_name: str,
        extract_time: Optional[Any] = None,
        use_single_pass: bool = True,
        **kwargs,
    ) -> ProcessingResult:
        """
        Process in-memory data (dictionary or list of dictionaries).

        Args:
            data: Input data (dict or list of dicts)
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            use_single_pass: Whether to use single-pass processing
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
        config_params = self._get_common_config_params(extract_time)

        # Use single-pass processing for better performance when appropriate
        if use_single_pass:
            return self._process_in_single_pass(
                iter(data_list), entity_name, extract_time
            )
        else:
            # Use chunked processing based on configuration
            return self._process_in_chunks(
                iter(data_list), entity_name, extract_time, kwargs.get("chunk_size")
            )

    def _process_in_single_pass(
        self,
        data_iterator: Iterator[Dict[str, Any]],
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """
        Process data in a single pass for better performance.

        Args:
            data_iterator: Iterator of data records
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp

        Returns:
            ProcessingResult containing processed data
        """
        # Convert iterator to list for single-pass processing
        records = list(data_iterator)

        # Get configuration parameters
        params = self._get_common_config_params(extract_time)

        # Process records in a single pass
        main_records, child_tables = process_records_in_single_pass(
            records=records, entity_name=entity_name, **params
        )

        return ProcessingResult(
            main_table=main_records,
            child_tables=child_tables,
            entity_name=entity_name,
            source_info={"record_count": len(records)},
        )

    def _process_in_chunks(
        self,
        data_iterator: Iterator[Dict[str, Any]],
        entity_name: str,
        extract_time: Optional[Any] = None,
        chunk_size: Optional[int] = None,
    ) -> ProcessingResult:
        """
        Process data in chunks for memory efficiency.

        Args:
            data_iterator: Iterator of data records
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            chunk_size: Size of chunks to process

        Returns:
            ProcessingResult containing processed data
        """
        # Get batch size from configuration or override
        batch_size = self._get_batch_size(chunk_size)

        # Create batches from the iterator
        batches = []
        total_records = 0

        # Process data in chunks
        for chunk in iter(
            lambda: list(itertools.islice(data_iterator, batch_size)), []
        ):
            batches.append(chunk)
            total_records += len(chunk)

        if not batches:
            # No data to process
            return ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
                source_info={"record_count": 0},
            )

        # Process the first batch to initialize the result
        first_batch = batches[0]
        result = self._process_batch_internal(first_batch, entity_name, extract_time)

        # Process remaining batches and combine results
        if len(batches) > 1:
            batch_results = [result]

            for batch in batches[1:]:
                batch_result = self._process_batch_internal(
                    batch, entity_name, extract_time
                )
                batch_results.append(batch_result)

            # Combine all batch results
            result = ProcessingResult.combine_results(batch_results, entity_name)

        # Update source info
        result.source_info["record_count"] = total_records

        return result

    def _process_batch_internal(
        self,
        batch_data: List[Dict[str, Any]],
        entity_name: str,
        extract_time: Optional[Any] = None,
    ) -> ProcessingResult:
        """
        Process a batch of records.

        Args:
            batch_data: Batch of records to process
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp

        Returns:
            ProcessingResult for the batch
        """
        # Get configuration parameters
        params = self._get_common_config_params(extract_time)

        # Process records in a single pass
        main_records, child_tables = process_records_in_single_pass(
            records=batch_data, entity_name=entity_name, **params
        )

        return ProcessingResult(
            main_table=main_records,
            child_tables=child_tables,
            entity_name=entity_name,
            source_info={"record_count": len(batch_data)},
        )


class FileStrategy(ProcessingStrategy):
    """Strategy for processing file-based data."""

    @error_context("Failed to process file", log_exceptions=True)
    def process(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
        **kwargs,
    ) -> ProcessingResult:
        """
        Process data from a file.

        Args:
            file_path: Path to the file to process
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            **kwargs: Additional parameters

        Returns:
            ProcessingResult containing processed data
        """
        if not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

        if not os.path.isfile(file_path):
            raise FileError(f"Not a file: {file_path}")

        # Detect file format based on extension
        file_ext = os.path.splitext(file_path)[1].lower()

        if file_ext == ".json":
            # Process JSON file
            return self._process_json_file(file_path, entity_name, extract_time)
        elif file_ext == ".jsonl":
            # Process JSONL file
            return self._process_jsonl_file(
                file_path, entity_name, extract_time, kwargs.get("chunk_size")
            )
        else:
            raise FileError(f"Unsupported file format: {file_ext}")

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


class BatchStrategy(ProcessingStrategy):
    """Strategy for processing batches of records."""

    @error_context("Failed to process batch", log_exceptions=True)
    def process(
        self,
        batch_data: List[Dict[str, Any]],
        entity_name: str,
        extract_time: Optional[Any] = None,
        **kwargs,
    ) -> ProcessingResult:
        """
        Process a batch of records.

        Args:
            batch_data: Batch of records to process
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            **kwargs: Additional parameters

        Returns:
            ProcessingResult for the batch
        """
        # Validate input
        validate_input(
            batch_data, expected_type=list, param_name="batch_data", allow_none=False
        )

        # Create in-memory strategy for processing
        in_memory_strategy = InMemoryStrategy(self.config)

        # Process the batch using in-memory strategy
        return in_memory_strategy._process_batch_internal(
            batch_data, entity_name, extract_time
        )


class ChunkedStrategy(ProcessingStrategy):
    """Strategy for processing data in chunks for memory efficiency."""

    @error_context("Failed to process in chunks", log_exceptions=True)
    def process(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
        entity_name: str,
        extract_time: Optional[Any] = None,
        chunk_size: Optional[int] = None,
        **kwargs,
    ) -> ProcessingResult:
        """
        Process data in chunks for memory efficiency.

        Args:
            data: Input data (various formats)
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            chunk_size: Size of chunks to process
            **kwargs: Additional parameters

        Returns:
            ProcessingResult containing processed data
        """
        # Get iterator for the data
        data_iterator = self._get_data_iterator(
            data, kwargs.get("input_format", "auto")
        )

        # Create in-memory strategy for processing
        in_memory_strategy = InMemoryStrategy(self.config)

        # Process using in-memory strategy with chunking
        return in_memory_strategy._process_in_chunks(
            data_iterator, entity_name, extract_time, chunk_size
        )

    def _get_data_iterator(
        self,
        data_source: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
        input_format: str = "auto",
    ) -> Iterator[Dict[str, Any]]:
        """
        Get an iterator for the data source based on its format.

        Args:
            data_source: Data source (various formats)
            input_format: Format of the data or 'auto' for detection

        Returns:
            Iterator of dictionaries
        """
        # Map legacy format names to new format names
        format_mapping = {
            "json": "json_string_object",
            "jsonl": "jsonl_string",
        }

        # Map legacy format to new format if needed
        if input_format in format_mapping:
            input_format = format_mapping[input_format]

        # Auto-detect format if not specified
        if input_format == "auto":
            if isinstance(data_source, dict):
                input_format = "json_object"
            elif isinstance(data_source, list):
                input_format = "json_array"
            # Check if object is a generator or iterator
            elif hasattr(data_source, "__iter__") and hasattr(data_source, "__next__"):
                # Direct return the generator/iterator
                return data_source
            elif isinstance(data_source, (str, bytes)):
                # Try to determine if it's a file path or JSON/JSONL string
                if isinstance(data_source, str) and os.path.exists(data_source):
                    # It's a file path
                    ext = os.path.splitext(data_source)[1].lower()
                    if ext == ".json":
                        input_format = "json_file"
                    elif ext == ".jsonl":
                        input_format = "jsonl_file"
                    else:
                        raise ConfigurationError(f"Unsupported file format: {ext}")
                else:
                    # It's a string or bytes, try to parse as JSON
                    try:
                        # If it's bytes, decode to string first
                        data_str = (
                            data_source.decode("utf-8")
                            if isinstance(data_source, bytes)
                            else data_source
                        )
                        data_str = data_str.strip()

                        if data_str.startswith("[") and data_str.endswith("]"):
                            input_format = "json_string_array"
                        elif data_str.startswith("{") and data_str.endswith("}"):
                            input_format = "json_string_object"
                        elif "\n" in data_str:
                            # Check if it looks like JSONL (multiple JSON objects, one per line)
                            input_format = "jsonl_string"
                        else:
                            input_format = (
                                "json_string_object"  # Default to single object
                            )
                    except Exception as e:
                        raise ConfigurationError(
                            f"Failed to determine format of input data: {str(e)}"
                        )
            else:
                raise ConfigurationError(f"Unsupported data type: {type(data_source)}")

        # Process based on determined format
        if input_format == "json_object":
            # Single object, convert to iterator
            return iter([data_source])

        elif input_format == "json_array":
            # Array of objects, convert to iterator
            return iter(data_source)

        elif input_format == "json_file":
            # JSON file path
            try:
                with open(data_source, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    return iter([data])
                elif isinstance(data, list):
                    return iter(data)
                else:
                    raise ParsingError(f"Invalid JSON data in {data_source}")
            except json.JSONDecodeError as e:
                raise ParsingError(f"Failed to parse JSON file {data_source}: {str(e)}")
            except IOError as e:
                raise FileError(f"Failed to read file {data_source}: {str(e)}")

        elif input_format == "jsonl_file":
            # JSONL file path
            def jsonl_iterator():
                with open(data_source, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue  # Skip empty lines and comments
                        try:
                            yield safe_json_loads(line)
                        except json.JSONDecodeError as e:
                            logger.warning(
                                f"Error parsing line {line_num} in {data_source}: {str(e)}"
                            )
                            continue

            return jsonl_iterator()

        elif input_format == "json_string_object":
            # JSON string (single object)
            try:
                data_str = (
                    data_source.decode("utf-8")
                    if isinstance(data_source, bytes)
                    else data_source
                )
                data = json.loads(data_str)

                if isinstance(data, dict):
                    return iter([data])
                else:
                    raise ParsingError(f"Expected JSON object, got {type(data)}")
            except json.JSONDecodeError as e:
                raise ParsingError(f"Failed to parse JSON string: {str(e)}")

        elif input_format == "json_string_array":
            # JSON string (array of objects)
            try:
                data_str = (
                    data_source.decode("utf-8")
                    if isinstance(data_source, bytes)
                    else data_source
                )
                data = json.loads(data_str)

                if isinstance(data, list):
                    return iter(data)
                else:
                    raise ParsingError(f"Expected JSON array, got {type(data)}")
            except json.JSONDecodeError as e:
                raise ParsingError(f"Failed to parse JSON string: {str(e)}")

        elif input_format == "jsonl_string":
            # JSONL string (multiple objects, one per line)
            try:
                data_str = (
                    data_source.decode("utf-8")
                    if isinstance(data_source, bytes)
                    else data_source
                )
                lines = data_str.strip().split("\n")

                def jsonl_string_iterator():
                    for line_num, line in enumerate(lines, 1):
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue  # Skip empty lines and comments
                        try:
                            yield safe_json_loads(line)
                        except json.JSONDecodeError as e:
                            logger.warning(
                                f"Error parsing line {line_num} in JSONL string: {str(e)}"
                            )
                            continue

                return jsonl_string_iterator()
            except Exception as e:
                raise ParsingError(f"Failed to parse JSONL string: {str(e)}")

        else:
            raise ConfigurationError(f"Unsupported input format: {input_format}")


class CSVStrategy(ProcessingStrategy):
    """Strategy for processing CSV files."""

    @error_context("Failed to process CSV file", log_exceptions=True)
    def process(
        self,
        file_path: str,
        entity_name: str,
        extract_time: Optional[Any] = None,
        delimiter: Optional[str] = None,
        has_header: bool = True,
        null_values: Optional[List[str]] = None,
        sanitize_column_names: bool = True,
        infer_types: bool = True,
        skip_rows: int = 0,
        quote_char: Optional[str] = None,
        encoding: str = "utf-8",
        **kwargs,
    ) -> ProcessingResult:
        """
        Process a CSV file.

        Args:
            file_path: Path to the CSV file
            entity_name: Name of the entity being processed
            extract_time: Optional extraction timestamp
            delimiter: CSV delimiter character
            has_header: Whether the CSV has a header row
            null_values: List of strings to treat as null values
            sanitize_column_names: Whether to sanitize column names
            infer_types: Whether to infer data types
            skip_rows: Number of rows to skip at the beginning
            quote_char: Quote character for CSV fields
            encoding: File encoding
            **kwargs: Additional parameters

        Returns:
            ProcessingResult containing processed data
        """
        if not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

        if not os.path.isfile(file_path):
            raise FileError(f"Not a file: {file_path}")

        try:
            # Try to import csv
            import csv

            # Default values
            if delimiter is None:
                delimiter = ","

            if null_values is None:
                null_values = [
                    "",
                    "NULL",
                    "null",
                    "None",
                    "none",
                    "NA",
                    "na",
                    "N/A",
                    "n/a",
                ]

            if quote_char is None:
                quote_char = '"'

            # Open the CSV file
            with open(file_path, "r", encoding=encoding) as f:
                # Skip initial rows if needed
                for _ in range(skip_rows):
                    next(f, None)

                # Create CSV reader
                csv_reader = csv.reader(f, delimiter=delimiter, quotechar=quote_char)

                # Read header if present
                headers = next(csv_reader) if has_header else None

                if headers is None:
                    # No header, use column indices as field names
                    sample_row = next(csv_reader, None)
                    if sample_row is None:
                        # Empty CSV
                        return ProcessingResult(
                            main_table=[],
                            child_tables={},
                            entity_name=entity_name,
                            source_info={"record_count": 0},
                        )

                    # Create headers based on column count
                    headers = [f"column_{i}" for i in range(len(sample_row))]

                    # Reset reader
                    f.seek(0)
                    for _ in range(skip_rows):
                        next(f, None)
                    csv_reader = csv.reader(
                        f, delimiter=delimiter, quotechar=quote_char
                    )

                # Sanitize header names if requested
                if sanitize_column_names:
                    from ..naming.conventions import sanitize_column_names

                    headers = sanitize_column_names(
                        headers, separator="_", replace_with="_", sql_safe=True
                    )

                # Read all rows into dictionaries
                records = []
                for row in csv_reader:
                    # Skip rows with incorrect number of fields
                    if len(row) != len(headers):
                        logger.warning(
                            f"Skipping row with {len(row)} fields (expected {len(headers)})"
                        )
                        continue

                    # Create record
                    record = {}
                    for i, value in enumerate(row):
                        # Handle null values
                        if value in null_values:
                            record[headers[i]] = None
                        else:
                            # Infer types if requested
                            if infer_types:
                                # Try to convert to int, float, or bool
                                if value.isdigit():
                                    record[headers[i]] = int(value)
                                elif value.replace(".", "", 1).isdigit():
                                    record[headers[i]] = float(value)
                                elif value.lower() in ("true", "false"):
                                    record[headers[i]] = value.lower() == "true"
                                else:
                                    record[headers[i]] = value
                            else:
                                record[headers[i]] = value

                    records.append(record)

                # Create in-memory strategy for processing
                in_memory_strategy = InMemoryStrategy(self.config)

                # Process using in-memory strategy
                return in_memory_strategy.process(records, entity_name, extract_time)

        except Exception as e:
            raise FileError(f"Failed to process CSV file {file_path}: {str(e)}")
