"""CSV processing strategy for handling CSV files with configurable options."""

import os
from typing import Any, Callable, Optional, Union

from ...core.metadata import annotate_with_metadata, get_current_timestamp
from ...error import FileError, error_context, logger
from ...naming.conventions import sanitize_name
from ..result import ProcessingResult
from ..utils import handle_file_error
from .base import ProcessingStrategy


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
        result: Optional[ProcessingResult] = kwargs.pop("result", None)
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
            from ...io.readers.csv import CSVReader

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
            id_field = params.get("id_field", "__transmog_id")
            parent_field = params.get("parent_field", "__parent_transmog_id")
            time_field = params.get("time_field", "__transmog_datetime")
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
                    transmog_time=extract_time,
                    id_field=id_field,
                    parent_field=parent_field,
                    time_field=time_field,
                    source_field=source_field_str,
                    id_generation_strategy=id_generation_strategy,
                    in_place=False,
                    id_field_patterns=params.get("id_field_patterns"),
                    path=entity_name,
                    id_field_mapping=params.get("id_field_mapping"),
                    force_transmog_id=params.get("force_transmog_id", False),
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
