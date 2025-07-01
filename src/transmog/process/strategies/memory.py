"""In-memory processing strategy for processing data structures in memory."""

from typing import Any, Optional

from ...core.hierarchy import process_records_in_single_pass
from ...core.metadata import get_current_timestamp
from ...error import error_context
from ..result import ProcessingResult
from .base import ProcessingStrategy


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
        transmog_time = params.get("transmog_time")
        separator = params.get("separator", "_")
        cast_to_string = params.get("cast_to_string", True)
        include_empty = params.get("include_empty", False)
        skip_null = params.get("skip_null", True)
        id_field = params.get("id_field", "__transmog_id")
        parent_field = params.get("parent_field", "__parent_transmog_id")
        time_field = params.get("time_field", "__transmog_datetime")
        visit_arrays = params.get("visit_arrays", True)
        nested_threshold = params.get("nested_threshold", 4)
        default_id_field = params.get("default_id_field")
        id_generation_strategy = params.get("id_generation_strategy")
        force_transmog_id = params.get("force_transmog_id", False)
        id_field_patterns = params.get("id_field_patterns")
        id_field_mapping = params.get("id_field_mapping")
        max_depth = params.get("max_depth", 100)

        # Process all records in a single pass
        main_records, child_tables = process_records_in_single_pass(
            records=data_list,
            entity_name=entity_name,
            transmog_time=transmog_time,
            separator=separator,
            cast_to_string=cast_to_string,
            include_empty=include_empty,
            skip_null=skip_null,
            id_field=id_field,
            parent_field=parent_field,
            time_field=time_field,
            visit_arrays=visit_arrays,
            nested_threshold=nested_threshold,
            default_id_field=default_id_field,
            id_generation_strategy=id_generation_strategy,
            recovery_strategy=recovery_strategy,
            max_depth=max_depth,
            keep_arrays=keep_arrays,
            id_field_patterns=id_field_patterns,
            id_field_mapping=id_field_mapping,
            force_transmog_id=force_transmog_id,
        )

        # Update result
        for record in main_records:
            result.add_main_record(record)

        result.add_child_tables(child_tables)
        result.source_info["record_count"] = len(data_list)

        return result
