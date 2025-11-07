"""In-memory processing strategy for processing data structures in memory."""

from typing import Any, Optional

from transmog.core.hierarchy import process_records_in_single_pass
from transmog.core.metadata import get_current_timestamp
from transmog.error import error_context
from transmog.process.result import ProcessingResult
from transmog.types import ProcessingContext

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
        result = kwargs.pop("result", None)

        if result is None:
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
            )

        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            raise TypeError(
                f"Expected dict or list of dicts, got {type(data).__name__}"
            )

        extract_time = extract_time or get_current_timestamp()
        context = ProcessingContext(extract_time=extract_time)

        return self._process_in_memory(data_list, entity_name, context, result)

    def _process_in_memory(
        self,
        data_list: list[dict[str, Any]],
        entity_name: str,
        context: ProcessingContext,
        result: ProcessingResult,
    ) -> ProcessingResult:
        """Process a list of dictionaries in memory.

        Args:
            data_list: List of dictionaries to process
            entity_name: Name of the entity
            context: Processing context
            result: Existing result object

        Returns:
            ProcessingResult with processed data
        """
        processed_records, child_arrays = process_records_in_single_pass(
            data_list,
            entity_name=entity_name,
            config=self.config,
            context=context,
        )

        if child_arrays:
            result.add_child_tables(child_arrays)

        for record in processed_records:
            result.add_main_record(record)

        return result
