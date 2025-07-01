"""Batch processing strategy for handling data in configurable batches."""

from collections.abc import Generator
from typing import Any, Optional, Union

from ...core.metadata import get_current_timestamp
from ..result import ProcessingResult
from .base import ProcessingStrategy
from .shared import process_batch_arrays, process_batch_main_records


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
        id_field = params.get("id_field", "__transmog_id")
        parent_field = params.get("parent_field", "__parent_transmog_id")
        time_field = params.get("time_field", "__transmog_datetime")
        default_id_field = params.get("default_id_field")
        id_generation_strategy = params.get("id_generation_strategy")

        # Process data in batches
        batch: list[dict[str, Any]] = []
        for record in data:
            batch.append(record)

            # Process batch when it reaches the desired size
            if len(batch) >= batch_size:
                main_ids = process_batch_main_records(
                    self,
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
                process_batch_arrays(
                    self,
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
            main_ids = process_batch_main_records(
                self,
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
            process_batch_arrays(
                self,
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
