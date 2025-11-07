"""Batch processing strategy for handling data in configurable batches."""

from collections.abc import Generator
from typing import Any, Optional, Union

from transmog.core.metadata import get_current_timestamp
from transmog.process.result import ProcessingResult
from transmog.types import ProcessingContext

from .base import ProcessingStrategy
from .shared import process_batch_main_records


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
        if result is None:
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
            )

        extract_time = extract_time or get_current_timestamp()
        context = ProcessingContext(extract_time=extract_time)

        batch_size = self.config.batch_size

        batch: list[dict[str, Any]] = []
        for record in data:
            batch.append(record)

            if len(batch) >= batch_size:
                process_batch_main_records(
                    self,
                    batch=batch,
                    entity_name=entity_name,
                    config=self.config,
                    context=context,
                    result=result,
                )

                batch = []

        if batch:
            process_batch_main_records(
                self,
                batch=batch,
                entity_name=entity_name,
                config=self.config,
                context=context,
                result=result,
            )

        return result
