"""File processing strategy for handling JSON and JSONL files."""

import os
from typing import Any, Optional

import orjson

from transmog.core.metadata import get_current_timestamp
from transmog.error import FileError, logger
from transmog.process.result import ProcessingResult
from transmog.process.utils import handle_file_error
from transmog.types import ProcessingContext

from .base import ProcessingStrategy
from .shared import process_batch_main_records


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
        if not isinstance(data, str):
            raise TypeError(f"Expected string file path, got {type(data).__name__}")

        file_path = data

        if not os.path.exists(file_path):
            raise FileError(f"File not found: {file_path}")

        if result is None:
            result = ProcessingResult(
                main_table=[],
                child_tables={},
                entity_name=entity_name,
            )
            result.source_info["file_path"] = file_path

        file_ext = os.path.splitext(file_path)[1].lower()

        extract_time = extract_time or get_current_timestamp()
        context = ProcessingContext(extract_time=extract_time)

        if file_ext == ".jsonl":
            return self._process_jsonl(file_path, entity_name, context, result)
        elif file_ext == ".json":
            return self._process_json(file_path, entity_name, context, result)
        else:
            raise FileError(f"Unsupported file format: {file_ext}")

    def _process_json(
        self,
        file_path: str,
        entity_name: str,
        context: ProcessingContext,
        result: ProcessingResult,
    ) -> ProcessingResult:
        """Process a JSON file.

        Args:
            file_path: Path to JSON file
            entity_name: Entity name
            context: Processing context
            result: Result object

        Returns:
            ProcessingResult with processed data
        """
        try:
            with open(file_path, "rb") as f:
                data = orjson.loads(f.read())

            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                raise FileError(
                    f"Expected dict or list in JSON file, got {type(data).__name__}"
                )

            batch_size = self.config.batch_size
            batch = []

            for record in data:
                batch.append(record)

                if len(batch) >= batch_size:
                    process_batch_main_records(
                        self, batch, entity_name, self.config, context, result
                    )
                    batch = []

            if batch:
                process_batch_main_records(
                    self, batch, entity_name, self.config, context, result
                )

            return result

        except Exception as e:
            handle_file_error(file_path, e, "JSON")

    def _process_jsonl(
        self,
        file_path: str,
        entity_name: str,
        context: ProcessingContext,
        result: ProcessingResult,
    ) -> ProcessingResult:
        """Process a JSONL file.

        Args:
            file_path: Path to JSONL file
            entity_name: Entity name
            context: Processing context
            result: Result object

        Returns:
            ProcessingResult with processed data
        """
        try:
            batch_size = self.config.batch_size
            batch = []

            with open(file_path, "rb") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = orjson.loads(line)
                        batch.append(record)

                        if len(batch) >= batch_size:
                            process_batch_main_records(
                                self, batch, entity_name, self.config, context, result
                            )
                            batch = []

                    except orjson.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line: {e}")
                        continue

            if batch:
                process_batch_main_records(
                    self, batch, entity_name, self.config, context, result
                )

            return result

        except Exception as e:
            handle_file_error(file_path, e, "JSONL")
