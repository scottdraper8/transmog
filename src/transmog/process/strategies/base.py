"""Base processing strategy with common functionality."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from transmog.config import TransmogConfig
from transmog.process.result import ProcessingResult


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

    def _remove_array_fields_from_record(self, record: dict[str, Any]) -> None:
        """Remove array fields from record in-place.

        Args:
            record: Record to modify
        """
        keys_to_remove = []
        for key, value in record.items():
            if isinstance(value, list):
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del record[key]
