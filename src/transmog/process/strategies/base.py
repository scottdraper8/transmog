"""Base processing strategy with common functionality."""

from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

from ...config import TransmogConfig
from ...config.utils import ConfigParameterBuilder
from ..result import ProcessingResult

T = TypeVar("T")


class ProcessingStrategy(ABC):
    """Abstract base class for processing strategies."""

    def __init__(self, config: TransmogConfig):
        """Initialize with configuration.

        Args:
            config: Processing configuration
        """
        self.config = config
        self._param_builder = ConfigParameterBuilder(config)

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

    def _get_common_config_params(
        self, extract_time: Optional[Any] = None
    ) -> dict[str, Any]:
        """Get common configuration parameters.

        Args:
            extract_time: Optional extraction timestamp

        Returns:
            Dictionary of common parameters
        """
        return self._param_builder.build_common_params(extract_time=extract_time)

    def _get_batch_size(self, chunk_size: Optional[int] = None) -> int:
        """Get batch size for processing.

        Args:
            chunk_size: Optional override for batch size

        Returns:
            Batch size to use
        """
        return self._param_builder.get_batch_size(chunk_size)

    def _get_common_parameters(self, **kwargs: Any) -> dict[str, Any]:
        """Get common parameters for processing.

        Args:
            **kwargs: Override parameters

        Returns:
            Dictionary of parameters
        """
        return self._param_builder.build_common_params(
            extract_time=kwargs.get("extract_time"),
            **{k: v for k, v in kwargs.items() if k != "extract_time"},
        )

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
