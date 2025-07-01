"""ProcessingResult package with modular components.

This package provides the ProcessingResult class and its supporting modules,
split from the original oversized result.py file for better maintainability.
"""

from typing import Any, BinaryIO, Optional, Union

from .converters import ResultConverters
from .core import ConversionMode, ProcessingResult as CoreProcessingResult
from .streaming import ResultStreaming
from .utils import ResultUtils
from .writers import ResultWriters


class ProcessingResult(CoreProcessingResult):
    """Complete ProcessingResult class with all functionality.

    This class combines the core functionality with converters, writers,
    streaming, and utility methods extracted from the original oversized
    result.py file.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize ProcessingResult with all components."""
        super().__init__(*args, **kwargs)

        # Initialize component helpers
        self._converters = ResultConverters(self)
        self._writers = ResultWriters(self)
        self._streaming = ResultStreaming(self)
        self._utils = ResultUtils(self)

    # Converter methods
    def to_json_bytes(
        self, indent: Optional[int] = None, **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert all tables to JSON bytes."""
        return self._converters.to_json_bytes(indent=indent, **kwargs)

    def to_csv_bytes(
        self, include_header: bool = True, **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert all tables to CSV bytes."""
        return self._converters.to_csv_bytes(include_header=include_header, **kwargs)

    def to_parquet_bytes(
        self, compression: str = "snappy", **kwargs: Any
    ) -> dict[str, bytes]:
        """Convert all tables to Parquet bytes."""
        return self._converters.to_parquet_bytes(compression=compression, **kwargs)

    # Writer methods
    def write_all_json(
        self, base_path: str, indent: Optional[int] = 2, **kwargs: Any
    ) -> dict[str, str]:
        """Write all tables to JSON files."""
        return self._writers.write_all_json(base_path, indent=indent, **kwargs)

    def write_all_csv(
        self, base_path: str, include_header: bool = True, **kwargs: Any
    ) -> dict[str, str]:
        """Write all tables to CSV files."""
        return self._writers.write_all_csv(
            base_path, include_header=include_header, **kwargs
        )

    def write_all_parquet(
        self, base_path: str, compression: str = "snappy", **kwargs: Any
    ) -> dict[str, str]:
        """Write all tables to Parquet files."""
        return self._writers.write_all_parquet(
            base_path, compression=compression, **kwargs
        )

    def write(
        self, format_name: str, base_path: str, **format_options: Any
    ) -> dict[str, str]:
        """Write all tables to files of the specified format."""
        return self._writers.write(format_name, base_path, **format_options)

    def write_to_file(
        self, format_name: str, output_directory: str, **options: Any
    ) -> dict[str, str]:
        """Write results to files using the specified writer."""
        return self._writers.write_to_file(format_name, output_directory, **options)

    # Streaming methods
    def stream_to_parquet(
        self,
        base_path: str,
        compression: str = "snappy",
        row_group_size: int = 10000,
        **kwargs: Any,
    ) -> dict[str, str]:
        """Stream all tables to Parquet files with memory-efficient processing."""
        return self._streaming.stream_to_parquet(
            base_path, compression=compression, row_group_size=row_group_size, **kwargs
        )

    def stream_to_output(
        self,
        format_name: str,
        output_destination: Optional[Union[str, BinaryIO]] = None,
        **options: Any,
    ) -> None:
        """Stream results to an output destination."""
        return self._streaming.stream_to_output(
            format_name, output_destination=output_destination, **options
        )

    # Utility methods
    def count_records(self) -> dict[str, int]:
        """Count records in all tables."""
        return self._utils.count_records()

    @staticmethod
    def is_parquet_available() -> bool:
        """Check if PyArrow is available for Parquet operations."""
        return ResultUtils.is_parquet_available()

    @staticmethod
    def is_orjson_available() -> bool:
        """Check if orjson is available for optimized JSON operations."""
        return ResultUtils.is_orjson_available()

    def _clear_intermediate_data(self) -> None:
        """Clear intermediate data representations to save memory."""
        return self._utils.clear_intermediate_data()


# Export the main classes and enums
__all__ = [
    "ProcessingResult",
    "ConversionMode",
    "ResultConverters",
    "ResultWriters",
    "ResultStreaming",
    "ResultUtils",
]
