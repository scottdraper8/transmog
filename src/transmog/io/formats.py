"""Format detection and registration for IO operations.

This module provides centralized format detection and handles
registration of readers and writers for different formats.
"""

from typing import Any, Optional


class FormatRegistry:
    """Registry for available input and output formats.

    This class manages the registration of format handlers and
    provides information about available formats.
    """

    _reader_formats: set[str] = set()
    _writer_formats: set[str] = set()
    _writer_classes: dict[str, Any] = {}  # Store writer classes

    @classmethod
    def register_reader_format(cls, format_name: str) -> None:
        """Register a format as having a reader implementation.

        Args:
            format_name: The name of the format (e.g., 'json', 'csv')
        """
        cls._reader_formats.add(format_name)

    @classmethod
    def register_writer_format(
        cls, format_name: str, writer_class: Optional[Any] = None
    ) -> None:
        """Register a format as having a writer implementation.

        Args:
            format_name: The name of the format (e.g., 'json', 'csv')
            writer_class: The writer class (optional)
        """
        cls._writer_formats.add(format_name)
        if writer_class is not None:
            cls._writer_classes[format_name] = writer_class

    @classmethod
    def get_available_reader_formats(cls) -> list[str]:
        """Get a list of all available reader formats.

        Returns:
            List of format names
        """
        return sorted(cls._reader_formats)

    @classmethod
    def get_available_writer_formats(cls) -> list[str]:
        """Get a list of all available writer formats.

        Returns:
            List of format names
        """
        return sorted(cls._writer_formats)

    @classmethod
    def list_all_writers(cls) -> dict[str, Any]:
        """Get all registered writer classes.

        Returns:
            Dictionary mapping format names to writer classes
        """
        from transmog.io.writer_factory import _WRITER_REGISTRY

        # Return the writer registry
        return _WRITER_REGISTRY

    @classmethod
    def list_available_formats(cls) -> list[str]:
        """Get a list of all available formats with writer implementations.

        This only includes formats with writers whose dependencies
        are available.

        Returns:
            List of available format names
        """
        available_formats = []
        for format_name, writer_class in cls.list_all_writers().items():
            if writer_class.is_available():
                available_formats.append(format_name)
        return sorted(available_formats)

    @classmethod
    def create_writer(cls, format_name: str, **options: Any) -> Any:
        """Create a writer for the specified format.

        Args:
            format_name: Name of the format
            **options: Writer options

        Returns:
            Writer instance
        """
        from transmog.io.writer_factory import create_writer

        # Use the existing create_writer function
        return create_writer(format_name, **options)

    @classmethod
    def has_reader_format(cls, format_name: str) -> bool:
        """Check if a reader format is available.

        Args:
            format_name: Format name to check

        Returns:
            Whether the format is available
        """
        return format_name in cls._reader_formats

    @classmethod
    def has_writer_format(cls, format_name: str) -> bool:
        """Check if a writer format is available.

        Args:
            format_name: Format name to check

        Returns:
            Whether the format is available
        """
        return format_name in cls._writer_formats


# Import the central DependencyManager instead of duplicating implementation


def detect_format(data_source: Any) -> str:
    """Detect the format of a data source.

    Args:
        data_source: Data source to examine

    Returns:
        Detected format name or 'unknown'
    """
    import os

    # File path detection
    if isinstance(data_source, str) and os.path.isfile(data_source):
        _, ext = os.path.splitext(data_source)
        ext = ext.lower()

        if ext == ".json":
            return "json"
        elif ext == ".jsonl":
            return "jsonl"
        elif ext in (".csv", ".tsv"):
            return "csv"
        elif ext == ".parquet":
            return "parquet"

    # Content-based detection
    if isinstance(data_source, (dict, list)):
        return "json"
    elif isinstance(data_source, (str, bytes)):
        # Try to determine if it's JSON or JSONL
        if isinstance(data_source, bytes):
            sample = data_source[:1000].decode("utf-8", errors="ignore")
        else:
            sample = data_source[:1000]

        sample = sample.strip()
        if sample.startswith("{") and sample.find("\n{") == -1:
            return "json"
        elif sample.startswith("[") and sample.find("\n[") == -1:
            return "json"
        elif sample.startswith("{") and sample.find("\n{") > -1:
            return "jsonl"

    return "unknown"
