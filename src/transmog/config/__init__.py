"""Configuration package for Transmog.

Provides settings management, profile configuration, and extension points.
"""

import dataclasses
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, Union

from transmog.config.settings import (
    ExtensionRegistry,
    TransmogSettings,
    configure,
    load_config,
    load_profile,
)
from transmog.config.validation import (
    validate_batch_size,
    validate_cache_size,
    validate_component_length,
    validate_field_name,
    validate_id_field_mapping,
    validate_max_depth,
    validate_recovery_strategy,
    validate_separator,
)
from transmog.error import ConfigurationError

__all__ = [
    "settings",
    "extensions",
    "load_profile",
    "load_config",
    "configure",
    "TransmogSettings",
    "ExtensionRegistry",
    "ProcessingMode",
    "NamingConfig",
    "ProcessingConfig",
    "MetadataConfig",
    "ErrorHandlingConfig",
    "TransmogConfig",
    "validate_component_length",
]

# Global instances
settings = TransmogSettings()
extensions = ExtensionRegistry()


class ProcessingMode(Enum):
    """Processing modes determining memory/performance tradeoff."""

    STANDARD = auto()  # Default mode
    LOW_MEMORY = auto()  # Memory usage optimization
    HIGH_PERFORMANCE = auto()  # Performance optimization


@dataclass
class NamingConfig:
    """Configuration for naming conventions."""

    separator: str = "_"
    deeply_nested_threshold: int = 4


@dataclass
class ProcessingConfig:
    """Configuration for data processing options."""

    cast_to_string: bool = True
    include_empty: bool = False
    skip_null: bool = True
    max_nesting_depth: Optional[int] = None
    max_depth: int = 100  # Maximum recursion depth
    path_parts_optimization: bool = True
    visit_arrays: bool = True
    keep_arrays: bool = (
        False  # Whether to keep arrays in main table after exploding into child tables
    )
    batch_size: int = 1000
    processing_mode: ProcessingMode = ProcessingMode.STANDARD
    # Validation settings
    validate_field_names: bool = False
    validate_table_names: bool = False


@dataclass
class MetadataConfig:
    """Configuration for metadata generation."""

    id_field: str = "__extract_id"
    parent_field: str = "__parent_extract_id"
    time_field: str = "__extract_datetime"
    default_id_field: Optional[Union[str, dict[str, str]]] = None
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None


@dataclass
class ErrorHandlingConfig:
    """Configuration for error handling and recovery."""

    allow_malformed_data: bool = False
    recovery_strategy: str = "strict"  # Options: "strict", "skip", "partial"
    max_retries: int = 3
    error_log_path: Optional[str] = None


@dataclass
class CacheConfig:
    """Configuration for value processing cache behavior."""

    enabled: bool = True
    maxsize: int = 10000
    clear_after_batch: bool = False


@dataclass
class TransmogConfig:
    """Complete configuration for Transmog processing."""

    naming: NamingConfig = field(default_factory=NamingConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    error_handling: ErrorHandlingConfig = field(default_factory=ErrorHandlingConfig)
    cache_config: CacheConfig = field(default_factory=CacheConfig)

    # Basic factories

    @classmethod
    def default(cls) -> "TransmogConfig":
        """Create a default configuration."""
        return cls()

    @classmethod
    def memory_optimized(cls) -> "TransmogConfig":
        """Create a memory-optimized configuration."""
        return cls(
            processing=ProcessingConfig(
                processing_mode=ProcessingMode.LOW_MEMORY,
                batch_size=100,
                path_parts_optimization=True,
                visit_arrays=True,
            ),
            cache_config=CacheConfig(
                enabled=True,
                maxsize=1000,  # Reduced cache size for memory conservation
                clear_after_batch=True,
            ),
        )

    @classmethod
    def performance_optimized(cls) -> "TransmogConfig":
        """Create a performance-optimized configuration."""
        return cls(
            processing=ProcessingConfig(
                processing_mode=ProcessingMode.HIGH_PERFORMANCE,
                batch_size=10000,
                path_parts_optimization=True,
                visit_arrays=True,
            ),
            cache_config=CacheConfig(
                enabled=True,
                maxsize=50000,  # Enlarged cache for performance
                clear_after_batch=False,
            ),
        )

    # ID Generation factories

    @classmethod
    def with_deterministic_ids(
        cls, source_field: Union[str, dict[str, str]]
    ) -> "TransmogConfig":
        """Create a configuration with deterministic ID generation.

        Args:
            source_field: Field name to use for deterministic ID generation, or
                          a dictionary mapping table names to field names

        Returns:
            TransmogConfig with deterministic ID generation
        """
        return cls(metadata=MetadataConfig(default_id_field=source_field))

    @classmethod
    def with_custom_id_generation(
        cls, strategy: Callable[[dict[str, Any]], str]
    ) -> "TransmogConfig":
        """Create a configuration with custom ID generation."""
        return cls(metadata=MetadataConfig(id_generation_strategy=strategy))

    # Predefined configurations for common use cases

    @classmethod
    def simple_mode(cls) -> "TransmogConfig":
        """Create a simple configuration for basic usage with minimal metadata.

        This configuration focuses on producing clean output with simplified field names
        and minimal metadata fields, suitable for users who want readable output.
        """
        return cls(
            metadata=MetadataConfig(
                id_field="id",
                parent_field="parent_id",
                time_field="extract_time",
            ),
            processing=ProcessingConfig(
                cast_to_string=False,
            ),
        )

    @classmethod
    def streaming_optimized(cls) -> "TransmogConfig":
        """Create a configuration optimized for streaming processing.

        This configuration balances memory usage and performance
        for streaming large datasets directly to output files.
        """
        return (
            cls.memory_optimized()
            .with_processing(
                batch_size=500,
                cast_to_string=True,
            )
            .with_caching(
                maxsize=5000,
                clear_after_batch=True,
            )
        )

    @classmethod
    def error_tolerant(cls) -> "TransmogConfig":
        """Create a configuration that's tolerant of data errors.

        This configuration is useful for processing potentially problematic data
        by skipping errors rather than failing completely.
        """
        return cls(
            error_handling=ErrorHandlingConfig(
                recovery_strategy="skip",
                allow_malformed_data=True,
                max_retries=5,
            ),
            processing=ProcessingConfig(
                cast_to_string=True,
                skip_null=True,
            ),
        )

    @classmethod
    def csv_optimized(cls) -> "TransmogConfig":
        """Create a configuration optimized for CSV output.

        This configuration ensures all values are strings and
        field names are compatible with CSV requirements.
        """
        return cls(
            processing=ProcessingConfig(
                cast_to_string=True,
                include_empty=True,
                skip_null=False,
            ),
            naming=NamingConfig(
                separator="_",
            ),
        )

    # Component-specific configuration methods

    def with_naming(
        self,
        separator: Optional[str] = None,
        deeply_nested_threshold: Optional[int] = None,
    ) -> "TransmogConfig":
        """Update naming configuration.

        Args:
            separator: Separator character for path components
            deeply_nested_threshold: Threshold for when to consider a path deeply nested

        Returns:
            Updated configuration
        """
        naming = dataclasses.replace(self.naming)

        if separator is not None:
            validate_separator(separator)
            naming.separator = separator

        if deeply_nested_threshold is not None:
            naming.deeply_nested_threshold = deeply_nested_threshold

        return dataclasses.replace(self, naming=naming)

    def with_processing(self, **kwargs: Any) -> "TransmogConfig":
        """Create a new configuration with updated processing settings.

        Args:
            **kwargs: Processing options to update

        Returns:
            Updated configuration
        """
        # Validate parameters
        if "batch_size" in kwargs:
            validate_batch_size(kwargs["batch_size"])

        if "max_depth" in kwargs:
            validate_max_depth(kwargs["max_depth"])

        return TransmogConfig(
            naming=self.naming,
            processing=ProcessingConfig(**{**self.processing.__dict__, **kwargs}),
            metadata=self.metadata,
            error_handling=self.error_handling,
            cache_config=self.cache_config,
        )

    def with_metadata(self, **kwargs: Any) -> "TransmogConfig":
        """Create a new configuration with updated metadata settings.

        Args:
            **kwargs: Metadata options to update

        Returns:
            Updated configuration
        """
        # Validate parameters
        if "id_field" in kwargs:
            validate_field_name(kwargs["id_field"])

        if "parent_field" in kwargs:
            validate_field_name(kwargs["parent_field"])

        if "time_field" in kwargs:
            validate_field_name(kwargs["time_field"])

        if "default_id_field" in kwargs:
            validate_id_field_mapping(kwargs["default_id_field"])

        # Check for duplicate field names
        id_field = kwargs.get("id_field", self.metadata.id_field)
        parent_field = kwargs.get("parent_field", self.metadata.parent_field)
        time_field = kwargs.get("time_field", self.metadata.time_field)

        if (
            id_field == parent_field
            or id_field == time_field
            or parent_field == time_field
        ):
            raise ConfigurationError(
                f"Metadata field names must be unique. Got: "
                f"id={id_field}, parent={parent_field}, time={time_field}"
            )

        return TransmogConfig(
            naming=self.naming,
            processing=self.processing,
            metadata=MetadataConfig(**{**self.metadata.__dict__, **kwargs}),
            error_handling=self.error_handling,
            cache_config=self.cache_config,
        )

    def with_error_handling(self, **kwargs: Any) -> "TransmogConfig":
        """Create a new configuration with updated error handling settings.

        Args:
            **kwargs: Error handling options to update

        Returns:
            Updated configuration
        """
        # Validate parameters
        if "recovery_strategy" in kwargs:
            validate_recovery_strategy(kwargs["recovery_strategy"])

        return TransmogConfig(
            naming=self.naming,
            processing=self.processing,
            metadata=self.metadata,
            error_handling=ErrorHandlingConfig(
                **{**self.error_handling.__dict__, **kwargs}
            ),
            cache_config=self.cache_config,
        )

    def with_caching(
        self,
        enabled: Optional[bool] = None,
        maxsize: Optional[int] = None,
        clear_after_batch: Optional[bool] = None,
    ) -> "TransmogConfig":
        """Configure caching behavior.

        Args:
            enabled: Whether caching is enabled
            maxsize: Maximum size of the LRU cache
            clear_after_batch: Whether to clear cache after batch processing

        Returns:
            Updated TransmogConfig
        """
        cache_config = dataclasses.replace(self.cache_config)

        if enabled is not None:
            cache_config.enabled = enabled

        if maxsize is not None:
            validate_cache_size(maxsize)
            cache_config.maxsize = maxsize

        if clear_after_batch is not None:
            cache_config.clear_after_batch = clear_after_batch

        return dataclasses.replace(self, cache_config=cache_config)

    # Convenience methods for common scenarios

    def with_extraction(self, **kwargs: Any) -> "TransmogConfig":
        """Update extraction settings.

        A convenience alias for with_metadata() focusing on extraction settings.

        Args:
            default_id_field: Field name to use for deterministic ID generation
            id_generation_strategy: Custom ID generation strategy function
            **kwargs: Additional metadata settings

        Returns:
            Updated TransmogConfig
        """
        # Extract extraction-specific settings
        default_id_field = kwargs.pop("default_id_field", None)
        id_generation_strategy = kwargs.pop("id_generation_strategy", None)

        # Create metadata kwargs
        metadata_kwargs = kwargs.copy()
        if default_id_field is not None:
            metadata_kwargs["default_id_field"] = default_id_field
        if id_generation_strategy is not None:
            metadata_kwargs["id_generation_strategy"] = id_generation_strategy

        # Apply changes through metadata method
        return self.with_metadata(**metadata_kwargs)

    def with_validation(self, **kwargs: Any) -> "TransmogConfig":
        """Update validation settings.

        A convenience alias for with_processing() focusing on validation settings.

        Args:
            validate_field_names: Whether to validate field names
            validate_table_names: Whether to validate table names
            **kwargs: Additional processing settings

        Returns:
            Updated TransmogConfig
        """
        # Extract validation-specific settings
        validate_field_names = kwargs.pop("validate_field_names", None)
        validate_table_names = kwargs.pop("validate_table_names", None)

        # Create processing kwargs
        processing_kwargs = kwargs.copy()
        if validate_field_names is not None:
            processing_kwargs["validate_field_names"] = validate_field_names
        if validate_table_names is not None:
            processing_kwargs["validate_table_names"] = validate_table_names

        # Apply changes through processing method
        return self.with_processing(**processing_kwargs)

    def use_dot_notation(self) -> "TransmogConfig":
        """Configure to use dot notation for nested fields.

        This is a common requirement for many database systems and query languages.

        Returns:
            Updated TransmogConfig using dot notation
        """
        return self.with_naming(separator=".")

    def disable_arrays(self) -> "TransmogConfig":
        """Configure to skip array processing and keep arrays as JSON strings.

        This is useful when you want to keep arrays as single fields rather
        than creating child tables.

        Returns:
            Updated TransmogConfig with array processing disabled
        """
        return self.with_processing(visit_arrays=False)

    def keep_arrays(self) -> "TransmogConfig":
        """Configure to keep arrays in the main table after processing.

        When enabled, arrays will be processed into child tables but will also
        remain in the main table. This can be useful for backward compatibility
        or when the same array data is needed in both forms.

        Returns:
            Updated TransmogConfig with arrays kept in main table after processing
        """
        return self.with_processing(keep_arrays=True)

    def use_string_format(self) -> "TransmogConfig":
        """Configure all fields to be cast to strings.

        This is useful for formats that require string values, like CSV.

        Returns:
            Updated TransmogConfig with string casting enabled
        """
        return self.with_processing(cast_to_string=True)
