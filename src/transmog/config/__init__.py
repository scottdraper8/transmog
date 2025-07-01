"""Configuration package for Transmog.

Provides settings management, profile configuration, and extension points.
"""

import dataclasses
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, Union, cast

from transmog.config.settings import (
    TransmogSettings,
    configure as configure,
    load_config as load_config,
    load_profile as load_profile,
)
from transmog.error import ConfigurationError
from transmog.validation import (
    ParameterValidator,
    validate_config_parameters,
)

logger = logging.getLogger(__name__)

# Internal use only - use the tm.flatten() API
# These are still needed internally but not exported

# Global instances
settings = TransmogSettings()


class ProcessingMode(Enum):
    """Processing modes determining memory/performance tradeoff."""

    STANDARD = auto()  # Default mode
    LOW_MEMORY = auto()  # Memory usage optimization
    HIGH_PERFORMANCE = auto()  # Performance optimization


@dataclass
class NamingConfig:
    """Configuration for naming conventions."""

    separator: str = "_"
    nested_threshold: int = 4


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


def _default_time_field() -> str:
    """Factory function for default time field."""
    return "__transmog_datetime"


@dataclass
class MetadataConfig:
    """Configuration for metadata generation."""

    id_field: str = "__transmog_id"
    parent_field: str = "__parent_transmog_id"
    time_field: Optional[str] = field(default_factory=_default_time_field)
    default_id_field: Optional[Union[str, dict[str, str]]] = None
    id_generation_strategy: Optional[Callable[[dict[str, Any]], str]] = None
    force_transmog_id: bool = False
    id_field_patterns: Optional[list[str]] = None
    id_field_mapping: Optional[dict[str, str]] = None

    def __post_init__(self) -> None:
        """Validate metadata configuration after initialization."""
        # Check for duplicate field names only if all fields are non-None strings
        fields_to_check = []
        if self.id_field:
            fields_to_check.append(("id_field", self.id_field))
        if self.parent_field:
            fields_to_check.append(("parent_field", self.parent_field))
        if self.time_field:
            fields_to_check.append(("time_field", self.time_field))

        # Check for duplicates
        field_values = [field[1] for field in fields_to_check]
        if len(field_values) != len(set(field_values)):
            field_names = {field[1]: field[0] for field in fields_to_check}
            duplicates = [
                name
                for value in field_values
                if field_values.count(value) > 1
                for name in [field_names[value]]
            ]
            duplicates_str = ", ".join(set(duplicates))
            raise ConfigurationError(
                f"Metadata field names must be unique. "
                f"Duplicate fields found: {duplicates_str}"
            )


def validate_and_convert_param(value: Any, param_name: str, target_type: type) -> Any:
    """Validate and convert parameter with clear error messages.

    Args:
        value: The value to validate and convert
        param_name: Name of the parameter for error messages
        target_type: Target type to convert to

    Returns:
        Converted value

    Raises:
        ConfigurationError: If conversion fails
    """
    if value is None:
        return None
    try:
        return target_type(value)
    except (ValueError, TypeError) as e:
        raise ConfigurationError(
            f"Invalid {param_name}: cannot convert {value!r} to "
            f"{target_type.__name__}: {e}"
        ) from e


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

    def __init__(
        self,
        # Component configs (existing)
        naming: Optional[NamingConfig] = None,
        processing: Optional[ProcessingConfig] = None,
        metadata: Optional[MetadataConfig] = None,
        error_handling: Optional[ErrorHandlingConfig] = None,
        cache_config: Optional[CacheConfig] = None,
        # Convenience parameters
        batch_size: Optional[int] = None,
        separator: Optional[str] = None,
        cast_to_string: Optional[bool] = None,
        include_empty: Optional[bool] = None,
        skip_null: Optional[bool] = None,
        visit_arrays: Optional[bool] = None,
        nested_threshold: Optional[int] = None,
        id_field: Optional[str] = None,
        parent_field: Optional[str] = None,
        time_field: Optional[str] = "UNSPECIFIED",
        recovery_strategy: Optional[str] = None,
        allow_malformed_data: Optional[bool] = None,
        cache_enabled: Optional[bool] = None,
        cache_maxsize: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize TransmogConfig with component configs or convenience parameters.

        Args:
            naming: Complete naming configuration
            processing: Complete processing configuration
            metadata: Complete metadata configuration
            error_handling: Complete error handling configuration
            cache_config: Complete cache configuration

            # Convenience parameters
            batch_size: Batch size for processing
            separator: Separator character for path components
            cast_to_string: Whether to cast all values to strings
            include_empty: Whether to include empty values
            skip_null: Whether to skip null values
            visit_arrays: Whether to process arrays into child tables
            nested_threshold: Threshold for deeply nested paths
            id_field: ID field name for metadata
            parent_field: Parent ID field name for metadata
            time_field: Timestamp field name for metadata (None to disable)
            recovery_strategy: Error recovery strategy
            allow_malformed_data: Whether to allow malformed data
            cache_enabled: Whether to enable caching
            cache_maxsize: Maximum cache size
            **kwargs: Additional parameters for component configs
        """
        # Start with default component configs
        self.naming = naming or NamingConfig()
        self.processing = processing or ProcessingConfig()
        self.metadata = metadata or MetadataConfig()
        self.error_handling = error_handling or ErrorHandlingConfig()
        self.cache_config = cache_config or CacheConfig()

        # Apply convenience parameters to appropriate components with validation
        if batch_size is not None:
            batch_size = cast(
                int, validate_and_convert_param(batch_size, "batch_size", int)
            )
            validate_config_parameters(batch_size=batch_size)
            self.processing = dataclasses.replace(
                self.processing, batch_size=batch_size
            )

        if separator is not None:
            validate_config_parameters(separator=separator)
            self.naming = dataclasses.replace(self.naming, separator=separator)

        if cast_to_string is not None:
            self.processing = dataclasses.replace(
                self.processing, cast_to_string=cast_to_string
            )

        if include_empty is not None:
            self.processing = dataclasses.replace(
                self.processing, include_empty=include_empty
            )

        if skip_null is not None:
            self.processing = dataclasses.replace(self.processing, skip_null=skip_null)

        if visit_arrays is not None:
            self.processing = dataclasses.replace(
                self.processing, visit_arrays=visit_arrays
            )

        if nested_threshold is not None:
            nested_threshold = cast(
                int,
                validate_and_convert_param(nested_threshold, "nested_threshold", int),
            )
            self.naming = dataclasses.replace(
                self.naming, nested_threshold=nested_threshold
            )

        if id_field is not None:
            validate_config_parameters(id_field=id_field)
            self.metadata = dataclasses.replace(self.metadata, id_field=id_field)

        if parent_field is not None:
            validate_config_parameters(parent_field=parent_field)
            self.metadata = dataclasses.replace(
                self.metadata, parent_field=parent_field
            )

        # Handle time_field parameter - only modify if explicitly provided
        if time_field != "UNSPECIFIED":
            # Explicitly set time_field (including None to disable)
            if (
                time_field is not None and time_field
            ):  # Only validate non-empty, non-None strings
                validate_config_parameters(time_field=time_field)
            self.metadata = dataclasses.replace(self.metadata, time_field=time_field)

        if recovery_strategy is not None:
            validate_config_parameters(recovery_strategy=recovery_strategy)
            self.error_handling = dataclasses.replace(
                self.error_handling, recovery_strategy=recovery_strategy
            )

        if allow_malformed_data is not None:
            self.error_handling = dataclasses.replace(
                self.error_handling, allow_malformed_data=allow_malformed_data
            )

        if cache_enabled is not None:
            self.cache_config = dataclasses.replace(
                self.cache_config, enabled=cache_enabled
            )

        if cache_maxsize is not None:
            cache_maxsize = cast(
                int, validate_and_convert_param(cache_maxsize, "cache_maxsize", int)
            )
            validate_config_parameters(cache_maxsize=cache_maxsize)
            self.cache_config = dataclasses.replace(
                self.cache_config, maxsize=cache_maxsize
            )

        # Final validation happens in MetadataConfig.__post_init__

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

    @classmethod
    def with_natural_ids(
        cls,
        id_field_patterns: Optional[list[str]] = None,
        id_field_mapping: Optional[dict[str, str]] = None,
    ) -> "TransmogConfig":
        """Create a configuration that discovers and uses natural IDs from data.

        Args:
            id_field_patterns: List of field names to check for natural IDs.
                               If None, uses default patterns (id, ID, uuid, etc.)
            id_field_mapping: Optional mapping of table names to specific ID field names

        Returns:
            TransmogConfig with natural ID discovery enabled
        """
        return cls(
            metadata=MetadataConfig(
                id_field_patterns=id_field_patterns,
                id_field_mapping=id_field_mapping,
                force_transmog_id=False,
            )
        )

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
                time_field="transmog_time",
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

    @classmethod
    def json_optimized(cls) -> "TransmogConfig":
        """Create a configuration optimized for JSON output.

        This configuration preserves data types and produces clean JSON
        with minimal metadata overhead.
        """
        return cls(
            processing=ProcessingConfig(
                cast_to_string=False,  # JSON can handle native types
                include_empty=True,  # JSON can represent null/empty values
                skip_null=False,  # Include nulls in JSON
            ),
            naming=NamingConfig(
                separator="_",
            ),
        )

    @classmethod
    def parquet_optimized(cls) -> "TransmogConfig":
        """Create a configuration optimized for Parquet output.

        This configuration preserves data types and optimizes for
        columnar storage efficiency.
        """
        return cls(
            processing=ProcessingConfig(
                cast_to_string=False,  # Parquet handles native types well
                include_empty=False,  # Parquet efficiently handles nulls
                skip_null=True,  # Skip nulls for better compression
                batch_size=10000,  # Larger batches for Parquet efficiency
                processing_mode=ProcessingMode.HIGH_PERFORMANCE,
            ),
            naming=NamingConfig(
                separator="_",  # Parquet column names work well with underscores
            ),
            cache_config=CacheConfig(
                enabled=True,
                maxsize=50000,  # Larger cache for performance
                clear_after_batch=False,
            ),
        )

    # Component-specific configuration methods

    def with_naming(
        self,
        separator: Optional[str] = None,
        nested_threshold: Optional[int] = None,
    ) -> "TransmogConfig":
        """Update naming configuration.

        Args:
            separator: Separator character for path components
            nested_threshold: Threshold for when to consider a path deeply nested

        Returns:
            Updated configuration
        """
        naming = dataclasses.replace(self.naming)

        if separator is not None:
            ParameterValidator.validate_separator(separator, "config")
            naming.separator = separator

        if nested_threshold is not None:
            naming.nested_threshold = nested_threshold

        return dataclasses.replace(self, naming=naming)

    def with_processing(self, **kwargs: Any) -> "TransmogConfig":
        """Create a configuration with updated processing settings.

        Args:
            **kwargs: Processing options to update

        Returns:
            Updated configuration
        """
        # Validate parameters
        if "batch_size" in kwargs:
            ParameterValidator.validate_batch_size(kwargs["batch_size"], "config")

        if "max_depth" in kwargs:
            ParameterValidator.validate_max_depth(kwargs["max_depth"], "config")

        return TransmogConfig(
            naming=self.naming,
            processing=ProcessingConfig(**{**self.processing.__dict__, **kwargs}),
            metadata=self.metadata,
            error_handling=self.error_handling,
            cache_config=self.cache_config,
        )

    def with_metadata(self, **kwargs: Any) -> "TransmogConfig":
        """Create a configuration with updated metadata settings.

        Args:
            **kwargs: Metadata options to update

        Returns:
            Updated configuration
        """
        # Validate parameters
        if "id_field" in kwargs:
            ParameterValidator.validate_field_name(kwargs["id_field"], "config")

        if "parent_field" in kwargs:
            ParameterValidator.validate_field_name(kwargs["parent_field"], "config")

        if "time_field" in kwargs:
            ParameterValidator.validate_field_name(kwargs["time_field"], "config")

        if "default_id_field" in kwargs:
            ParameterValidator.validate_id_field(kwargs["default_id_field"], "config")

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
        """Create a configuration with updated error handling settings.

        Args:
            **kwargs: Error handling options to update

        Returns:
            Updated configuration
        """
        # Validate parameters
        if "recovery_strategy" in kwargs:
            ParameterValidator.validate_recovery_strategy(
                kwargs["recovery_strategy"], "config"
            )

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
            ParameterValidator.validate_cache_size(maxsize, "config")
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

        This setting is useful when arrays should be kept as single fields rather
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
