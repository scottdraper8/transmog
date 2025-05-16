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
    """Configuration for naming conventions and abbreviations."""

    separator: str = "_"
    abbreviate_table_names: bool = True
    abbreviate_field_names: bool = True
    max_table_component_length: Optional[int] = None
    max_field_component_length: Optional[int] = None
    preserve_root_component: bool = True
    preserve_leaf_component: bool = True
    custom_abbreviations: dict[str, str] = field(default_factory=dict)


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
            ),
            cache_config=CacheConfig(
                enabled=True,
                maxsize=50000,  # Enlarged cache for performance
                clear_after_batch=False,
            ),
        )

    @classmethod
    def with_deterministic_ids(cls, source_field: str) -> "TransmogConfig":
        """Create a configuration with deterministic ID generation.

        Args:
            source_field: Field name to use for deterministic ID generation

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

    def with_naming(
        self,
        separator: Optional[str] = None,
        abbreviate_table_names: Optional[bool] = None,
        abbreviate_field_names: Optional[bool] = None,
        max_table_component_length: Optional[int] = None,
        max_field_component_length: Optional[int] = None,
        preserve_root_component: Optional[bool] = None,
        preserve_leaf_component: Optional[bool] = None,
        custom_abbreviations: Optional[dict[str, str]] = None,
    ) -> "TransmogConfig":
        """Update naming configuration."""
        naming = dataclasses.replace(self.naming)

        if separator is not None:
            naming.separator = separator

        if abbreviate_table_names is not None:
            naming.abbreviate_table_names = abbreviate_table_names

        if abbreviate_field_names is not None:
            naming.abbreviate_field_names = abbreviate_field_names

        if max_table_component_length is not None:
            naming.max_table_component_length = max_table_component_length

        if max_field_component_length is not None:
            naming.max_field_component_length = max_field_component_length

        if preserve_root_component is not None:
            naming.preserve_root_component = preserve_root_component

        if preserve_leaf_component is not None:
            naming.preserve_leaf_component = preserve_leaf_component

        if custom_abbreviations is not None:
            naming.custom_abbreviations = custom_abbreviations

        return dataclasses.replace(self, naming=naming)

    def with_processing(self, **kwargs: Any) -> "TransmogConfig":
        """Create a new configuration with updated processing settings."""
        return TransmogConfig(
            naming=self.naming,
            processing=ProcessingConfig(**{**self.processing.__dict__, **kwargs}),
            metadata=self.metadata,
            error_handling=self.error_handling,
            cache_config=self.cache_config,
        )

    def with_metadata(self, **kwargs: Any) -> "TransmogConfig":
        """Create a new configuration with updated metadata settings."""
        return TransmogConfig(
            naming=self.naming,
            processing=self.processing,
            metadata=MetadataConfig(**{**self.metadata.__dict__, **kwargs}),
            error_handling=self.error_handling,
            cache_config=self.cache_config,
        )

    def with_error_handling(self, **kwargs: Any) -> "TransmogConfig":
        """Create a new configuration with updated error handling settings."""
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
            cache_config.maxsize = maxsize

        if clear_after_batch is not None:
            cache_config.clear_after_batch = clear_after_batch

        return dataclasses.replace(self, cache_config=cache_config)

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
]
