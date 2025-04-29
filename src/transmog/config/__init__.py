"""
Configuration package for Transmog.

Provides settings management, profile configuration, and extension points.
"""

from transmog.config.settings import (
    settings,
    extensions,
    load_profile,
    load_config,
    configure,
    TransmogSettings,
    ExtensionRegistry,
)

from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, Any, List
from enum import Enum, auto


class ProcessingMode(Enum):
    """Processing modes determining memory/performance tradeoff."""

    STANDARD = auto()  # Default mode
    LOW_MEMORY = auto()  # Optimize for memory usage
    HIGH_PERFORMANCE = auto()  # Optimize for performance


@dataclass
class NamingConfig:
    """Configuration for naming conventions and abbreviations."""

    separator: str = "_"
    abbreviate_table_names: bool = True
    abbreviate_field_names: bool = True
    max_table_component_length: Optional[int] = None
    max_field_component_length: Optional[int] = None
    preserve_leaf_component: bool = True
    custom_abbreviations: Dict[str, str] = field(default_factory=dict)


@dataclass
class ProcessingConfig:
    """Configuration for data processing options."""

    cast_to_string: bool = True
    include_empty: bool = False
    skip_null: bool = True
    max_nesting_depth: Optional[int] = None
    path_parts_optimization: bool = True
    visit_arrays: bool = False
    batch_size: int = 1000
    processing_mode: ProcessingMode = ProcessingMode.STANDARD


@dataclass
class MetadataConfig:
    """Configuration for metadata generation."""

    id_field: str = "__extract_id"
    parent_field: str = "__parent_extract_id"
    time_field: str = "__extract_datetime"
    deterministic_id_fields: Dict[str, str] = field(default_factory=dict)
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None


@dataclass
class ErrorHandlingConfig:
    """Configuration for error handling and recovery."""

    allow_malformed_data: bool = False
    recovery_strategy: str = "strict"  # "strict", "skip", "partial"
    max_retries: int = 3
    error_log_path: Optional[str] = None


@dataclass
class TransmogConfig:
    """Complete configuration for Transmog processing."""

    naming: NamingConfig = field(default_factory=NamingConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    error_handling: ErrorHandlingConfig = field(default_factory=ErrorHandlingConfig)

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
            )
        )

    @classmethod
    def performance_optimized(cls) -> "TransmogConfig":
        """Create a performance-optimized configuration."""
        return cls(
            processing=ProcessingConfig(
                processing_mode=ProcessingMode.HIGH_PERFORMANCE,
                batch_size=10000,
                path_parts_optimization=True,
            )
        )

    @classmethod
    def with_deterministic_ids(cls, id_fields: Dict[str, str]) -> "TransmogConfig":
        """Create a configuration with deterministic ID generation."""
        return cls(metadata=MetadataConfig(deterministic_id_fields=id_fields))

    @classmethod
    def with_custom_id_generation(
        cls, strategy: Callable[[Dict[str, Any]], str]
    ) -> "TransmogConfig":
        """Create a configuration with custom ID generation."""
        return cls(metadata=MetadataConfig(id_generation_strategy=strategy))

    def with_naming(self, **kwargs) -> "TransmogConfig":
        """Create a new configuration with updated naming settings."""
        return TransmogConfig(
            naming=NamingConfig(**{**self.naming.__dict__, **kwargs}),
            processing=self.processing,
            metadata=self.metadata,
            error_handling=self.error_handling,
        )

    def with_processing(self, **kwargs) -> "TransmogConfig":
        """Create a new configuration with updated processing settings."""
        return TransmogConfig(
            naming=self.naming,
            processing=ProcessingConfig(**{**self.processing.__dict__, **kwargs}),
            metadata=self.metadata,
            error_handling=self.error_handling,
        )

    def with_metadata(self, **kwargs) -> "TransmogConfig":
        """Create a new configuration with updated metadata settings."""
        return TransmogConfig(
            naming=self.naming,
            processing=self.processing,
            metadata=MetadataConfig(**{**self.metadata.__dict__, **kwargs}),
            error_handling=self.error_handling,
        )

    def with_error_handling(self, **kwargs) -> "TransmogConfig":
        """Create a new configuration with updated error handling settings."""
        return TransmogConfig(
            naming=self.naming,
            processing=self.processing,
            metadata=self.metadata,
            error_handling=ErrorHandlingConfig(
                **{**self.error_handling.__dict__, **kwargs}
            ),
        )


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
