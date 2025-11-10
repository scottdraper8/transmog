"""Configuration package for Transmog.

Provides configuration management with a single TransmogConfig class.
"""

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Union

from transmog.error import ConfigurationError
from transmog.types.base import ArrayMode, RecoveryMode

logger = logging.getLogger(__name__)


@dataclass
class TransmogConfig:
    """Configuration for Transmog processing with sensible defaults.

    All parameters have sensible defaults that work for most use cases.
    Use factory methods for common scenarios or customize parameters as needed.
    """

    # Naming configuration (2 parameters)
    separator: str = "_"
    """Character to join nested field names (e.g., 'user.name' becomes 'user_name')."""

    nested_threshold: int = 4
    """Depth at which to simplify deeply nested field names to prevent long names."""

    # Processing configuration (6 parameters)
    cast_to_string: bool = False
    """Convert all values to strings for CSV compatibility."""

    include_empty: bool = False
    """Include empty values in output instead of omitting them."""

    skip_null: bool = True
    """Skip null values instead of including them as empty strings."""

    array_mode: ArrayMode = ArrayMode.SMART
    """Strategy for handling arrays: SMART (default), SEPARATE, INLINE, or SKIP."""

    batch_size: int = 1000
    """Number of records to process at once for memory efficiency."""

    max_depth: int = 100
    """Maximum recursion depth to prevent stack overflow on pathological data."""

    # Metadata configuration (3 parameters)
    id_field: str = "_id"
    """Field name for record IDs in flattened output."""

    parent_field: str = "_parent_id"
    """Field name for parent relationship references."""

    time_field: Optional[str] = "_timestamp"
    """Field name for timestamps. Set to None to disable timestamp tracking."""

    # ID Discovery configuration (1 parameter)
    id_patterns: Optional[list[str]] = None
    """List of field name patterns to check for natural IDs."""

    # Error handling configuration (2 parameters)
    recovery_mode: RecoveryMode = RecoveryMode.STRICT
    """Strategy for handling errors: STRICT (default), SKIP, or PARTIAL."""

    allow_malformed_data: bool = False
    """Allow processing of malformed data that would normally cause errors."""

    # Cache configuration (1 parameter)
    cache_size: int = 10000
    """Maximum cache size for value processing optimizations. Set to 0 to disable."""

    # Advanced configuration (1 parameter)
    id_generator: Optional[Callable[[dict[str, Any]], str]] = None
    """Custom function to generate IDs for records that lack natural identifiers."""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.separator:
            raise ConfigurationError("Separator cannot be empty")

        if self.nested_threshold < 2:
            raise ConfigurationError("Nested threshold must be at least 2")

        if self.batch_size < 1:
            raise ConfigurationError("Batch size must be at least 1")

        if self.max_depth < 1:
            raise ConfigurationError("Max depth must be at least 1")

        if self.cache_size < 0:
            raise ConfigurationError("Cache size must be non-negative")

        if not isinstance(self.recovery_mode, RecoveryMode):
            raise ConfigurationError(
                f"recovery_mode must be a RecoveryMode enum value, "
                f"got {type(self.recovery_mode).__name__}"
            )

        fields_to_check = []
        if self.id_field:
            fields_to_check.append(self.id_field)
        if self.parent_field:
            fields_to_check.append(self.parent_field)
        if self.time_field:
            fields_to_check.append(self.time_field)

        if len(fields_to_check) != len(set(fields_to_check)):
            raise ConfigurationError(
                f"Metadata field names must be unique: "
                f"id={self.id_field}, parent={self.parent_field}, "
                f"time={self.time_field}"
            )

    def validate_config(self) -> list[str]:
        """Return list of validation warnings for problematic configurations.

        These are warnings, not errors - the configuration will still work,
        but may not be optimal for the intended use case.

        Returns:
            List of warning messages. Empty list means no warnings.
        """
        warnings = []

        # Performance warnings
        if self.batch_size > 50000:
            warnings.append(
                f"Large batch_size ({self.batch_size}) may cause memory issues. "
                "Consider values < 50000 for most use cases."
            )

        if self.max_depth > 500:
            warnings.append(
                f"High max_depth ({self.max_depth}) may cause stack overflow. "
                "Consider values under 500 for most data structures."
            )

        if self.cache_size > 100000:
            warnings.append(
                f"Large cache_size ({self.cache_size}) may use excessive memory. "
                "Consider values < 100000 unless processing very large datasets."
            )

        # Configuration interaction warnings
        if self.include_empty and not self.skip_null:
            warnings.append("include_empty=True with skip_null=False may confuse.")

        if self.cast_to_string and self.array_mode == ArrayMode.INLINE:
            warnings.append(
                "cast_to_string=True with INLINE may create very long strings."
            )

        if self.allow_malformed_data and self.recovery_mode == RecoveryMode.STRICT:
            warnings.append(
                "allow_malformed_data=True with STRICT may still raise errors."
            )

        return warnings

    @classmethod
    def for_memory(cls) -> "TransmogConfig":
        """Create memory-optimized configuration."""
        return cls(
            batch_size=100,
            cache_size=1000,
        )

    @classmethod
    def for_csv(cls) -> "TransmogConfig":
        """Create configuration optimized for CSV output.

        Equivalent to:
            TransmogConfig(
                include_empty=True,
                skip_null=False,
                cast_to_string=True
            )

        This configuration ensures all values are strings and includes
        empty/null values to maintain consistent CSV column structure.
        """
        return cls(
            include_empty=True,
            skip_null=False,
            cast_to_string=True,
        )

    @classmethod
    def for_parquet(cls) -> "TransmogConfig":
        """Create performance-optimized configuration for large datasets.

        Equivalent to:
            TransmogConfig(
                batch_size=10000,
                cache_size=50000
            )

        This configuration optimizes for speed when processing large datasets
        or Parquet files by using larger batches and cache sizes.
        """
        return cls(
            batch_size=10000,
            cache_size=50000,
        )

    @classmethod
    def simple(cls) -> "TransmogConfig":
        """Create configuration with clean, readable metadata field names.

        Equivalent to:
            TransmogConfig(
                id_field="id",
                parent_field="parent_id",
                time_field="timestamp"
            )

        This configuration uses more conventional field names instead of
        underscore prefixes, making output more readable.
        """
        return cls(
            id_field="id",
            parent_field="parent_id",
            time_field="timestamp",
        )

    @classmethod
    def error_tolerant(cls) -> "TransmogConfig":
        """Create error-tolerant configuration that continues processing on errors.

        Equivalent to:
            TransmogConfig(
                recovery_mode=RecoveryMode.SKIP,
                allow_malformed_data=True
            )

        This configuration skips malformed records and allows processing
        to continue even when encountering data quality issues.
        """
        return cls(
            recovery_mode=RecoveryMode.SKIP,
            allow_malformed_data=True,
        )

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "TransmogConfig":
        """Load configuration from JSON, YAML, or TOML file.

        Args:
            path: Path to configuration file

        Raises:
            ConfigurationError: If file not found or invalid format
        """
        path = Path(path)

        if not path.exists():
            raise ConfigurationError(f"Configuration file not found: {path}")

        file_ext = path.suffix.lower()

        try:
            if file_ext == ".json":
                with open(path) as f:
                    data = json.load(f)
            elif file_ext in (".yaml", ".yml"):
                try:
                    import yaml

                    with open(path) as f:
                        data = yaml.safe_load(f)
                except ImportError as e:
                    raise ConfigurationError(
                        "PyYAML required for YAML support: pip install PyYAML"
                    ) from e
            elif file_ext in (".toml", ".tml"):
                try:
                    import tomli

                    with open(path, "rb") as f:
                        data = tomli.load(f)
                except ImportError:
                    try:
                        import toml

                        with open(path) as f:
                            data = toml.load(f)
                    except ImportError as e:
                        raise ConfigurationError(
                            "tomli or toml required for TOML support: pip install tomli"
                        ) from e
            else:
                raise ConfigurationError(
                    f"Unsupported file format: {file_ext}. "
                    f"Supported: .json, .yaml, .yml, .toml"
                )

            return cls(**data)

        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON in config file: {e}") from e
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(f"Failed to load config: {e}") from e

    @classmethod
    def from_env(cls, prefix: str = "TRANSMOG_") -> "TransmogConfig":
        """Load configuration from environment variables.

        Args:
            prefix: Prefix for environment variables
        """
        config_dict = {}

        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Strip prefix and convert to lowercase
                config_key = key[len(prefix) :].lower()

                # Try to parse as JSON for complex types
                try:
                    parsed_value = json.loads(value)
                    config_dict[config_key] = parsed_value
                except (json.JSONDecodeError, ValueError):
                    # Keep as string if not valid JSON
                    config_dict[config_key] = value

        return cls(**config_dict)


__all__ = ["TransmogConfig", "ArrayMode", "RecoveryMode"]
