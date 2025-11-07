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
    """Configuration for Transmog processing with sensible defaults."""

    # Naming (2)
    separator: str = "_"
    nested_threshold: int = 4

    # Processing (6)
    cast_to_string: bool = False
    include_empty: bool = False
    skip_null: bool = True
    array_mode: ArrayMode = ArrayMode.SMART
    batch_size: int = 1000
    max_depth: int = 100

    # Metadata (3)
    id_field: str = "_id"
    parent_field: str = "_parent_id"
    time_field: Optional[str] = "_timestamp"

    # ID Discovery (1)
    id_patterns: Optional[list[str]] = None

    # Error handling (2)
    recovery_mode: RecoveryMode = RecoveryMode.STRICT
    allow_malformed_data: bool = False

    # Cache (1)
    cache_size: int = 10000

    # Advanced options (1)
    id_generator: Optional[Callable[[dict[str, Any]], str]] = None

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

    @classmethod
    def for_memory(cls) -> "TransmogConfig":
        """Create memory-optimized configuration."""
        return cls(
            batch_size=100,
            cache_size=1000,
        )

    @classmethod
    def for_performance(cls) -> "TransmogConfig":
        """Create performance-optimized configuration."""
        return cls(
            batch_size=10000,
            cache_size=50000,
        )

    @classmethod
    def for_csv(cls) -> "TransmogConfig":
        """Create configuration optimized for CSV output."""
        return cls(
            include_empty=True,
            skip_null=False,
            cast_to_string=True,
        )

    @classmethod
    def for_parquet(cls) -> "TransmogConfig":
        """Create configuration optimized for Parquet output."""
        return cls(
            batch_size=10000,
            cache_size=50000,
        )

    @classmethod
    def simple(cls) -> "TransmogConfig":
        """Create configuration with clean metadata field names."""
        return cls(
            id_field="id",
            parent_field="parent_id",
            time_field="timestamp",
        )

    @classmethod
    def error_tolerant(cls) -> "TransmogConfig":
        """Create configuration that skips errors."""
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
