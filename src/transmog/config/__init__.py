"""Configuration package for Transmog.

Provides configuration management with a single TransmogConfig class.
"""

from dataclasses import dataclass
from typing import Optional

from transmog.error import ConfigurationError
from transmog.types import ArrayMode, NullHandling, RecoveryMode


@dataclass
class TransmogConfig:
    """Configuration for Transmog processing with sensible defaults.

    All parameters have sensible defaults that work for most use cases.
    Use factory methods for common scenarios or customize parameters as needed.
    """

    separator: str = "_"
    """Character to join nested field names."""

    cast_to_string: bool = False
    """Convert all values to strings for CSV compatibility."""

    null_handling: NullHandling = NullHandling.SKIP
    """How to handle null and empty values: SKIP (default) or INCLUDE."""

    array_mode: ArrayMode = ArrayMode.SMART
    """Strategy for handling arrays: SMART (default), SEPARATE, INLINE, or SKIP."""

    batch_size: int = 1000
    """Number of records to process at once for memory efficiency."""

    max_depth: int = 100
    """Maximum recursion depth to prevent stack overflow."""

    id_field: str = "_id"
    """Field name for record IDs in flattened output."""

    parent_field: str = "_parent_id"
    """Field name for parent relationship references."""

    time_field: Optional[str] = "_timestamp"
    """Field name for timestamps. Set to None to disable timestamp tracking."""

    id_patterns: Optional[list[str]] = None
    """List of field name patterns to check for natural IDs."""

    deterministic_ids: bool = False
    """Generate deterministic IDs based on record content instead of random UUIDs."""

    id_fields: Optional[list[str]] = None
    """List of field names to use for composite deterministic IDs.

    If None, uses entire record.
    """

    recovery_mode: RecoveryMode = RecoveryMode.STRICT
    """Strategy for handling errors: STRICT (default) or SKIP."""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.separator:
            raise ConfigurationError("Separator cannot be empty")

        if self.batch_size < 1:
            raise ConfigurationError("Batch size must be at least 1")

        if self.max_depth < 1:
            raise ConfigurationError("Max depth must be at least 1")

        if not isinstance(self.recovery_mode, RecoveryMode):
            raise ConfigurationError(
                f"recovery_mode must be a RecoveryMode enum value, "
                f"got {type(self.recovery_mode).__name__}"
            )

        if not isinstance(self.null_handling, NullHandling):
            raise ConfigurationError(
                f"null_handling must be a NullHandling enum value, "
                f"got {type(self.null_handling).__name__}"
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
        )

    @classmethod
    def for_csv(cls) -> "TransmogConfig":
        """Create configuration optimized for CSV output.

        Equivalent to:
            TransmogConfig(
                null_handling=NullHandling.INCLUDE,
                cast_to_string=True
            )

        This configuration ensures all values are strings and includes
        empty/null values to maintain consistent CSV column structure.
        """
        return cls(
            null_handling=NullHandling.INCLUDE,
            cast_to_string=True,
        )

    @classmethod
    def error_tolerant(cls) -> "TransmogConfig":
        """Create error-tolerant configuration that continues processing on errors.

        Equivalent to:
            TransmogConfig(
                recovery_mode=RecoveryMode.SKIP
            )

        This configuration skips malformed records and allows processing
        to continue even when encountering data quality issues.
        """
        return cls(
            recovery_mode=RecoveryMode.SKIP,
        )


__all__ = ["TransmogConfig", "ArrayMode", "NullHandling", "RecoveryMode"]
