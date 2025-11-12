"""Configuration for Transmog processing."""

from dataclasses import dataclass

from transmog.exceptions import ConfigurationError
from transmog.types import ArrayMode


@dataclass
class TransmogConfig:
    """Configuration for Transmog processing with sensible defaults.

    All parameters have sensible defaults that work for most use cases.
    Use factory methods for common scenarios or customize parameters as needed.
    """

    # === Data Transformation ===
    array_mode: ArrayMode = ArrayMode.SMART
    """Strategy for handling arrays: SMART (default), SEPARATE, INLINE, or SKIP."""

    include_nulls: bool = False
    """Include null and empty values in output. False (default) skips them."""

    max_depth: int = 100
    """Maximum recursion depth to prevent stack overflow."""

    # === ID and Metadata ===
    id_generation: str | list[str] = "random"
    """ID generation strategy.

    Options:
        - "random" (default): Generate random UUID for each record
        - "natural": Use existing ID from source field
        - "hash": Generate deterministic hash of entire record
        - ["field1", "field2"]: Generate deterministic hash of specific fields
    """

    id_field: str = "_id"
    """Field name for record IDs (generated or existing)."""

    parent_field: str = "_parent_id"
    """Field name for parent relationship references."""

    time_field: str | None = "_timestamp"
    """Field name for timestamps. Set to None to disable timestamp tracking."""

    # === Processing Control ===
    batch_size: int = 1000
    """Number of records to process at once for memory efficiency."""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.batch_size < 1:
            raise ConfigurationError("Batch size must be at least 1")

        if self.max_depth < 1:
            raise ConfigurationError("Max depth must be at least 1")

        if not isinstance(self.include_nulls, bool):
            raise ConfigurationError(
                f"include_nulls must be a boolean, "
                f"got {type(self.include_nulls).__name__}"
            )

        if isinstance(self.id_generation, str):
            valid_strategies = {"random", "natural", "hash"}
            if self.id_generation not in valid_strategies:
                raise ConfigurationError(
                    f"id_generation must be one of {valid_strategies} "
                    f"or a list of field names, got {self.id_generation!r}"
                )
        elif isinstance(self.id_generation, list):
            if not self.id_generation:
                raise ConfigurationError("id_generation list cannot be empty")
            if not all(isinstance(f, str) for f in self.id_generation):
                raise ConfigurationError("id_generation list must contain only strings")
        else:
            raise ConfigurationError(
                f"id_generation must be a string or list, "
                f"got {type(self.id_generation).__name__}"
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


__all__ = ["TransmogConfig"]
