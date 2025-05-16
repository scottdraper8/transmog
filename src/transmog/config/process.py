"""Processing configuration for Transmog."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class ProcessingMode(str, Enum):
    """Processing mode enum for Transmog."""

    STANDARD = "standard"
    HIGH_PERFORMANCE = "high_performance"
    LOW_MEMORY = "low_memory"


@dataclass
class ProcessingConfig:
    """Configuration for processing options."""

    cast_to_string: bool = True
    include_empty: bool = False
    skip_null: bool = True
    visit_arrays: bool = False
    batch_size: int = 1000
    processing_mode: ProcessingMode = ProcessingMode.STANDARD
    optimize_for_memory: Optional[bool] = None
    optimize_for_performance: Optional[bool] = None
    memory_threshold: Optional[int] = (
        None  # Threshold in bytes for switching to low-memory mode
    )
    memory_tracking_enabled: bool = (
        False  # Controls memory usage tracking during processing
    )
    additional_options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Convert string processing mode to enum if needed."""
        if isinstance(self.processing_mode, str):
            try:
                self.processing_mode = ProcessingMode(self.processing_mode.lower())
            except ValueError:
                self.processing_mode = ProcessingMode.STANDARD

        # Apply optimization flags to processing mode
        if self.optimize_for_memory and self.processing_mode == ProcessingMode.STANDARD:
            self.processing_mode = ProcessingMode.LOW_MEMORY
        elif (
            self.optimize_for_performance
            and self.processing_mode == ProcessingMode.STANDARD
        ):
            self.processing_mode = ProcessingMode.HIGH_PERFORMANCE

        # Set default memory threshold
        if self.memory_threshold is None:
            self.memory_threshold = 100 * 1024 * 1024  # 100MB default
