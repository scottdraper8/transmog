"""Feature detection for Transmog.

This module performs feature detection at import time to allow
conditional functionality based on available dependencies.
"""

from .dependencies import DependencyManager


class Features:
    """Feature detection for optional functionality.

    This class detects and caches information about available
    optional features based on installed packages.
    """

    # Feature flags
    HAS_PYARROW = False
    HAS_ORJSON = False
    HAS_TYPING_EXTENSIONS = False
    HAS_POLARS = False

    HAS_PARQUET_SUPPORT = False
    HAS_FAST_JSON = False
    HAS_ENHANCED_TYPING = False
    HAS_FAST_CSV = False

    @classmethod
    def detect_features(cls) -> None:
        """Detect available optional features.

        This method checks for all optional dependencies and
        sets the appropriate feature flags.
        """
        # Check individual packages
        cls.HAS_PYARROW = DependencyManager.has_dependency("pyarrow")
        cls.HAS_ORJSON = DependencyManager.has_dependency("orjson")
        cls.HAS_TYPING_EXTENSIONS = DependencyManager.has_dependency(
            "typing_extensions"
        )
        cls.HAS_POLARS = DependencyManager.has_dependency("polars")

        # Check complete features
        cls.HAS_PARQUET_SUPPORT = DependencyManager.has_feature("parquet")
        cls.HAS_FAST_JSON = DependencyManager.has_feature("fast_json")
        cls.HAS_ENHANCED_TYPING = DependencyManager.has_feature("typing")
        cls.HAS_FAST_CSV = cls.HAS_POLARS  # Polars enables fast CSV processing

    @classmethod
    def has_pyarrow(cls) -> bool:
        """Check if PyArrow is available."""
        return cls.HAS_PYARROW

    @classmethod
    def has_orjson(cls) -> bool:
        """Check if orjson is available."""
        return cls.HAS_ORJSON

    @classmethod
    def has_polars(cls) -> bool:
        """Check if Polars is available."""
        return cls.HAS_POLARS


# Run detection at import time
Features.detect_features()
