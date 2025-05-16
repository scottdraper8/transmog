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

    # Initialize feature flags
    HAS_PYARROW = False
    HAS_ORJSON = False

    HAS_PARQUET_SUPPORT = False
    HAS_FAST_JSON = False

    @classmethod
    def detect_features(cls) -> None:
        """Detect available optional features.

        This method checks for all optional dependencies and
        sets the appropriate feature flags.
        """
        # Check individual packages
        cls.HAS_PYARROW = DependencyManager.has_dependency("pyarrow")
        cls.HAS_ORJSON = DependencyManager.has_dependency("orjson")

        # Check complete features
        cls.HAS_PARQUET_SUPPORT = DependencyManager.has_feature("parquet")
        cls.HAS_FAST_JSON = DependencyManager.has_feature("fast_json")


# Run detection at import time
Features.detect_features()
