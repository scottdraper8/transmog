"""Optional dependency management for Transmog.

This module provides centralized management of optional dependencies,
allowing the code to adapt based on what's available while providing
informative messages when functionality is not available.
"""

import importlib
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DependencyManager:
    """Manager for optional dependencies.

    This class tracks the availability of optional dependencies and provides
    utility functions for handling missing dependencies.
    """

    # Optional dependencies availability tracking
    _optional_deps: dict[str, bool] = {}

    # Dependencies grouped by feature
    _feature_deps: dict[str, set[str]] = {
        "parquet": {"pyarrow"},
        "fast_json": {"orjson"},
        "typing": {"typing-extensions"},  # Enhanced typing support
    }

    @classmethod
    def has_dependency(cls, name: str) -> bool:
        """Check if an optional dependency is available.

        Args:
            name: Name of the dependency package

        Returns:
            Whether the dependency is available
        """
        # Check cache first
        if name in cls._optional_deps:
            return cls._optional_deps[name]

        # Try to import and cache the result
        available = cls._check_dependency(name)
        cls._optional_deps[name] = available
        return available

    @classmethod
    def register_dependency(cls, name: str, available: bool) -> None:
        """Manually register a dependency availability.

        This method is primarily used for testing or for cases
        where dependency availability needs to be manually controlled
        without actually importing the package.

        Args:
            name: Name of the dependency package
            available: Whether the dependency should be marked as available
        """
        cls._optional_deps[name] = available

    @staticmethod
    def _check_dependency(name: str) -> bool:
        """Check if a dependency can be imported.

        Args:
            name: Name of the dependency package

        Returns:
            Whether the import succeeds
        """
        try:
            importlib.import_module(name)
            return True
        except ImportError:
            return False

    @classmethod
    def has_feature(cls, feature_name: str) -> bool:
        """Check if all dependencies for a feature are available.

        Args:
            feature_name: Name of the feature

        Returns:
            Whether all required dependencies are available
        """
        if feature_name not in cls._feature_deps:
            return False

        return all(cls.has_dependency(dep) for dep in cls._feature_deps[feature_name])

    @classmethod
    def missing_dependencies(cls, feature_name: str) -> set[str]:
        """Get the set of missing dependencies for a feature.

        Args:
            feature_name: Name of the feature

        Returns:
            Set of missing dependency names
        """
        if feature_name not in cls._feature_deps:
            return set()

        return {
            dep
            for dep in cls._feature_deps[feature_name]
            if not cls.has_dependency(dep)
        }

    @classmethod
    def require_dependency(cls, name: str, message: Optional[str] = None) -> bool:
        """Check for a dependency and log an informative message if missing.

        Args:
            name: Name of the dependency package
            message: Optional custom message to log

        Returns:
            Whether the dependency is available
        """
        if cls.has_dependency(name):
            return True

        if message is None:
            message = f"The '{name}' package is required for this operation."
            message += f" Install with: pip install {name}"

        logger.warning(message)
        return False

    @classmethod
    def require_feature(cls, feature_name: str, message: Optional[str] = None) -> bool:
        """Check for all dependencies of a feature and log if any are missing.

        Args:
            feature_name: Name of the feature
            message: Optional custom message to log

        Returns:
            Whether all required dependencies are available
        """
        if cls.has_feature(feature_name):
            return True

        missing = cls.missing_dependencies(feature_name)
        if not missing:
            return False

        if message is None:
            deps_str = ", ".join(missing)
            message = (
                f"The following packages are required for {feature_name}: {deps_str}"
            )
            message += f"\nInstall with: pip install {' '.join(missing)}"

        logger.warning(message)
        return False
