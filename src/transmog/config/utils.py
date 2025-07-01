"""Utility functions for Transmog configuration.

This module provides helper functions for working with Transmog configuration objects.
"""

from datetime import datetime, timezone
from typing import Any, Optional

from transmog.config import TransmogConfig


class ConfigParameterBuilder:
    """Unified configuration parameter builder for processing functions.

    This class consolidates the repetitive config parameter extraction logic
    that was scattered across multiple modules, providing a single source of
    truth for converting TransmogConfig objects to parameter dictionaries.
    """

    def __init__(self, config: TransmogConfig):
        """Initialize with a configuration object.

        Args:
            config: Configuration object to extract parameters from
        """
        self.config = config

    def build_common_params(
        self,
        extract_time: Optional[Any] = None,
        exclude: Optional[set[str]] = None,
        **overrides: Any,
    ) -> dict[str, Any]:
        """Build common parameter dictionary from configuration.

        Args:
            extract_time: Optional extraction timestamp
            exclude: Set of parameter names to exclude
            **overrides: Parameter overrides

        Returns:
            Dictionary of parameters for processing functions
        """
        # Set extraction time
        if extract_time is None:
            extract_time = datetime.now(timezone.utc)

        # Build base parameters
        params = {
            # Naming parameters
            "separator": self._get_naming_param("separator", "_"),
            "nested_threshold": self._get_naming_param("nested_threshold", 4),
            # Processing parameters
            "cast_to_string": self._get_processing_param("cast_to_string", True),
            "include_empty": self._get_processing_param("include_empty", False),
            "skip_null": self._get_processing_param("skip_null", True),
            "path_parts_optimization": self._get_processing_param(
                "path_parts_optimization", True
            ),
            "visit_arrays": self._get_processing_param("visit_arrays", True),
            "keep_arrays": self._get_processing_param("keep_arrays", False),
            "max_depth": self._get_processing_param("max_depth", 100),
            # Metadata parameters
            "id_field": self._get_metadata_param("id_field", "__transmog_id"),
            "parent_field": self._get_metadata_param(
                "parent_field", "__parent_transmog_id"
            ),
            "time_field": self._get_metadata_param("time_field", "__transmog_datetime"),
            "default_id_field": self._get_metadata_param("default_id_field", None),
            "id_generation_strategy": self._get_metadata_param(
                "id_generation_strategy", None
            ),
            "force_transmog_id": self._get_metadata_param("force_transmog_id", False),
            "id_field_patterns": self._get_metadata_param("id_field_patterns", None),
            "id_field_mapping": self._get_metadata_param("id_field_mapping", None),
            # Error handling parameters
            "recovery_strategy": self._get_error_handling_param(
                "recovery_strategy", "strict"
            ),
            # Timestamp
            "transmog_time": extract_time,
        }

        # Apply exclusions
        if exclude:
            for key in exclude:
                params.pop(key, None)

        # Apply overrides
        params.update(overrides)

        return params

    def build_processing_params(
        self, extract_time: Optional[Any] = None, **overrides: Any
    ) -> dict[str, Any]:
        """Build parameters specifically for processing functions.

        Args:
            extract_time: Optional extraction timestamp
            **overrides: Parameter overrides

        Returns:
            Dictionary of parameters optimized for processing functions
        """
        return self.build_common_params(extract_time=extract_time, **overrides)

    def build_streaming_params(
        self,
        extract_time: Optional[Any] = None,
        use_deterministic_ids: Optional[bool] = None,
        force_transmog_id: Optional[bool] = None,
        **overrides: Any,
    ) -> dict[str, Any]:
        """Build parameters specifically for streaming functions.

        Args:
            extract_time: Optional extraction timestamp
            use_deterministic_ids: Whether to use deterministic IDs
            force_transmog_id: Whether to force transmog ID generation
            **overrides: Parameter overrides

        Returns:
            Dictionary of parameters optimized for streaming functions
        """
        # Exclude parameters not accepted by streaming functions
        exclude = {"path_parts_optimization"}

        params = self.build_common_params(
            extract_time=extract_time, exclude=exclude, **overrides
        )

        # Add streaming-specific parameters
        if use_deterministic_ids is not None:
            params["use_deterministic_ids"] = use_deterministic_ids

        if force_transmog_id is not None:
            params["force_transmog_id"] = force_transmog_id

        return params

    def build_hierarchy_params(
        self,
        extract_time: Optional[Any] = None,
        streaming: bool = False,
        **overrides: Any,
    ) -> dict[str, Any]:
        """Build parameters specifically for hierarchy processing functions.

        Args:
            extract_time: Optional extraction timestamp
            streaming: Whether this is for streaming functions
            **overrides: Parameter overrides

        Returns:
            Dictionary of parameters for hierarchy functions
        """
        # Hierarchy functions don't accept path_parts_optimization
        # Streaming hierarchy functions also don't accept recovery_strategy
        exclude = {"path_parts_optimization"}
        if streaming:
            exclude.add("recovery_strategy")

        params = self.build_common_params(
            extract_time=extract_time, exclude=exclude, **overrides
        )

        # For streaming hierarchy functions, add use_deterministic_ids if specified
        if streaming and "use_deterministic_ids" in overrides:
            params["use_deterministic_ids"] = overrides["use_deterministic_ids"]

        return params

    def get_batch_size(self, override: Optional[int] = None) -> int:
        """Get batch size for processing.

        Args:
            override: Optional batch size override

        Returns:
            Batch size to use
        """
        if override is not None:
            return int(override)

        return self._get_processing_param("batch_size", 1000)

    def _get_naming_param(self, param: str, default: Any) -> Any:
        """Get parameter from naming config with fallback."""
        naming_config = getattr(self.config, "naming", None)
        if naming_config and hasattr(naming_config, param):
            return getattr(naming_config, param)
        return default

    def _get_processing_param(self, param: str, default: int) -> int:
        """Get parameter from processing config with validation and error reporting.

        Args:
            param: Parameter name to extract
            default: Default value if parameter not found or None

        Returns:
            Validated integer parameter

        Raises:
            ConfigurationError: If parameter exists but cannot be converted to int
        """
        from ..error import ConfigurationError

        processing_config = getattr(self.config, "processing", None)
        if not processing_config or not hasattr(processing_config, param):
            return default

        value = getattr(processing_config, param)
        if value is None:
            return default

        if isinstance(value, int):
            return value

        # Attempt conversion with clear error reporting
        try:
            converted_value = int(value)
            return converted_value
        except (ValueError, TypeError) as e:
            raise ConfigurationError(
                f"Invalid configuration parameter '{param}': "
                f"cannot convert {value!r} (type: {type(value).__name__}) to int: {e}"
            ) from e

    def _get_metadata_param(self, param: str, default: Any) -> Any:
        """Get parameter from metadata config with fallback."""
        metadata_config = getattr(self.config, "metadata", None)
        if metadata_config and hasattr(metadata_config, param):
            return getattr(metadata_config, param)
        return default

    def _get_error_handling_param(self, param: str, default: Any) -> Any:
        """Get parameter from error handling config with fallback."""
        error_config = getattr(self.config, "error_handling", None)
        if error_config and hasattr(error_config, param):
            return getattr(error_config, param)
        return default


# Convenience functions for backward compatibility
def get_common_config_params(config: TransmogConfig) -> dict[str, Any]:
    """Get common parameter dictionary from configuration.

    This utility creates a dictionary of commonly used
    configuration parameters to pass to processing functions.

    Args:
        config: Configuration object

    Returns:
        Dict of parameters
    """
    builder = ConfigParameterBuilder(config)
    return builder.build_common_params()


def build_config_params(
    config: TransmogConfig, extract_time: Optional[Any] = None, **overrides: Any
) -> dict[str, Any]:
    """Build configuration parameters with optional overrides.

    Args:
        config: Configuration object
        extract_time: Optional extraction timestamp
        **overrides: Parameter overrides

    Returns:
        Dictionary of parameters
    """
    builder = ConfigParameterBuilder(config)
    return builder.build_common_params(extract_time=extract_time, **overrides)


def build_streaming_params(
    config: TransmogConfig,
    extract_time: Optional[Any] = None,
    use_deterministic_ids: Optional[bool] = None,
    force_transmog_id: Optional[bool] = None,
    **overrides: Any,
) -> dict[str, Any]:
    """Build streaming-specific parameters.

    Args:
        config: Configuration object
        extract_time: Optional extraction timestamp
        use_deterministic_ids: Whether to use deterministic IDs
        force_transmog_id: Whether to force transmog ID generation
        **overrides: Parameter overrides

    Returns:
        Dictionary of parameters for streaming functions
    """
    builder = ConfigParameterBuilder(config)
    return builder.build_streaming_params(
        extract_time=extract_time,
        use_deterministic_ids=use_deterministic_ids,
        force_transmog_id=force_transmog_id,
        **overrides,
    )


def build_hierarchy_params(
    config: TransmogConfig,
    extract_time: Optional[Any] = None,
    streaming: bool = False,
    **overrides: Any,
) -> dict[str, Any]:
    """Build parameters for hierarchy processing functions.

    Args:
        config: Configuration object
        extract_time: Optional extraction timestamp
        streaming: Whether this is for streaming functions
        **overrides: Parameter overrides

    Returns:
        Dictionary of parameters for hierarchy functions
    """
    builder = ConfigParameterBuilder(config)
    return builder.build_hierarchy_params(
        extract_time=extract_time, streaming=streaming, **overrides
    )
