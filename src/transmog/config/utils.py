"""Utility functions for Transmog configuration.

This module provides helper functions for working with Transmog configuration objects.
"""

from typing import Any

from transmog.config import TransmogConfig


def get_common_config_params(config: TransmogConfig) -> dict[str, Any]:
    """Get common parameter dictionary from configuration.

    This utility creates a dictionary of commonly used
    configuration parameters to pass to processing functions.

    Args:
        config: Configuration object

    Returns:
        Dict of parameters
    """
    return {
        # Processing parameters
        "cast_to_string": config.processing.cast_to_string,
        "include_empty": config.processing.include_empty,
        "skip_null": config.processing.skip_null,
        "visit_arrays": config.processing.visit_arrays,
        "max_depth": config.processing.max_nesting_depth,
        # Naming parameters
        "separator": config.naming.separator,
        "abbreviate_field_names": config.naming.abbreviate_field_names,
        "abbreviate_table_names": config.naming.abbreviate_table_names,
        "max_field_component_length": config.naming.max_field_component_length,
        "max_table_component_length": config.naming.max_table_component_length,
        "preserve_root_component": config.naming.preserve_root_component,
        "preserve_leaf_component": config.naming.preserve_leaf_component,
        "custom_abbreviations": config.naming.custom_abbreviations,
    }
