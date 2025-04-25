"""
Configuration package for Transmog.

Provides settings management, profile configuration, and extension points.
"""

from transmog.config.settings import (
    settings,
    extensions,
    load_profile,
    load_config,
    configure,
    TransmogSettings,
    ExtensionRegistry,
)

__all__ = [
    "settings",
    "extensions",
    "load_profile",
    "load_config",
    "configure",
    "TransmogSettings",
    "ExtensionRegistry",
]
