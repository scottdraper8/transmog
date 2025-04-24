"""
Configuration package for Transmogrify.

Provides settings management, profile configuration, and extension points.
"""

from src.transmogrify.config.settings import (
    settings,
    extensions,
    load_profile,
    load_config,
    configure,
    TransmogrifySettings,
    ExtensionRegistry,
)

__all__ = [
    "settings",
    "extensions",
    "load_profile",
    "load_config",
    "configure",
    "TransmogrifySettings",
    "ExtensionRegistry",
]
