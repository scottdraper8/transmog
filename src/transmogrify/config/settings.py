"""
Configuration settings for Transmogrify.

This module provides default settings, configuration profiles, and utilities
for customizing Transmogrify's behavior.
"""

import os
import json
import logging
from typing import Any, Dict, List, Optional, Union, Set, Callable

from ..exceptions import ConfigurationError

# Create logger for settings module
logger = logging.getLogger("transmogrify.settings")


class TransmogrifySettings:
    """
    Settings manager for Transmogrify.

    Provides access to configuration settings with support for defaults,
    environment variables, and configuration profiles.
    """

    # Default processor settings
    DEFAULT_SEPARATOR = "_"
    DEFAULT_CAST_TO_STRING = True
    DEFAULT_INCLUDE_EMPTY = False
    DEFAULT_SKIP_NULL = True
    DEFAULT_ID_FIELD = "__extract_id"
    DEFAULT_PARENT_FIELD = "__parent_extract_id"
    DEFAULT_TIME_FIELD = "__extract_datetime"
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_OPTIMIZE_FOR_MEMORY = False
    DEFAULT_MAX_NESTING_DEPTH = None
    DEFAULT_PATH_PARTS_OPTIMIZATION = True
    DEFAULT_VISIT_ARRAYS = False
    DEFAULT_ALLOW_MALFORMED_DATA = False
    DEFAULT_DETERMINISTIC_ID_FIELDS = {}
    DEFAULT_ID_GENERATION_STRATEGY = None

    # Abbreviation settings
    DEFAULT_ABBREVIATE_TABLE_NAMES = True
    DEFAULT_ABBREVIATE_FIELD_NAMES = True
    DEFAULT_MAX_TABLE_COMPONENT_LENGTH = 4
    DEFAULT_MAX_FIELD_COMPONENT_LENGTH = 4
    DEFAULT_PRESERVE_LEAF_COMPONENT = True
    DEFAULT_CUSTOM_ABBREVIATIONS = {}

    # CSV settings
    DEFAULT_CSV_DELIMITER = ","
    DEFAULT_CSV_QUOTE_CHAR = '"'
    DEFAULT_CSV_NULL_VALUES = ["", "NULL", "null", "NA", "na", "N/A", "n/a"]
    DEFAULT_CSV_INFER_TYPES = True
    DEFAULT_CSV_SANITIZE_COLUMN_NAMES = True

    # Performance settings
    DEFAULT_LRU_CACHE_SIZE = 1024
    DEFAULT_MAX_WORKERS = 4
    DEFAULT_CHUNK_SIZE = 500
    DEFAULT_FILE_BUFFER_SIZE = 8192  # 8KB

    # Logging settings
    DEFAULT_LOG_LEVEL = logging.WARNING
    DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Configuration profiles
    PROFILES = {
        "default": {
            # Uses all the DEFAULT_* values
        },
        "memory_efficient": {
            "optimize_for_memory": True,
            "batch_size": 500,
            "chunk_size": 200,
            "lru_cache_size": 256,
        },
        "performance": {
            "optimize_for_memory": False,
            "batch_size": 2000,
            "chunk_size": 1000,
            "path_parts_optimization": True,
            "lru_cache_size": 2048,
            "max_workers": 8,
        },
        "strict": {
            "allow_malformed_data": False,
            "log_level": logging.ERROR,
        },
        "lenient": {
            "allow_malformed_data": True,
            "log_level": logging.INFO,
            "include_empty": True,
            "skip_null": False,
        },
        "no_abbreviation": {
            "abbreviate_table_names": False,
            "abbreviate_field_names": False,
        },
        "full_abbreviation": {
            "abbreviate_table_names": True,
            "abbreviate_field_names": True,
            "max_table_component_length": 4,
            "max_field_component_length": 4,
            "preserve_leaf_component": False,
        },
        "short_abbreviation": {
            "abbreviate_table_names": True,
            "abbreviate_field_names": True,
            "max_table_component_length": 4,
            "max_field_component_length": 4,
            "preserve_leaf_component": True,
        },
        "csv_strict": {
            "csv_delimiter": ",",
            "csv_quote_char": '"',
            "csv_null_values": ["", "NULL", "null"],
            "csv_infer_types": False,
            "cast_to_string": True,
        },
        "csv_flexible": {
            "csv_delimiter": None,  # Auto-detect
            "csv_quote_char": '"',
            "csv_null_values": [
                "",
                "NULL",
                "null",
                "NA",
                "na",
                "N/A",
                "n/a",
                "#N/A",
                "#N/A N/A",
                "#NA",
                "-1.#IND",
                "-1.#QNAN",
                "-NaN",
                "-nan",
                "1.#IND",
                "1.#QNAN",
                "N/A",
                "NA",
                "NULL",
                "NaN",
                "n/a",
                "nan",
                "null",
            ],
            "csv_infer_types": True,
            "cast_to_string": False,
        },
        "deterministic_ids": {
            # Define deterministic ID fields for the root level
            "deterministic_id_fields": {
                "": "id",  # Root level uses 'id' field
                "*": "id",  # Default for all paths is 'id'
            },
        },
    }

    # Environment variable prefix
    ENV_PREFIX = "TRANSMOGRIFY_"

    def __init__(
        self,
        profile: str = "default",
        config_file: Optional[str] = None,
    ):
        """
        Initialize settings with optional profile and config file.

        Args:
            profile: Name of configuration profile to use
            config_file: Optional path to configuration file
        """
        # Start with an empty settings dict
        self._settings = {}

        # Load all default values first
        self._load_defaults()

        # Apply profile if specified
        if profile:
            if profile in self.PROFILES:
                # Update settings with profile values
                self._settings.update(self.PROFILES.get(profile, {}))
            else:
                logging.warning(f"Profile '{profile}' not found, using defaults")

        # Load from config file if specified
        if config_file:
            self._load_from_file(config_file)

        # Apply environment variables
        self._load_from_env()

    def _load_defaults(self):
        """Load default settings."""
        for key, value in vars(self.__class__).items():
            if key.startswith("DEFAULT_"):
                setting_name = key[8:].lower()
                self._settings[setting_name] = value

    def _apply_profile(self, profile: str):
        """Apply settings from the specified profile."""
        profile_settings = self.PROFILES.get(profile, {})
        self._settings.update(profile_settings)

    def _load_from_file(self, config_file: str):
        """Load settings from configuration file."""
        if not os.path.exists(config_file):
            logging.warning(f"Configuration file not found: {config_file}")
            return

        try:
            with open(config_file, "r") as f:
                if config_file.endswith((".json", ".JSON")):
                    config = json.load(f)
                elif config_file.endswith((".yaml", ".yml", ".YAML", ".YML")):
                    try:
                        import yaml

                        config = yaml.safe_load(f)
                    except ImportError:
                        logging.warning(
                            "PyYAML not installed. Cannot load YAML config."
                        )
                        return
                elif config_file.endswith((".toml", ".TOML")):
                    try:
                        import toml

                        config = toml.load(f)
                    except ImportError:
                        logging.warning("toml not installed. Cannot load TOML config.")
                        return
                else:
                    logging.warning(f"Unsupported config file format: {config_file}")
                    return

            # Update settings with file config
            if isinstance(config, dict):
                self._settings.update(config)
        except Exception as e:
            logging.error(f"Error loading configuration file: {e}")

    def _load_from_env(self):
        """Load settings from environment variables."""
        for env_var, value in os.environ.items():
            if env_var.startswith(self.ENV_PREFIX):
                setting_name = env_var[len(self.ENV_PREFIX) :].lower()

                # Check if we have a default for this setting to determine type
                default_value = None
                default_attr = f"DEFAULT_{setting_name.upper()}"
                has_default_type = hasattr(self.__class__, default_attr)

                if has_default_type:
                    default_value = getattr(self.__class__, default_attr)

                # Try to convert value to the same type as default
                converted_value = self._convert_value(value, default_value)
                self._settings[setting_name] = converted_value

    def _convert_value(self, value: str, default_value: Any = None) -> Any:
        """
        Convert a string value to the appropriate type based on default value.

        Args:
            value: String value to convert
            default_value: Default value to determine type

        Returns:
            Converted value
        """
        # If default is None, try to parse as JSON or return as string
        if default_value is None:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value

        # Convert based on default value type
        try:
            if isinstance(default_value, bool):
                # Handle boolean conversion (treat certain strings as True/False)
                lower_val = value.lower()
                if lower_val in ("true", "t", "yes", "y", "1"):
                    return True
                elif lower_val in ("false", "f", "no", "n", "0"):
                    return False
                else:
                    # Default to boolean conversion of the string
                    return bool(value)
            elif isinstance(default_value, int):
                return int(value)
            elif isinstance(default_value, float):
                return float(value)
            elif isinstance(default_value, list):
                return json.loads(value)
            elif isinstance(default_value, dict):
                return json.loads(value)
            else:
                # For other types, use the same type as the default
                return type(default_value)(value)
        except (ValueError, json.JSONDecodeError):
            # If conversion fails, log a warning and use the string value
            logger.warning(
                f"Could not convert value '{value}' to type {type(default_value).__name__}"
            )
            return value

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a setting value.

        Args:
            key: Setting name
            default: Default value if setting not found

        Returns:
            Setting value or default
        """
        normalized_key = key.lower()
        return self._settings.get(normalized_key, default)

    def get_option(
        self, key: str, default: Any = None, expected_type: Any = None
    ) -> Any:
        """
        Get a setting value with type checking and validation.

        This is the preferred method for accessing settings as it provides
        type checking and consistent behavior.

        Args:
            key: Setting name
            default: Default value if setting not found
            expected_type: Expected type or tuple of types (optional)

        Returns:
            Setting value or default, with appropriate type conversion

        Raises:
            ConfigurationError: If value doesn't match expected type
        """
        normalized_key = key.lower()

        # Use the setting if it exists, otherwise use default
        value = self._settings.get(normalized_key, default)

        # If no value found and no default provided, check class defaults
        if value is None and default is None:
            default_attr = f"DEFAULT_{key.upper()}"
            if hasattr(self.__class__, default_attr):
                value = getattr(self.__class__, default_attr)

        # If expected_type is not specified but default is provided,
        # infer expected_type from default
        if expected_type is None and default is not None:
            expected_type = type(default)

        # If we have an expected type, validate the value
        if expected_type is not None and value is not None:
            if not isinstance(value, expected_type):
                # Try to convert the value
                try:
                    if isinstance(expected_type, tuple):
                        # Try each type in the tuple
                        for t in expected_type:
                            try:
                                value = t(value)
                                break
                            except (ValueError, TypeError):
                                continue
                        else:
                            # If we get here, none of the conversions worked
                            raise ConfigurationError(
                                f"Setting '{key}' has invalid type: expected {expected_type}, got {type(value).__name__}"
                            )
                    else:
                        value = expected_type(value)
                except (ValueError, TypeError):
                    raise ConfigurationError(
                        f"Setting '{key}' has invalid type: expected {expected_type.__name__}, got {type(value).__name__}"
                    )

        return value

    def __getattr__(self, name: str) -> Any:
        """
        Access settings as attributes.

        Args:
            name: Setting name

        Returns:
            Setting value

        Raises:
            AttributeError: If setting not found
        """
        normalized_name = name.lower()

        if normalized_name in self._settings:
            return self._settings[normalized_name]

        # Check for DEFAULT_* class attribute as fallback
        default_name = f"DEFAULT_{name.upper()}"
        if hasattr(self.__class__, default_name):
            # Store the default in settings for future access
            default_value = getattr(self.__class__, default_name)
            self._settings[normalized_name] = default_value
            return default_value

        raise AttributeError(f"Setting not found: {name}")

    def update(self, **kwargs):
        """
        Update settings with provided values.

        Args:
            **kwargs: Settings to update
        """
        self._settings.update(kwargs)

    def as_dict(self) -> Dict[str, Any]:
        """
        Get all settings as a dictionary.

        Returns:
            Dictionary of all settings
        """
        return dict(self._settings)

    def configure_logging(self):
        """Configure logging based on current settings."""
        log_level = self.get("log_level", self.DEFAULT_LOG_LEVEL)
        log_format = self.get("log_format", self.DEFAULT_LOG_FORMAT)

        logging.basicConfig(level=log_level, format=log_format)


# Extension point registration system
class ExtensionRegistry:
    """Registry for Transmogrify extensions and custom handlers."""

    def __init__(self):
        """Initialize empty registry."""
        self._type_handlers = {}
        self._naming_strategies = {}
        self._validators = {}

    def register_type_handler(self, type_name: str, handler_func: callable):
        """Register a custom type handler."""
        self._type_handlers[type_name] = handler_func

    def register_naming_strategy(self, strategy_name: str, strategy_func: callable):
        """Register a custom naming strategy."""
        self._naming_strategies[strategy_name] = strategy_func

    def register_validator(self, field_pattern: str, validator_func: callable):
        """Register a field validator."""
        self._validators[field_pattern] = validator_func

    def get_type_handler(self, type_name: str) -> Optional[callable]:
        """Get registered type handler."""
        return self._type_handlers.get(type_name)

    def get_naming_strategy(self, strategy_name: str) -> Optional[callable]:
        """Get registered naming strategy."""
        return self._naming_strategies.get(strategy_name)

    def get_validator(self, field_pattern: str) -> Optional[callable]:
        """Get registered validator."""
        return self._validators.get(field_pattern)

    def get_all_type_handlers(self) -> Dict[str, callable]:
        """Get all registered type handlers."""
        return dict(self._type_handlers)

    def get_all_naming_strategies(self) -> Dict[str, callable]:
        """Get all registered naming strategies."""
        return dict(self._naming_strategies)

    def get_all_validators(self) -> Dict[str, callable]:
        """Get all registered validators."""
        return dict(self._validators)


# Singleton instances
settings = TransmogrifySettings()
extensions = ExtensionRegistry()


def load_profile(profile_name: str):
    """
    Load settings from a predefined profile.

    Args:
        profile_name: Name of the profile to load

    Returns:
        Updated settings object
    """
    global settings
    settings = TransmogrifySettings(profile=profile_name)
    print(f"Loaded profile {profile_name}: {settings.as_dict()}")
    return settings


def load_config(config_file: str):
    """
    Load settings from a configuration file.

    Args:
        config_file: Path to configuration file

    Returns:
        Updated settings object
    """
    global settings

    # Create a new settings object to load the file
    temp_settings = TransmogrifySettings()
    temp_settings._load_from_file(config_file)

    # Update the global settings with the file settings
    settings = temp_settings
    print(f"Loaded settings from {config_file}: {settings.as_dict()}")
    return settings


def configure(**kwargs):
    """
    Update settings with provided values.

    Args:
        **kwargs: Settings to update

    Returns:
        Updated settings object
    """
    global settings
    for key, value in kwargs.items():
        settings._settings[key.lower()] = value
    print(f"Updated settings: {settings.as_dict()}")
    return settings
