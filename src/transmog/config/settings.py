"""Configuration settings for Transmog.

This module provides configuration management for the Transmog package,
including default settings, profiles, and environmental configuration.
"""

import json
import logging
import os
from typing import Any, Callable, Optional

from ..error.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

# Default options for Transmog
DEFAULT_OPTIONS = {
    "separator": "_",
    "cast_to_string": True,
    "include_empty": False,
    "skip_null": True,
    "visit_arrays": True,
    "deeply_nested_threshold": 4,
    "preserve_leaf_component": True,
    "path_parts_optimization": True,
    "max_nesting_depth": 100,
    "indent": 2,
    "batch_size": 1000,
    "processing_mode": "standard",
    "memory_threshold": 100 * 1024 * 1024,  # 100MB threshold for memory mode switching
    "memory_tracking_enabled": False,  # Memory usage tracking flag
    "cache_enabled": True,
    "cache_maxsize": 10000,
    "clear_cache_after_batch": False,
}

# Configuration file path environment variable
CONFIG_PATH_ENV_VAR = "TRANSMOG_CONFIG_PATH"


class TransmogSettings:
    """Settings manager for Transmog.

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
    DEFAULT_VISIT_ARRAYS = True
    DEFAULT_ALLOW_MALFORMED_DATA = False
    DEFAULT_DEFAULT_ID_FIELD = None
    DEFAULT_ID_GENERATION_STRATEGY = None
    DEFAULT_DEEPLY_NESTED_THRESHOLD = 4

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

    # Cache settings
    DEFAULT_CACHE_ENABLED = True
    DEFAULT_CACHE_MAXSIZE = 10000
    DEFAULT_CLEAR_CACHE_AFTER_BATCH = False

    # Configuration profiles
    PROFILES: dict[str, dict[str, Any]] = {
        "default": {
            # Uses all the DEFAULT_* values
        },
        "memory_efficient": {
            "optimize_for_memory": True,
            "batch_size": 500,
            "chunk_size": 200,
            "lru_cache_size": 256,
            "cache_enabled": True,
            "cache_maxsize": 1000,
            "clear_cache_after_batch": True,
        },
        "performance": {
            "optimize_for_memory": False,
            "batch_size": 2000,
            "chunk_size": 1000,
            "path_parts_optimization": True,
            "lru_cache_size": 2048,
            "max_workers": 8,
            "cache_enabled": True,
            "cache_maxsize": 50000,
            "clear_cache_after_batch": False,
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
        "simple_naming": {
            "deeply_nested_threshold": 6,
        },
        "compact_naming": {
            "deeply_nested_threshold": 3,
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
            # Define deterministic ID field for the root level
            "default_id_field": "id",  # Uses 'id' field for deterministic IDs
        },
    }

    # Environment variable prefix
    ENV_PREFIX = "TRANSMOG_"

    def __init__(
        self,
        profile: str = "default",
        config_file: Optional[str] = None,
    ):
        """Initialize settings.

        Args:
            profile: Configuration profile to use
            config_file: Optional path to configuration file
        """
        # Initialize settings dictionary
        self._settings: dict[str, Any] = {}

        # Load default settings
        self._load_defaults()

        # Apply profile if specified
        if profile:
            if profile in self.PROFILES:
                # Update settings with profile values
                profile_values = self.PROFILES.get(profile, {})
                if profile_values:
                    self._settings.update(profile_values)
            else:
                logging.warning(f"Profile '{profile}' not found, using defaults")

        # Load from config file if specified
        if config_file:
            self._load_from_file(config_file)

        # Apply environment variables
        self._load_from_env()

    def _load_defaults(self) -> None:
        """Load default settings from class attributes."""
        for attr in dir(self.__class__):
            if attr.startswith("DEFAULT_"):
                key = attr[8:].lower()  # Strip DEFAULT_ and lowercase
                self._settings[key] = getattr(self.__class__, attr)

    def _apply_profile(self, profile: str) -> None:
        """Apply a settings profile.

        Args:
            profile: Name of the profile to apply
        """
        if profile in self.__class__.PROFILES:
            profile_values = self.__class__.PROFILES[profile]
            if profile_values:
                self._settings.update(profile_values)
        else:
            raise ConfigurationError(f"Unknown profile: {profile}")

    def _load_from_file(self, config_file: str) -> None:
        """Load settings from a configuration file.

        Args:
            config_file: Path to the configuration file
        """
        if not os.path.exists(config_file):
            raise ConfigurationError(f"Configuration file not found: {config_file}")

        # Get file extension
        file_ext = os.path.splitext(config_file)[1].lower()

        try:
            # Load based on file extension
            if file_ext == ".json":
                with open(config_file) as f:
                    config_data = json.load(f)
            elif file_ext in (".yaml", ".yml"):
                try:
                    import yaml

                    with open(config_file) as f:
                        config_data = yaml.safe_load(f)
                except ImportError as e:
                    raise ConfigurationError(
                        "PyYAML is required for YAML configuration support. "
                        "Install with: pip install PyYAML"
                    ) from e
            elif file_ext in (".toml", ".tml"):
                try:
                    import toml

                    with open(config_file) as f:
                        config_data = toml.load(f)
                except ImportError as e:
                    raise ConfigurationError(
                        "toml is required for TOML configuration support. "
                        "Install with: pip install toml"
                    ) from e
            else:
                raise ConfigurationError(
                    f"Unsupported configuration file format: {file_ext}"
                )

            # Update settings with loaded data
            self._settings.update(config_data)

        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON configuration file: {e}") from e
        except Exception as e:
            if file_ext == ".json":
                raise ConfigurationError(
                    f"Failed to load JSON configuration: {e}"
                ) from e
            elif file_ext in (".yaml", ".yml"):
                raise ConfigurationError(
                    f"Failed to load YAML configuration: {e}"
                ) from e
            elif file_ext in (".toml", ".tml"):
                raise ConfigurationError(
                    f"Failed to load TOML configuration: {e}"
                ) from e
            else:
                raise ConfigurationError(f"Failed to load configuration: {e}") from e

    def _load_from_env(self) -> None:
        """Load settings from environment variables."""
        prefix = self.__class__.ENV_PREFIX
        for env_var, value in os.environ.items():
            if env_var.startswith(prefix):
                # Strip prefix and convert to lowercase
                key = env_var[len(prefix) :].lower()
                # Convert value to appropriate type
                converted_value = self._convert_value(value)
                self._settings[key] = converted_value

    def _convert_value(self, value: str, default_value: Any = None) -> Any:
        """Convert a string value to the appropriate type based on default value.

        Args:
            value: String value to convert
            default_value: Default value to determine type

        Returns:
            Converted value
        """
        # Parse as JSON when no default value is provided
        if default_value is None:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value

        # Convert based on default value type
        try:
            if isinstance(default_value, bool):
                # Convert string to boolean based on common true/false representations
                lower_val = value.lower()
                if lower_val in ("true", "t", "yes", "y", "1"):
                    return True
                elif lower_val in ("false", "f", "no", "n", "0"):
                    return False
                else:
                    # Fall back to standard boolean conversion
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
                # Apply same type as default value
                return type(default_value)(value)
        except (ValueError, json.JSONDecodeError):
            # Log warning on conversion failure and return original string
            type_name = type(default_value).__name__
            logger.warning(f"Could not convert value '{value}' to type {type_name}")
            return value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a setting value.

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
        """Get a setting value with type checking and validation.

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

        # Retrieve setting or use default if not found
        value = self._settings.get(normalized_key, default)

        # Fall back to class default if no value or default provided
        if value is None and default is None:
            default_attr = f"DEFAULT_{key.upper()}"
            if hasattr(self.__class__, default_attr):
                value = getattr(self.__class__, default_attr)

        # Infer expected type from default value if not explicitly specified
        if expected_type is None and default is not None:
            expected_type = type(default)

        # Validate and convert value to expected type if needed
        if expected_type is not None and value is not None:
            if not isinstance(value, expected_type):
                try:
                    if isinstance(expected_type, tuple):
                        # Attempt conversion with each type in the tuple
                        for t in expected_type:
                            try:
                                value = t(value)
                                break
                            except (ValueError, TypeError):
                                continue
                        else:
                            # No conversion succeeded
                            type_name = type(value).__name__
                            msg_part1 = f"Setting '{key}' has invalid type: expected "
                            error_msg = f"{msg_part1}{expected_type}, got {type_name}"
                            raise ConfigurationError(error_msg)
                    else:
                        try:
                            value = expected_type(value)
                        except (ValueError, TypeError) as type_err:
                            expected_name = expected_type.__name__
                            type_name = type(value).__name__
                            msg_part1 = f"Setting '{key}' has invalid type: expected "
                            error_msg = f"{msg_part1}{expected_name}, got {type_name}"
                            raise ConfigurationError(error_msg) from type_err
                except (ValueError, TypeError):
                    expected_name = expected_type.__name__
                    type_name = type(value).__name__
                    msg_part1 = f"Setting '{key}' has invalid type: expected "
                    error_msg = f"{msg_part1}{expected_name}, got {type_name}"
                    raise ConfigurationError(error_msg) from None

        return value

    def __getattr__(self, name: str) -> Any:
        """Access settings as attributes.

        Args:
            name: Setting name

        Returns:
            Setting value

        Raises:
            AttributeError: If setting not found
        """
        # Support both camelCase and snake_case access patterns
        snake_case = name[0].lower() + "".join(
            ["_" + c.lower() if c.isupper() else c for c in name[1:]]
        )

        # Try to get the setting
        value = self._settings.get(name.lower()) or self._settings.get(snake_case)

        if value is not None:
            return value

        # Check for default attribute
        default_attr = f"DEFAULT_{name.upper()}"
        if hasattr(self.__class__, default_attr):
            return getattr(self.__class__, default_attr)

        # Setting not found in configuration
        raise AttributeError(f"Setting '{name}' not found")

    def update(self, **kwargs: Any) -> None:
        """Update settings with new values.

        Args:
            **kwargs: Settings to update
        """
        # Convert all keys to lowercase
        for key, value in kwargs.items():
            self._settings[key.lower()] = value

    def as_dict(self) -> dict[str, Any]:
        """Get all settings as a dictionary.

        Returns:
            Dictionary of all settings
        """
        return dict(self._settings)

    def configure_logging(self) -> None:
        """Configure logging based on current settings."""
        log_level = self.get("log_level", logging.WARNING)
        log_format = self.get("log_format")
        log_file = self.get("log_file")

        from ..error.handling import setup_logging

        setup_logging(level=log_level, log_format=log_format, log_file=log_file)


# Extension point registration system
class ExtensionRegistry:
    """Registry for Transmog extensions and custom handlers."""

    def __init__(self) -> None:
        """Initialize the extension registry."""
        self._type_handlers: dict[str, Callable[..., Any]] = {}
        self._naming_strategies: dict[str, Callable[..., Any]] = {}
        self._validators: dict[str, Callable[..., Any]] = {}

    def register_type_handler(
        self, type_name: str, handler_func: Callable[..., Any]
    ) -> None:
        """Register a custom type handler.

        Args:
            type_name: Type name to handle
            handler_func: Handler function
        """
        self._type_handlers[type_name] = handler_func

    def register_naming_strategy(
        self, strategy_name: str, strategy_func: Callable[..., Any]
    ) -> None:
        """Register a custom naming strategy.

        Args:
            strategy_name: Strategy name
            strategy_func: Strategy function
        """
        self._naming_strategies[strategy_name] = strategy_func

    def register_validator(
        self, field_pattern: str, validator_func: Callable[..., Any]
    ) -> None:
        """Register a custom field validator.

        Args:
            field_pattern: Field pattern to validate
            validator_func: Validator function
        """
        self._validators[field_pattern] = validator_func

    def get_type_handler(self, type_name: str) -> Optional[Callable[..., Any]]:
        """Get a registered type handler by name."""
        return self._type_handlers.get(type_name)

    def get_naming_strategy(self, strategy_name: str) -> Optional[Callable[..., Any]]:
        """Get a registered naming strategy by name."""
        return self._naming_strategies.get(strategy_name)

    def get_validator(self, field_pattern: str) -> Optional[Callable[..., Any]]:
        """Get a registered validator by field pattern."""
        return self._validators.get(field_pattern)

    def get_all_type_handlers(self) -> dict[str, Callable[..., Any]]:
        """Get all registered type handlers."""
        return dict(self._type_handlers)

    def get_all_naming_strategies(self) -> dict[str, Callable[..., Any]]:
        """Get all registered naming strategies."""
        return dict(self._naming_strategies)

    def get_all_validators(self) -> dict[str, Callable[..., Any]]:
        """Get all registered validators."""
        return dict(self._validators)


# Global settings instance
_default_settings: Optional[TransmogSettings] = None


def load_profile(profile_name: str) -> TransmogSettings:
    """Load a predefined configuration profile.

    Args:
        profile_name: Name of the profile to load

    Returns:
        TransmogSettings instance with the profile applied

    Raises:
        ConfigurationError: If profile not found
    """
    if profile_name not in TransmogSettings.PROFILES:
        raise ConfigurationError(f"Unknown profile: {profile_name}")

    # Initialize settings with specified profile
    settings = TransmogSettings(profile=profile_name)
    return settings


def load_config(config_file: str) -> TransmogSettings:
    """Load configuration from a file.

    Supports JSON, YAML, and TOML file formats based on file extension.
    - .json: Standard JSON format
    - .yaml/.yml: YAML format (requires PyYAML)
    - .toml/.tml: TOML format (requires toml)

    Args:
        config_file: Path to configuration file

    Returns:
        TransmogSettings instance with the configuration applied

    Raises:
        ConfigurationError: If file not found, has invalid format,
                           or required dependency is missing
    """
    # Create settings with default profile
    settings = TransmogSettings(profile="default")

    # Load settings from file
    settings._load_from_file(config_file)

    return settings


def configure(**kwargs: Any) -> TransmogSettings:
    """Configure Transmog with custom settings.

    This is the primary entry point for configuring the library.

    Args:
        **kwargs: Configuration options to set

    Returns:
        TransmogSettings instance with the configuration applied
    """
    global _default_settings

    # Get profile and config file if specified
    profile = kwargs.pop("profile", "default")
    config_file = kwargs.pop("config_file", None)

    # Create settings with profile and config file
    settings = TransmogSettings(profile=profile, config_file=config_file)

    # Apply additional settings
    settings.update(**kwargs)

    # Set as default settings if not already set
    if _default_settings is None:
        _default_settings = settings

    return settings
