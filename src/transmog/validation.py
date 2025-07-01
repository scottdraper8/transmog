"""Unified parameter validation for Transmog.

This module provides centralized validation for both API and configuration parameters,
ensuring consistent error messages and validation logic across the entire codebase.
"""

import re
from typing import Any, Literal, Optional, Union

from transmog.error import ConfigurationError, ValidationError

# Type aliases for validation
ArrayHandling = Literal["separate", "inline", "skip"]
ErrorHandling = Literal["raise", "skip", "warn"]
IdSource = Union[str, dict[str, str], None]


class ParameterValidator:
    """Unified validation for API and configuration parameters."""

    @staticmethod
    def validate_separator(value: str, context: str = "parameter") -> None:
        """Validate separator character.

        Args:
            value: Separator character to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError

        if not value:
            raise error_class("Separator cannot be empty")

        if len(value) > 2:
            raise error_class(
                f"Separator '{value}' is too long. Must be 1-2 characters."
            )

        # Check for invalid characters in separator
        invalid_chars = r"[\\\/\"\'\[\]\{\}\(\)\s]"
        if re.search(invalid_chars, value):
            raise error_class(
                f"Separator '{value}' contains invalid characters. "
                f"Cannot use spaces or special characters: \\ / \" ' [ ] {{ }} ( )"
            )

    @staticmethod
    def validate_nested_threshold(value: int, context: str = "parameter") -> None:
        """Validate nested threshold value.

        Args:
            value: Nested threshold to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError

        if not isinstance(value, int):
            raise error_class(
                f"Nested threshold must be an integer, got {type(value).__name__}"
            )

        if value < 2:
            raise error_class(f"Nested threshold must be at least 2, got {value}")

        if value > 20:
            import warnings

            warnings.warn(
                f"Very high nested threshold ({value}) may impact performance. "
                f"Consider using a smaller value (2-10) for better efficiency.",
                stacklevel=3,
            )

    @staticmethod
    def validate_batch_size(value: int, context: str = "parameter") -> None:
        """Validate batch size.

        Args:
            value: Batch size to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError

        if not isinstance(value, int):
            raise error_class(
                f"Batch size must be an integer, got {type(value).__name__}"
            )

        if value <= 0:
            raise error_class(f"Batch size must be positive, got {value}")

        if value > 100000:
            import warnings

            warnings.warn(
                f"Very large batch size ({value}) may cause memory issues. "
                f"Consider using a smaller value (1000-10000) for better performance.",
                stacklevel=3,
            )

    @staticmethod
    def validate_field_name(value: str, context: str = "parameter") -> None:
        """Validate field name.

        Args:
            value: Field name to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError

        if not value:
            raise error_class("Field name cannot be empty")

        if not isinstance(value, str):
            raise error_class(
                f"Field name must be a string, got {type(value).__name__}"
            )

        if value.startswith("__") and not (
            value.startswith("__extract_") or value.startswith("__custom_")
        ):
            raise error_class(
                f"Field name '{value}' cannot start with '__' unless it's a "
                f"recognized metadata field (e.g., __extract_* or __custom_*)"
            )

    @staticmethod
    def validate_arrays(value: ArrayHandling, context: str = "parameter") -> None:
        """Validate array handling option.

        Args:
            value: Array handling option to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError
        valid_options = ["separate", "inline", "skip"]

        if value not in valid_options:
            raise error_class(
                f"Invalid array handling option: '{value}'. "
                f"Must be one of: {', '.join(valid_options)}"
            )

    @staticmethod
    def validate_error_handling(
        value: ErrorHandling, context: str = "parameter"
    ) -> None:
        """Validate error handling option.

        Args:
            value: Error handling option to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError
        valid_options = ["raise", "skip", "warn"]

        if value not in valid_options:
            raise error_class(
                f"Invalid error handling option: '{value}'. "
                f"Must be one of: {', '.join(valid_options)}"
            )

    @staticmethod
    def validate_recovery_strategy(value: str, context: str = "parameter") -> None:
        """Validate recovery strategy.

        Args:
            value: Recovery strategy to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError
        valid_strategies = ["strict", "skip", "partial"]

        if value not in valid_strategies:
            raise error_class(
                f"Invalid recovery strategy: '{value}'. "
                f"Must be one of: {', '.join(valid_strategies)}"
            )

    @staticmethod
    def validate_id_field(value: IdSource, context: str = "parameter") -> None:
        """Validate ID field specification.

        Args:
            value: ID field specification to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError

        if value is None:
            return  # None is valid

        if isinstance(value, str):
            ParameterValidator.validate_field_name(value, context)
        elif isinstance(value, dict):
            for path, field in value.items():
                if not path:
                    raise error_class("Path key cannot be empty in ID field mapping")
                ParameterValidator.validate_field_name(field, context)
        else:
            raise error_class(
                f"ID field must be a string, dictionary, or None, "
                f"got {type(value).__name__}"
            )

    @staticmethod
    def validate_format(value: Optional[str], context: str = "parameter") -> None:
        """Validate output format.

        Args:
            value: Format to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        if value is None:
            return  # None is valid for auto-detection

        error_class = ValidationError if context == "api" else ConfigurationError
        valid_formats = ["json", "csv", "parquet"]

        if not isinstance(value, str):
            raise error_class(f"Format must be a string, got {type(value).__name__}")

        if value not in valid_formats:
            raise error_class(
                f"Invalid format: '{value}'. Must be one of: {', '.join(valid_formats)}"
            )

    @staticmethod
    def validate_data_input(value: Any, context: str = "parameter") -> None:
        """Validate data input.

        Args:
            value: Data input to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError

        if value is None:
            raise error_class("Data input cannot be None")

        # Allow common data types
        valid_types = (dict, list, str, bytes)
        if not isinstance(value, valid_types):
            # Also allow Path objects
            try:
                from pathlib import Path

                if isinstance(value, Path):
                    return
            except ImportError:
                pass

            raise error_class(
                f"Data input must be a dictionary, list, string, bytes, "
                f"or Path object, got {type(value).__name__}"
            )

    @staticmethod
    def validate_cache_size(value: int, context: str = "parameter") -> None:
        """Validate cache size.

        Args:
            value: Cache size to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError

        if not isinstance(value, int):
            raise error_class(
                f"Cache size must be an integer, got {type(value).__name__}"
            )

        if value < 0:
            raise error_class(f"Cache size must be non-negative, got {value}")

    @staticmethod
    def validate_max_depth(value: int, context: str = "parameter") -> None:
        """Validate maximum recursion depth.

        Args:
            value: Maximum depth to validate
            context: Context for error messages (api/config/parameter)

        Raises:
            ValidationError: For API context
            ConfigurationError: For config context
        """
        error_class = ValidationError if context == "api" else ConfigurationError

        if not isinstance(value, int):
            raise error_class(
                f"Maximum depth must be an integer, got {type(value).__name__}"
            )

        if value <= 0:
            raise error_class(f"Maximum depth must be positive, got {value}")

        if value > 1000:
            import warnings

            warnings.warn(
                f"Very high maximum recursion depth ({value}) may cause "
                f"stack overflow or performance issues.",
                stacklevel=3,
            )


# Convenience functions for API validation
def validate_api_parameters(**params: Any) -> None:
    """Validate API parameters using unified validation logic.

    Args:
        **params: Dictionary of parameter names to values

    Raises:
        ValidationError: If any parameter validation fails
    """
    validator = ParameterValidator()

    if "data" in params:
        validator.validate_data_input(params["data"], "api")

    if "batch_size" in params:
        validator.validate_batch_size(params["batch_size"], "api")

    if "separator" in params:
        validator.validate_separator(params["separator"], "api")

    if "nested_threshold" in params:
        validator.validate_nested_threshold(params["nested_threshold"], "api")

    if "arrays" in params:
        validator.validate_arrays(params["arrays"], "api")

    if "errors" in params:
        validator.validate_error_handling(params["errors"], "api")

    if "id_field" in params:
        validator.validate_id_field(params["id_field"], "api")

    if "parent_id_field" in params:
        validator.validate_field_name(params["parent_id_field"], "api")

    if "format" in params:
        validator.validate_format(params["format"], "api")

    if "name" in params:
        if not isinstance(params["name"], str) or not params["name"]:
            raise ValidationError("Name must be a non-empty string")


# Convenience functions for config validation
def validate_config_parameters(**params: Any) -> None:
    """Validate configuration parameters using unified validation logic.

    Args:
        **params: Dictionary of parameter names to values

    Raises:
        ConfigurationError: If any parameter validation fails
    """
    validator = ParameterValidator()

    if "separator" in params:
        validator.validate_separator(params["separator"], "config")

    if "nested_threshold" in params:
        validator.validate_nested_threshold(params["nested_threshold"], "config")

    if "batch_size" in params:
        validator.validate_batch_size(params["batch_size"], "config")

    if "recovery_strategy" in params:
        validator.validate_recovery_strategy(params["recovery_strategy"], "config")

    if "id_field" in params:
        validator.validate_field_name(params["id_field"], "config")

    if "parent_field" in params:
        validator.validate_field_name(params["parent_field"], "config")

    if "time_field" in params:
        validator.validate_field_name(params["time_field"], "config")

    if "cache_maxsize" in params:
        validator.validate_cache_size(params["cache_maxsize"], "config")

    if "max_depth" in params:
        validator.validate_max_depth(params["max_depth"], "config")
