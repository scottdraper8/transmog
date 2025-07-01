"""API Parameter Validation for Transmog.

This module provides validation functions specifically for API parameters,
separate from configuration validation. Always uses ValidationError for
consistency in API error handling.
"""

from typing import Any, Literal, Union

from transmog.error import ValidationError

# Type aliases used in API
ArrayHandling = Literal["separate", "inline", "skip"]
ErrorHandling = Literal["raise", "skip", "warn"]
IdSource = Union[str, dict[str, str], None]


def validate_api_parameters(**params: Any) -> None:
    """Validate API parameters and raise ValidationError for issues.

    Args:
        **params: Dictionary of parameter names to values

    Raises:
        ValidationError: If any parameter validation fails
    """
    if "data" in params:
        validate_data_api(params["data"])

    if "batch_size" in params:
        validate_batch_size_api(params["batch_size"])

    if "separator" in params:
        validate_separator_api(params["separator"])

    if "nested_threshold" in params:
        validate_nested_threshold_api(params["nested_threshold"])

    if "arrays" in params:
        validate_arrays_api(params["arrays"])

    if "errors" in params:
        validate_errors_api(params["errors"])

    if "id_field" in params:
        validate_id_field_api(params["id_field"])

    if "parent_id_field" in params:
        validate_parent_id_field_api(params["parent_id_field"])

    if "format" in params:
        validate_format_api(params["format"])


def validate_batch_size_api(batch_size: Any) -> None:
    """Validate batch_size parameter for API functions.

    Args:
        batch_size: Value to validate

    Raises:
        ValidationError: If batch_size is invalid
    """
    if not isinstance(batch_size, int):
        raise ValidationError(
            f"batch_size must be an integer, got {type(batch_size).__name__}"
        )

    if batch_size <= 0:
        raise ValidationError(f"batch_size must be positive, got {batch_size}")


def validate_separator_api(separator: Any) -> None:
    """Validate separator parameter for API functions.

    Args:
        separator: Value to validate

    Raises:
        ValidationError: If separator is invalid
    """
    if not isinstance(separator, str):
        raise ValidationError(
            f"separator must be a string, got {type(separator).__name__}"
        )

    if not separator:
        raise ValidationError("separator must be a non-empty string")

    if len(separator) > 10:
        raise ValidationError(
            f"separator too long (max 10 characters), got {len(separator)}"
        )


def validate_nested_threshold_api(nested_threshold: Any) -> None:
    """Validate nested_threshold parameter for API functions.

    Args:
        nested_threshold: Value to validate

    Raises:
        ValidationError: If nested_threshold is invalid
    """
    if not isinstance(nested_threshold, int):
        raise ValidationError(
            f"nested_threshold must be an integer, got "
            f"{type(nested_threshold).__name__}"
        )

    if nested_threshold < 1:
        raise ValidationError(
            f"nested_threshold must be positive, got {nested_threshold}"
        )


def validate_arrays_api(arrays: Any) -> None:
    """Validate arrays parameter for API functions.

    Args:
        arrays: Value to validate

    Raises:
        ValidationError: If arrays value is invalid
    """
    valid_values = ["separate", "inline", "skip"]

    if arrays not in valid_values:
        raise ValidationError(f"arrays must be one of {valid_values}, got {arrays!r}")


def validate_errors_api(errors: Any) -> None:
    """Validate errors parameter for API functions.

    Args:
        errors: Value to validate

    Raises:
        ValidationError: If errors value is invalid
    """
    valid_values = ["raise", "skip", "warn"]

    if errors not in valid_values:
        raise ValidationError(f"errors must be one of {valid_values}, got {errors!r}")


def validate_id_field_api(id_field: Any) -> None:
    """Validate id_field parameter for API functions.

    Args:
        id_field: Value to validate

    Raises:
        ValidationError: If id_field is invalid
    """
    if id_field is None:
        return  # None is valid

    if isinstance(id_field, str):
        if not id_field:
            raise ValidationError("id_field string cannot be empty")
        return

    if isinstance(id_field, dict):
        if not id_field:
            raise ValidationError("id_field dict cannot be empty")

        for key, value in id_field.items():
            if not isinstance(key, str) or not key:
                raise ValidationError("id_field dict keys must be non-empty strings")
            if not isinstance(value, str) or not value:
                raise ValidationError("id_field dict values must be non-empty strings")
        return

    raise ValidationError(
        f"id_field must be None, string, or dict, got {type(id_field).__name__}"
    )


def validate_parent_id_field_api(parent_id_field: Any) -> None:
    """Validate parent_id_field parameter for API functions.

    Args:
        parent_id_field: Value to validate

    Raises:
        ValidationError: If parent_id_field is invalid
    """
    if not isinstance(parent_id_field, str):
        raise ValidationError(
            f"parent_id_field must be a string, got {type(parent_id_field).__name__}"
        )

    if not parent_id_field:
        raise ValidationError("parent_id_field cannot be empty")


def validate_format_api(format_value: Any) -> None:
    """Validate format parameter for API functions.

    Args:
        format_value: Value to validate

    Raises:
        ValidationError: If format is invalid
    """
    if format_value is None:
        return  # None is valid for auto-detection

    if not isinstance(format_value, str):
        raise ValidationError(
            f"format must be a string or None, got {type(format_value).__name__}"
        )

    valid_formats = ["csv", "json", "parquet"]
    if format_value not in valid_formats:
        raise ValidationError(
            f"format must be one of {valid_formats}, got {format_value!r}"
        )


def validate_name_api(name: Any) -> None:
    """Validate name parameter for API functions.

    Args:
        name: Value to validate

    Raises:
        ValidationError: If name is invalid
    """
    if not isinstance(name, str):
        raise ValidationError(f"name must be a string, got {type(name).__name__}")

    if not name:
        raise ValidationError("name cannot be empty")


def validate_data_api(data: Any) -> None:
    """Validate data parameter for API functions.

    Args:
        data: Value to validate

    Raises:
        ValidationError: If data is invalid
    """
    import json
    from pathlib import Path

    if data is None:
        raise ValidationError("data cannot be None")

    # If it's a string, check if it's valid JSON or a valid file path
    if isinstance(data, str):
        # Check if it's a file path first
        try:
            path = Path(data)
            if path.exists():
                return  # Valid file path
        except (OSError, ValueError):
            pass  # Not a valid path, continue to JSON check

        # Check if it's valid JSON
        try:
            json.loads(data)
            return  # Valid JSON string
        except json.JSONDecodeError:
            raise ValidationError(
                "String data must be valid JSON or a valid file path"
            ) from None


def validate_path_api(path: Any) -> None:
    """Validate path parameter for API functions.

    Args:
        path: Value to validate

    Raises:
        ValidationError: If path is invalid
    """
    from pathlib import Path

    if isinstance(path, (str, Path)):
        return  # Valid path types

    raise ValidationError(
        f"path must be a string or Path object, got {type(path).__name__}"
    )
