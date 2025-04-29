"""
Error handling utilities for the Transmog package.

This module provides decorators and utility functions for consistent error handling,
logging, and recovery strategies throughout the package.
"""

import functools
import inspect
import json
import logging
import os
import sys
import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Union, cast

from .exceptions import (
    CircularReferenceError,
    ConfigurationError,
    FileError,
    MissingDependencyError,
    TransmogError,
    OutputError,
    ParsingError,
    ProcessingError,
    ValidationError,
)

# Set up logging
logger = logging.getLogger("transmog")

# Type variables for function signatures
T = TypeVar("T")
R = TypeVar("R")


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_format: Optional[str] = None,
) -> None:
    """
    Set up logging for the Transmog package.

    Args:
        level: Logging level (default: INFO)
        log_file: Optional path to log file
        log_format: Custom log format string
    """
    if log_format is None:
        log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    # Create formatter
    formatter = logging.Formatter(log_format)

    # Configure root logger
    root_logger = logging.getLogger("transmog")
    root_logger.setLevel(level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Failed to set up log file '{log_file}': {str(e)}")


def safe_json_loads(s: Union[str, bytes]) -> Any:
    """
    Safely load JSON data with better error handling.

    This function attempts to use orjson first (for better performance),
    falling back to standard library json if orjson is not available.

    Args:
        s: JSON string to parse

    Returns:
        Parsed JSON data

    Raises:
        ParsingError: If the input is not valid JSON
    """
    # Try orjson first (much faster)
    try:
        import orjson

        try:
            return orjson.loads(s)
        except (orjson.JSONDecodeError, ValueError, TypeError) as e:
            # Get the first 100 characters for error context
            context = str(s)[:100] + ("..." if len(str(s)) > 100 else "")
            raise ParsingError(f"Invalid JSON data: {str(e)}. Context: {context}")
    except ImportError:
        # Fall back to standard json
        try:
            return json.loads(s)
        except json.JSONDecodeError as e:
            # Get the first 100 characters for error context
            context = str(s)[:100] + ("..." if len(str(s)) > 100 else "")
            raise ParsingError(f"Invalid JSON data: {str(e)}. Context: {context}")


def check_dependency(package_name: str, feature: Optional[str] = None) -> bool:
    """
    Check if an optional dependency is installed.

    Args:
        package_name: The package to check
        feature: The feature that requires this package (for error messages)

    Returns:
        True if installed, False otherwise

    Raises:
        MissingDependencyError: If raise_error is True and dependency is missing
    """
    try:
        __import__(package_name)
        return True
    except ImportError:
        return False


def require_dependency(package_name: str, feature: Optional[str] = None) -> None:
    """
    Require that an optional dependency is installed.

    Args:
        package_name: The package to check
        feature: The feature that requires this package

    Raises:
        MissingDependencyError: If dependency is missing
    """
    if not check_dependency(package_name):
        feature_name = feature or package_name
        raise MissingDependencyError(
            f"{package_name} is required but not installed",
            package=package_name,
            feature=feature,
        )


def handle_circular_reference(
    obj_id: int,
    visited: Set[int],
    path: List[str],
    max_depth: Optional[int] = None,
) -> None:
    """
    Check for circular references and handle appropriately.

    Args:
        obj_id: Object ID to check
        visited: Set of already visited object IDs
        path: Current object path for error reporting
        max_depth: Maximum allowed reference depth

    Raises:
        CircularReferenceError: If circular reference is detected
    """
    if obj_id in visited:
        raise CircularReferenceError(
            "Object referenced multiple times in data structure", path=path
        )

    if max_depth is not None and len(path) > max_depth:
        raise CircularReferenceError(
            f"Maximum nesting depth exceeded ({max_depth})", path=path
        )

    # Add object ID to visited set
    visited.add(obj_id)


def error_context(
    context_message: str,
    *,
    log_exceptions: bool = True,
    reraise: bool = True,
    wrap_as: Optional[Callable[[Exception], Exception]] = None,
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """
    Decorator to add context to exceptions and handle logging consistently.

    Args:
        context_message: Context message to add to exceptions
        log_exceptions: Whether to log exceptions
        reraise: Whether to reraise exceptions
        wrap_as: Function to wrap exceptions (or None to keep original)

    Returns:
        Decorated function
    """

    def decorator(func: Callable[..., R]) -> Callable[..., R]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Format the error message with function info
                func_name = func.__qualname__
                arg_str = ", ".join(
                    [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
                )
                if len(arg_str) > 100:
                    arg_str = arg_str[:97] + "..."

                error_msg = f"{context_message} in {func_name}({arg_str}): {str(e)}"

                # Log the exception if requested
                if log_exceptions:
                    logger.error(error_msg)
                    if not isinstance(e, TransmogError):
                        logger.debug(
                            "".join(
                                traceback.format_exception(type(e), e, e.__traceback__)
                            )
                        )

                # Wrap exception if requested
                if wrap_as is not None:
                    new_exception = wrap_as(e)
                    if hasattr(new_exception, "__cause__"):
                        new_exception.__cause__ = e
                    if reraise:
                        raise new_exception
                elif reraise:
                    raise

                return None  # This is never reached if reraise=True

        return wrapper

    return decorator


def try_with_recovery(
    func: Callable[..., T],
    recovery_func: Callable[[Exception], T],
    *args: Any,
    **kwargs: Any,
) -> T:
    """
    Execute a function with recovery handling.

    This function executes the given function with the specified arguments,
    and if an exception is raised, calls the recovery function with the
    exception as its argument.

    Args:
        func: The function to execute
        recovery_func: Function to call if an exception is raised
        *args: Positional arguments to pass to func
        **kwargs: Keyword arguments to pass to func

    Returns:
        The result of func or recovery_func if an exception occurred
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        return recovery_func(e)


def validate_input(
    data: Any,
    expected_type: Union[type, Tuple[type, ...]],
    param_name: str,
    allow_none: bool = False,
    validation_func: Optional[Callable[[Any], Tuple[bool, Optional[str]]]] = None,
) -> None:
    """
    Validate input data against expected type and custom validation.

    Args:
        data: Data to validate
        expected_type: Expected type or tuple of types
        param_name: Parameter name for error messages
        allow_none: Whether None is allowed
        validation_func: Optional custom validation function

    Raises:
        ValidationError: If validation fails
    """
    # Check if None is allowed
    if data is None:
        if allow_none:
            return
        raise ValidationError(f"Parameter '{param_name}' cannot be None")

    # Check type
    if not isinstance(data, expected_type):
        type_names = (
            [t.__name__ for t in expected_type]
            if isinstance(expected_type, tuple)
            else [expected_type.__name__]
        )
        expected_type_str = " or ".join(type_names)
        actual_type = type(data).__name__
        raise ValidationError(
            f"Parameter '{param_name}' must be of type {expected_type_str}, "
            f"got {actual_type}"
        )

    # Run custom validation if provided
    if validation_func is not None:
        is_valid, error_message = validation_func(data)
        if not is_valid:
            raise ValidationError(
                f"Validation failed for parameter '{param_name}': {error_message}"
            )
