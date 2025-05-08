"""
Error handling utilities for the Transmog package.

This module provides functions and decorators for error handling
and recovery in the Transmog package.
"""

import functools
import importlib
import json
import logging
import os
import traceback
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
    cast,
    Tuple,
    Type,
)

from transmog.error.exceptions import (
    ConfigurationError,
    FileError,
    MissingDependencyError,
    ParsingError,
    ProcessingError,
    TransmogError,
    ValidationError,
)

# Optional dependency on recovery strategies
try:
    from ..error.recovery import RecoveryStrategy
except (ImportError, ModuleNotFoundError):
    # Define a placeholder for type hints
    class RecoveryStrategy:  # type: ignore
        """Placeholder for RecoveryStrategy."""

        pass


# Type variables for return type preservation
T = TypeVar("T")
R = TypeVar("R")

# Setup module logger
logger = logging.getLogger(__name__)


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_format: Optional[str] = None,
) -> None:
    """
    Set up logging for the Transmog package.

    Args:
        level: Logging level
        log_file: Optional file path for log output
        log_format: Optional custom log format
    """
    # Use a reasonable default format if none specified
    if log_format is None:
        log_format = (
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            if log_file
            else "%(levelname)s: %(message)s"
        )

    # Configure basic logger
    logger = logging.getLogger("transmog")
    logger.setLevel(level)

    # Remove existing handlers
    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    # Create handlers based on configuration
    handlers = []

    # Console handler always added
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(log_format))
    handlers.append(console_handler)

    # File handler if specified
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)
            file_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(file_handler)
        except Exception as e:
            logger.warning(f"Failed to create log file at {log_file}: {e}")

    # Add all handlers
    for handler in handlers:
        logger.addHandler(handler)

    # Log setup completion
    logger.debug(f"Logging configured with level {level}")


def safe_json_loads(s: Union[str, bytes]) -> Any:
    """
    Safely load JSON data with enhanced error handling.

    Args:
        s: JSON string or bytes to parse

    Returns:
        Parsed JSON data

    Raises:
        ParsingError: If JSON parsing fails
    """
    import json

    try:
        if isinstance(s, bytes):
            # Try UTF-8 first
            return json.loads(s.decode("utf-8"))
        return json.loads(s)
    except UnicodeDecodeError:
        # If UTF-8 fails, try with errors='replace'
        try:
            return json.loads(s.decode("utf-8", errors="replace"))
        except Exception as e:
            raise ParsingError(f"Failed to decode bytes: {str(e)}")
    except json.JSONDecodeError as e:
        # Provide detailed error context
        line_col = f"line {e.lineno}, column {e.colno}" if hasattr(e, "lineno") else ""
        raise ParsingError(f"Invalid JSON format at {line_col}: {str(e)}")
    except Exception as e:
        # Generic fallback
        raise ParsingError(f"JSON parsing error: {str(e)}")


def check_dependency(package_name: str, feature: Optional[str] = None) -> bool:
    """
    Check if a dependency is available.

    Args:
        package_name: Name of the package to check
        feature: Optional feature name requiring this package

    Returns:
        True if the dependency is available, False otherwise
    """
    try:
        __import__(package_name)
        return True
    except ImportError:
        return False


def require_dependency(package_name: str, feature: Optional[str] = None) -> None:
    """
    Require a dependency, raising an error if not available.

    Args:
        package_name: Name of the package to require
        feature: Optional feature name requiring this package

    Raises:
        MissingDependencyError: If the dependency is not available
    """
    try:
        __import__(package_name)
    except ImportError:
        feature_name = feature or package_name
        raise MissingDependencyError(
            f"{package_name} is required but not installed",
            package=package_name,
            feature=feature,
        )


def error_context(
    context_message: str,
    wrap_as: Optional[Callable[[Exception], Exception]] = None,
    reraise: bool = True,
    log_exceptions: bool = True,
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """
    Decorator to add context to errors raised by a function.

    Args:
        context_message: Error context prefix
        wrap_as: Function to wrap exceptions
        reraise: Whether to reraise exceptions
        log_exceptions: Whether to log exceptions

    Returns:
        Decorated function
    """
    # Default wrapper uses ProcessingError if not specified
    if wrap_as is None:
        wrap_as = lambda e: ProcessingError(f"{context_message}: {str(e)}")

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

                # Try recovery if recovery strategy is available
                recovery_strategy = kwargs.get("recovery_strategy")
                if recovery_strategy and hasattr(recovery_strategy, "recover"):
                    # Get current path parts if available for better error context
                    path = kwargs.get("path_parts", [])
                    if "parent_path" in kwargs and not path and kwargs["parent_path"]:
                        # Try to extract path from parent_path using separator
                        separator = kwargs.get("separator", "_")
                        path = kwargs["parent_path"].split(separator)

                    try:
                        # Try generic recovery with path context
                        return recovery_strategy.recover(e, path=path)
                    except Exception as re:
                        # If recovery fails, continue with normal error handling
                        logger.warning(f"Recovery failed: {str(re)}")

                # Wrap exception if requested
                if wrap_as is not None:
                    new_exception = wrap_as(e)
                    if hasattr(new_exception, "__cause__"):
                        new_exception.__cause__ = e
                    if reraise:
                        raise new_exception
                    return cast(R, None)  # Return None if not reraising

                # Propagate the original exception
                if reraise:
                    raise
                return cast(R, None)  # Return None if not reraising

        return wrapper

    return decorator


def try_with_recovery(
    func: Callable[..., T],
    recovery_func: Optional[Callable[[Exception], T]] = None,
    recovery_strategy: Optional[RecoveryStrategy] = None,
    *func_args: Any,
    **func_kwargs: Any,
) -> T:
    """
    Execute a function with recovery handling.

    This function executes the given function with the specified arguments,
    and if an exception is raised, calls the recovery function with the
    exception as its argument.

    Args:
        func: The function to execute
        recovery_func: Function to call if an exception is raised
        recovery_strategy: Optional recovery strategy to use
        *func_args: Positional arguments to pass to func
        **func_kwargs: Keyword arguments to pass to func

    Returns:
        The result of func or recovery_func if an exception occurred
    """
    try:
        return func(*func_args, **func_kwargs)
    except Exception as e:
        if recovery_func:
            return recovery_func(e)
        elif recovery_strategy:
            return recovery_strategy.recover(e)
        else:
            raise


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
        data: The data to validate
        expected_type: Expected type or types
        param_name: Parameter name for error messages
        allow_none: Whether None is allowed
        validation_func: Optional custom validation function

    Raises:
        ValidationError: If validation fails
    """
    # Check for None if not allowed
    if data is None and not allow_none:
        raise ValidationError(
            f"Parameter '{param_name}' cannot be None", {param_name: "none_not_allowed"}
        )

    # Skip type check if None is provided and allowed
    if data is None and allow_none:
        return

    # Type check
    if not isinstance(data, expected_type):
        type_name = (
            ", ".join(t.__name__ for t in expected_type)
            if isinstance(expected_type, tuple)
            else expected_type.__name__
        )
        actual_type = type(data).__name__
        raise ValidationError(
            f"Parameter '{param_name}' has wrong type",
            {param_name: f"expected {type_name}, got {actual_type}"},
        )

    # Custom validation if provided
    if validation_func:
        is_valid, error_msg = validation_func(data)
        if not is_valid:
            raise ValidationError(
                f"Parameter '{param_name}' failed validation",
                {param_name: error_msg or "invalid_value"},
            )
