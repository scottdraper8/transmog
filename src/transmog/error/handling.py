"""Error handling utilities for the Transmog package.

This module provides functions and decorators for error handling
and recovery in the Transmog package.
"""

import functools
import json
import logging
import traceback
from logging import FileHandler, StreamHandler
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
    cast,
    overload,
)

from transmog.error.exceptions import (
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
    """Set up logging for the Transmog package.

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
    handlers: list[Union[StreamHandler, FileHandler]] = []

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
    """Safe JSON loading function with better error handling.

    Args:
        s: JSON string or bytes to parse

    Returns:
        Parsed JSON data

    Raises:
        ParsingError: On JSON parsing errors
    """
    try:
        # Handle bytes input
        if isinstance(s, bytes):
            try:
                return json.loads(s.decode("utf-8", errors="replace"))
            except Exception as e:
                raise ParsingError(f"Failed to decode bytes: {str(e)}") from e

        # Handle string input
        return json.loads(s)
    except json.JSONDecodeError as e:
        # Provide detailed error context
        line_col = f"line {e.lineno}, column {e.colno}" if hasattr(e, "lineno") else ""
        raise ParsingError(f"Invalid JSON format at {line_col}: {str(e)}") from e
    except Exception as e:
        # Generic fallback
        raise ParsingError(f"JSON parsing error: {str(e)}") from e


def check_dependency(package_name: str, feature: Optional[str] = None) -> bool:
    """Check if a dependency is available.

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
    """Check if a dependency is available and raise error if not.

    Args:
        package_name: Name of the package to check
        feature: Optional feature name requiring this dependency

    Raises:
        MissingDependencyError: If the dependency is not available
    """
    try:
        __import__(package_name)
    except ImportError as e:
        raise MissingDependencyError(
            f"{package_name} is required but not installed",
            package=package_name,
            feature=feature or f"transmog.{package_name}",
        ) from e


@overload
def error_context(
    context_message: str,
    wrap_as: None = None,
    reraise: bool = True,
    log_exceptions: bool = True,
) -> Callable[[Callable[..., R]], Callable[..., Optional[R]]]: ...


@overload
def error_context(
    context_message: str,
    wrap_as: Callable[[Exception], Exception],
    reraise: bool = True,
    log_exceptions: bool = True,
) -> Callable[[Callable[..., R]], Callable[..., Optional[R]]]: ...


def error_context(
    context_message: str,
    wrap_as: Optional[Callable[[Exception], Exception]] = None,
    reraise: bool = True,
    log_exceptions: bool = True,
) -> Callable[[Callable[..., R]], Callable[..., Optional[R]]]:
    """Decorator to add context to errors raised by a function.

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

        def wrap_as(e: Exception) -> ProcessingError:
            return ProcessingError(f"{context_message}: {str(e)}")

    def decorator(func: Callable[..., R]) -> Callable[..., Optional[R]]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Optional[R]:
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
                        recovery_result = recovery_strategy.recover(e, path=path)
                        return cast(R, recovery_result)
                    except Exception as re:
                        # If recovery fails, continue with normal error handling
                        logger.warning(f"Recovery failed: {str(re)}")

                # Wrap exception and handle reraising
                new_exception = wrap_as(e)
                if hasattr(new_exception, "__cause__"):
                    new_exception.__cause__ = e
                if reraise:
                    raise new_exception from e
                # Return None when not reraising
                return None

        return wrapper

    return decorator


def try_with_recovery(
    func: Callable[..., T],
    recovery_func: Optional[Callable[[Exception], T]] = None,
    recovery_strategy: Optional[RecoveryStrategy] = None,
    *func_args: Any,
    **func_kwargs: Any,
) -> T:
    """Execute a function with recovery handling.

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
        if recovery_strategy and hasattr(recovery_strategy, "recover"):
            try:
                return cast(T, recovery_strategy.recover(e))
            except Exception as re:
                logger.warning(f"Recovery strategy failed: {str(re)}")
                # Fall through to recovery function

        if recovery_func:
            return recovery_func(e)

        # If no recovery is possible, re-raise the exception
        raise


def validate_input(
    data: Any,
    expected_type: Union[type, tuple[type, ...]],
    param_name: str,
    allow_none: bool = False,
    validation_func: Optional[Callable[[Any], tuple[bool, Optional[str]]]] = None,
) -> None:
    """Validate that the input data is of the expected type.

    Args:
        data: The data to validate
        expected_type: The expected type(s) of the data
        param_name: The name of the parameter (for error messages)
        allow_none: Whether None is an allowed value
        validation_func: Optional function to perform additional validation

    Raises:
        ValidationError: If validation fails
    """
    # Check for None if not allowed
    if data is None and not allow_none:
        raise ValidationError(f"{param_name} must not be None")

    # Skip type check if None and allowed
    if data is None and allow_none:
        return

    # Type check
    if not isinstance(data, expected_type):
        actual_type = type(data).__name__
        expected_type_name = (
            expected_type.__name__
            if isinstance(expected_type, type)
            else " or ".join(t.__name__ for t in expected_type)
        )
        raise ValidationError(
            f"{param_name} must be of type {expected_type_name}, got {actual_type}"
        )

    # Additional validation if provided
    if validation_func:
        is_valid, error_msg = validation_func(data)
        if not is_valid:
            message = f"Invalid {param_name}"
            if error_msg:
                message += f": {error_msg}"
            raise ValidationError(message)
