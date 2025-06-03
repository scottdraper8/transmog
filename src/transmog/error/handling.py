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

# Load recovery strategy with fallback placeholder
try:
    from ..error.recovery import RecoveryStrategy
except (ImportError, ModuleNotFoundError):
    # Placeholder class for type hinting
    class RecoveryStrategy:  # type: ignore
        """Placeholder for RecoveryStrategy."""

        pass


# Type variables for return type preservation
T = TypeVar("T")
R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])

# Module logger
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
    # Default format based on output destination
    if log_format is None:
        log_format = (
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            if log_file
            else "%(levelname)s: %(message)s"
        )

    # Configure logger
    logger = logging.getLogger("transmog")
    logger.setLevel(level)

    # Clear existing handlers
    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    # Handler collection
    handlers: list[Union[StreamHandler, FileHandler]] = []

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(log_format))
    handlers.append(console_handler)

    # Add file handler if specified
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)
            file_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(file_handler)
        except Exception as e:
            logger.warning(f"Failed to create log file at {log_file}: {e}")

    # Register all handlers
    for handler in handlers:
        logger.addHandler(handler)

    # Confirm configuration
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
        # Add position context for errors
        line_col = f"line {e.lineno}, column {e.colno}" if hasattr(e, "lineno") else ""
        raise ParsingError(f"Invalid JSON format at {line_col}: {str(e)}") from e
    except Exception as e:
        # Generic error handler
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
) -> Callable[[F], F]: ...


@overload
def error_context(
    context_message: str,
    wrap_as: Callable[[Exception], Exception],
    reraise: bool = True,
    log_exceptions: bool = True,
) -> Callable[[F], F]: ...


def error_context(
    context_message: str,
    wrap_as: Optional[Callable[[Exception], Exception]] = None,
    reraise: bool = True,
    log_exceptions: bool = True,
) -> Callable[[F], F]:
    """Decorator to add context to errors raised by a function.

    Args:
        context_message: Error context prefix
        wrap_as: Function to wrap exceptions
        reraise: Whether to reraise exceptions
        log_exceptions: Whether to log exceptions

    Returns:
        Decorated function with proper type preservation
    """
    # Default to ProcessingError if no wrapper provided
    if wrap_as is None:

        def wrap_as(e: Exception) -> ProcessingError:
            return ProcessingError(f"{context_message}: {str(e)}")

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Create detailed error message
                func_name = func.__qualname__
                arg_str = ", ".join(
                    [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs.items()]
                )
                if len(arg_str) > 100:
                    arg_str = arg_str[:97] + "..."

                error_msg = f"{context_message} in {func_name}({arg_str}): {str(e)}"

                # Log based on configuration
                if log_exceptions:
                    logger.error(error_msg)
                    if not isinstance(e, TransmogError):
                        logger.debug(
                            "".join(
                                traceback.format_exception(type(e), e, e.__traceback__)
                            )
                        )

                # Attempt recovery if strategy exists
                recovery_strategy = kwargs.get("recovery_strategy")
                if recovery_strategy and hasattr(recovery_strategy, "recover"):
                    # Extract path context for recovery
                    path = kwargs.get("path_parts", [])
                    if "parent_path" in kwargs and not path and kwargs["parent_path"]:
                        separator = kwargs.get("separator", "_")
                        path = kwargs["parent_path"].split(separator)

                    try:
                        recovery_result = recovery_strategy.recover(e, path=path)
                        if recovery_result is not None:
                            return recovery_result
                    except Exception as recovery_err:
                        logger.error(
                            f"Recovery failed: {str(recovery_err)}. Original: {str(e)}"
                        )

                # Wrap or reraise based on configuration
                if reraise:
                    if wrap_as and not isinstance(e, TransmogError):
                        wrapped = wrap_as(e)
                        if isinstance(wrapped, type):
                            raise wrapped(error_msg) from e
                        else:
                            raise wrapped from e
                    else:
                        raise

                # Default return value for non-reraising case
                return None

        return cast(F, wrapper)

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

        # No recovery options available
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

    # Run custom validation
    if validation_func:
        is_valid, error_msg = validation_func(data)
        if not is_valid:
            message = f"Invalid {param_name}"
            if error_msg:
                message += f": {error_msg}"
            raise ValidationError(message)
