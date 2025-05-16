"""Recovery strategies for error handling.

Provides different strategies for recovering from errors during processing.
"""

import functools
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, NoReturn, Optional, TypeVar, Union, cast

from .exceptions import (
    FileError,
    ParsingError,
    ProcessingError,
    TransmogError,
)

# Set up logger
logger = logging.getLogger("transmog")

# Type variable for generic return types
T = TypeVar("T")


class RecoveryStrategy(ABC):
    """Abstract base class for recovery strategies.

    Recovery strategies define how to handle different types of errors
    during data processing operations. They provide policies for:

    - Whether to continue or abort on error
    - What kind of partial results to return
    - Logging and error reporting behavior
    """

    @abstractmethod
    def recover(
        self, error: Exception, entity_name: Optional[str] = None, **kwargs: Any
    ) -> Any:
        """Recover from an error according to the strategy.

        Args:
            error: The exception to recover from
            entity_name: Optional name of the entity being processed
            **kwargs: Additional context for error handling

        Returns:
            Recovered data or raises an exception
        """
        pass

    def recover_or_raise(
        self,
        func: Callable[..., T],
        *args: Any,
        exception_type: Optional[type[Exception]] = None,
        entity_name: Optional[str] = None,
        **kwargs: Any,
    ) -> T:
        """Attempt to execute a function and recover from errors.

        Args:
            func: Function to execute
            *args: Positional arguments to pass to the function
            exception_type: Specific exception type to catch (default: all exceptions)
            entity_name: Optional entity name for context
            **kwargs: Keyword arguments to pass to the function

        Returns:
            The return value of the function, or result of recovery
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Check if this is a specific exception we want to handle
            if exception_type is not None and not isinstance(e, exception_type):
                raise

            # Check if this is TransmogError that has built-in recovery support
            if hasattr(e, "recover") and callable(e.recover):
                # Use the error's recovery mechanism
                return cast(T, e.recover())

            # For strict recovery, just raise (don't use self.recover)
            if (
                isinstance(e, TransmogError)
                and e.recover_strategy == "strict"
                and isinstance(self, StrictRecovery)
            ):
                raise

            # Otherwise, wrap in ProcessingError for better context
            if not isinstance(e, ProcessingError):
                error = ProcessingError(f"Error processing data: {e}")
            else:
                error = e

            return cast(T, self.recover(error, entity_name=entity_name, **kwargs))


class StrictRecovery(RecoveryStrategy):
    """Recovery strategy that re-raises all errors.

    This strategy provides no recovery, instead ensuring that errors
    are immediately propagated. Use this strategy when:

    - Data integrity is critical
    - You want to fail fast on any error
    - Errors need immediate attention
    """

    def recover(
        self, error: Exception, entity_name: Optional[str] = None, **kwargs: Any
    ) -> NoReturn:
        """Re-raise all errors without recovery."""
        raise error


class SkipAndLogRecovery(RecoveryStrategy):
    """Recovery strategy that logs errors and continues processing.

    This strategy is useful for batch processing where a few records
    failing shouldn't stop the entire process.
    """

    def __init__(self, log_level: int = logging.WARNING):
        """Initialize with the specified log level.

        Args:
            log_level: Logging level to use for error messages
        """
        self.log_level = log_level

    def recover(
        self, error: Exception, entity_name: Optional[str] = None, **kwargs: Any
    ) -> Union[None, dict[str, Any]]:
        """Recover from any exception by logging and returning appropriate empty values.

        Args:
            error: The exception to recover from
            entity_name: Optional entity name for context
            **kwargs: Additional context for error handling

        Returns:
            None for batch operations, empty dict for single record operations
        """
        # Get entity information for better context
        entity_info = f" for entity '{entity_name or ''}'" if entity_name else ""

        # Log the error with appropriate context
        if isinstance(error, ParsingError):
            source_info = (
                f" in {kwargs.get('source', '')}" if kwargs.get("source") else ""
            )
            logger.log(
                self.log_level,
                f"Skipping record due to parsing error{source_info}"
                f"{entity_info}: {error}",
            )
        elif isinstance(error, ProcessingError):
            logger.log(
                self.log_level,
                f"Skipping record due to processing error{entity_info}: {error}",
            )
        elif isinstance(error, FileError):
            file_info = (
                f" with file '{kwargs.get('file_path', '')}'"
                if kwargs.get("file_path")
                else ""
            )
            logger.log(self.log_level, f"Skipping file{file_info}: {error}")
        else:
            # Log other errors
            logger.log(self.log_level, f"Error processing data{entity_info}: {error}")

        # Check if data is a list to determine appropriate return type
        data = kwargs.get("data", {})
        if isinstance(data, list):
            # For batch operations, return None to be consistent with tests
            return None
        else:
            # For single record operations, return empty dict
            return {}


class PartialProcessingRecovery(RecoveryStrategy):
    """Recovery strategy that attempts to recover partial results.

    This strategy is useful for complex processing operations where
    some parts may fail but partial results are still valuable.
    """

    def __init__(self, log_level: int = logging.WARNING):
        """Initialize with a log level.

        Args:
            log_level: Logging level for error messages
        """
        self.log_level = log_level

    def recover(
        self, error: Exception, entity_name: Optional[str] = None, **kwargs: Any
    ) -> Union[list[Any], dict[str, Any]]:
        """Recover partial data from errors when possible.

        This method attempts to extract any usable data from errors, particularly
        useful for partially valid JSON or nested structures where only
        part of the data is problematic.

        Args:
            error: The exception to recover from
            entity_name: Optional entity name for context
            **kwargs: Additional context for error handling

        Returns:
            List or dict with partial data and error information
        """
        # Get entity information for better context
        entity_info = f" for entity '{entity_name or ''}'" if entity_name else ""

        # Handle common error types
        if isinstance(error, (KeyError, IndexError, AttributeError)):
            logger.log(
                self.log_level,
                f"Attempting partial recovery from {type(error).__name__}{entity_info}:"
                f"{error}",
            )
            return {
                "_partial_error": str(error),
                "_error_type": type(error).__name__,
                "error": str(error),
            }

        elif isinstance(error, ParsingError):
            source_info = (
                f" in {kwargs.get('source', '')}" if kwargs.get("source") else ""
            )
            logger.log(
                self.log_level,
                f"Attempting partial recovery from parsing error{source_info}"
                f"{entity_info}: {error}",
            )

            # For parsing errors, return a minimal valid structure
            return {"_error": str(error), "error": str(error)}

        elif isinstance(error, ProcessingError):
            logger.log(
                self.log_level,
                f"Attempting partial recovery from processing error{entity_info}: "
                f"{error}",
            )

            # Get the data from the error if available
            data = getattr(error, "data", None)
            if data is None:
                return {"_error": str(error), "error": str(error)}

            # Return the data with error information
            if isinstance(data, dict):
                result = data.copy()
                result["_error"] = str(error)
                result["error"] = str(error)
                return result
            else:
                # Convert to string representation as fallback
                return {
                    "_error": str(error),
                    "_value": str(data),
                    "error": str(error),
                }

        elif isinstance(error, FileError):
            file_info = (
                f" with file '{kwargs.get('file_path', '')}'"
                if kwargs.get("file_path")
                else ""
            )
            logger.log(
                self.log_level,
                f"Attempting partial recovery from file error{file_info}"
                f"{entity_info}: {error}",
            )

            # For file errors, return an empty list
            return []

        else:
            # For other error types, return empty dict with error information
            logger.log(
                self.log_level, f"Unable to recover from error{entity_info}: {error}"
            )
            return {"_error": str(error), "error": str(error)}


# Predefined recovery strategies for common use cases
STRICT = StrictRecovery()
DEFAULT = SkipAndLogRecovery()
LENIENT = PartialProcessingRecovery()


def with_recovery(
    strategy: Optional[RecoveryStrategy] = None,
    fallback_value: Any = None,
    **options: Any,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator that applies recovery strategy to a function.

    Args:
        strategy: Recovery strategy to use (default: StrictRecovery)
        fallback_value: Value to return on error if no recovery strategy
        **options: Additional options passed to recovery handler

    Returns:
        Decorator function
    """
    # Use strict recovery by default
    actual_strategy = strategy or StrictRecovery()

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        """Decorate the function with error recovery."""

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            """Execute the function with error recovery."""
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Apply the recovery strategy
                try:
                    # Get entity name from kwargs if available
                    entity_name = kwargs.get("entity_name")

                    # Add the original function to kwargs for context
                    context = {**options, **kwargs, "_func": func.__name__}

                    # Try to recover using the strategy
                    result = actual_strategy.recover(
                        e, entity_name=entity_name, **context
                    )

                    # Return the recovered result
                    return cast(T, result)
                except Exception:
                    # If recovery fails, use fallback value or re-raise
                    if fallback_value is not None:
                        return cast(T, fallback_value)
                    raise

        return wrapper

    return decorator
