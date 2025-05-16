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

# Module logger
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
            # Filter exceptions by type if specified
            if exception_type is not None and not isinstance(e, exception_type):
                raise

            # Use built-in recovery if available
            if hasattr(e, "recover") and callable(e.recover):
                return cast(T, e.recover())

            # Skip recovery for strict TransmogErrors
            if (
                isinstance(e, TransmogError)
                and e.recover_strategy == "strict"
                and isinstance(self, StrictRecovery)
            ):
                raise

            # Wrap non-ProcessingErrors for context
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
        # Format entity context for error messages
        entity_info = f" for entity '{entity_name or ''}'" if entity_name else ""

        # Handle different error types with appropriate logging
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
            logger.log(self.log_level, f"Error processing data{entity_info}: {error}")

        # Return type depends on input data structure
        data = kwargs.get("data", {})
        if isinstance(data, list):
            # None for batch operations
            return None
        else:
            # Empty dict for single record operations
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
        # Format entity context for error messages
        entity_info = f" for entity '{entity_name or ''}'" if entity_name else ""

        # Handle different error types with appropriate recovery
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

            # Minimal valid structure for parsing errors
            return {"_error": str(error), "error": str(error)}

        elif isinstance(error, ProcessingError):
            logger.log(
                self.log_level,
                f"Attempting partial recovery from processing error{entity_info}: "
                f"{error}",
            )

            # Extract data from error if available
            data = getattr(error, "data", None)
            if data is None:
                return {"_error": str(error), "error": str(error)}

            # Preserve existing data with error information
            if isinstance(data, dict):
                result = data.copy()
                result["_error"] = str(error)
                result["error"] = str(error)
                return result
            else:
                # String representation as fallback
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

            # Empty list for file errors
            return []

        else:
            # Generic error handling
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
    # Default to strict recovery
    actual_strategy = strategy or StrictRecovery()

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        """Decorate the function with error recovery."""

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            """Execute the function with error recovery."""
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Apply recovery strategy
                try:
                    entity_name = kwargs.get("entity_name")

                    # Add function name to context for better error reporting
                    context = {**options, **kwargs, "_func": func.__name__}

                    result = actual_strategy.recover(
                        e, entity_name=entity_name, **context
                    )

                    return cast(T, result)
                except Exception:
                    # Use fallback or re-raise
                    if fallback_value is not None:
                        return cast(T, fallback_value)
                    raise

        return wrapper

    return decorator
