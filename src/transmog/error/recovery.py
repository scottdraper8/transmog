"""
Recovery strategies for error handling.

Provides different strategies for recovering from errors during processing.
"""

import logging
import os
import functools
import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Union, Type
from abc import ABC, abstractmethod

from .exceptions import (
    TransmogError,
    ProcessingError,
    ParsingError,
    FileError,
    ConfigurationError,
    ValidationError,
    CircularReferenceError,
)

from ..config.settings import settings

# Set up logger
logger = logging.getLogger("transmog")

# Type variable for generic return types
T = TypeVar("T")


class RecoveryStrategy(ABC):
    """Base class for recovery strategies."""

    @abstractmethod
    def recover(
        self, error: Exception, entity_name: Optional[str] = None, **kwargs: Any
    ) -> Any:
        """
        Recover from an error.

        Args:
            error: The error to recover from
            entity_name: Optional entity name for context
            **kwargs: Additional parameters for recovery

        Returns:
            The recovered value or result

        Raises:
            Exception: If recovery is not possible
        """
        pass

    def recover_or_raise(
        self,
        func: Callable[..., T],
        *args: Any,
        exception_type: Optional[Type[Exception]] = None,
        entity_name: Optional[str] = None,
        **kwargs: Any,
    ) -> T:
        """
        Execute a function with recovery or re-raise a specified error.

        This method executes the given function and, if an error occurs,
        passes it to the recovery strategy appropriate for the error type.
        If the exception_type is specified and the error doesn't match that type,
        the error is re-raised without attempting recovery.

        Args:
            func: Function to execute
            *args: Arguments to pass to the function
            exception_type: Optional specific exception type to handle
            entity_name: Name of the entity being processed
            **kwargs: Additional keyword arguments for the function

        Returns:
            Result from the function or recovery mechanism

        Raises:
            Exception: If the error cannot be recovered from
        """
        # No positional args - just call the function directly
        if not args:
            try:
                return func(**kwargs)
            except Exception as e:
                # Re-raise without wrapping if it's not the target exception type
                if exception_type and not isinstance(e, exception_type):
                    raise

                # For StrictRecovery, re-raise the original exception
                if (
                    exception_type
                    and isinstance(e, exception_type)
                    and isinstance(self, StrictRecovery)
                ):
                    raise

                # Otherwise, wrap in ProcessingError for better context
                if not isinstance(e, ProcessingError):
                    error = ProcessingError(f"Error processing data: {e}")
                else:
                    error = e

                return self.recover(error, entity_name=entity_name, **kwargs)

        # Process positional args with flexible handling
        # Interpret args based on common calling patterns
        data_args = []
        if len(args) >= 1:
            data_args = [args[0]]  # First arg is data

            if len(args) > 1:
                exception_type = args[1]  # Second arg is exception_type

            if len(args) > 2:
                entity_name = args[2]  # Third arg is entity_name

            # Any remaining args stay in data_args
            if len(args) > 3:
                data_args.extend(args[3:])

        try:
            return func(*data_args, **kwargs)
        except Exception as e:
            # Re-raise without wrapping if it's not the target exception type
            if exception_type and not isinstance(e, exception_type):
                raise

            # For StrictRecovery, re-raise the original exception
            if (
                exception_type
                and isinstance(e, exception_type)
                and isinstance(self, StrictRecovery)
            ):
                raise

            # Otherwise, wrap in ProcessingError for better context
            if not isinstance(e, ProcessingError):
                error = ProcessingError(f"Error processing data: {e}")
            else:
                error = e

            return self.recover(error, entity_name=entity_name, **kwargs)


class StrictRecovery(RecoveryStrategy):
    """
    Recovery strategy that re-raises all errors.

    This strategy provides no recovery, instead ensuring that errors
    are immediately propagated. Use this strategy when:

    - Data integrity is critical
    - You want to fail fast on any error
    - Errors need immediate attention

    Example:
        ```python
        # Create a processor with strict error handling
        strategy = StrictRecovery()
        processor = Processor(recovery_strategy=strategy)

        try:
            result = processor.process(data)
        except TransmogError as e:
            print(f"Processing failed: {e}")
        ```
    """

    def handle_parsing_error(
        self, error: ParsingError, source: Optional[str] = None
    ) -> Any:
        """Re-raise parsing errors."""
        raise error

    def handle_processing_error(
        self, error: ProcessingError, entity_name: Optional[str] = None
    ) -> Any:
        """Re-raise processing errors."""
        raise error

    def handle_circular_reference(
        self, error: CircularReferenceError, path: List[str]
    ) -> Any:
        """Re-raise circular reference errors."""
        raise error

    def handle_file_error(
        self, error: FileError, file_path: Optional[str] = None
    ) -> Any:
        """Re-raise file errors."""
        raise error

    def recover(self, error: Exception, **kwargs: Any) -> Any:
        """
        Generic method to recover from any exception based on its type.

        This method maps different exception types to their specific handlers.

        Args:
            error: The exception to recover from
            **kwargs: Additional context for error handling

        Returns:
            Recovery result based on the error type

        Raises:
            Exception: If the error type is not handled by this strategy
        """
        if isinstance(error, ParsingError):
            return self.handle_parsing_error(error, kwargs.get("source"))
        elif isinstance(error, ProcessingError):
            return self.handle_processing_error(error, kwargs.get("entity_name"))
        elif isinstance(error, CircularReferenceError):
            return self.handle_circular_reference(error, kwargs.get("path", []))
        elif isinstance(error, FileError):
            return self.handle_file_error(error, kwargs.get("file_path"))
        elif isinstance(error, Exception):
            # For other exceptions, wrap as a processing error
            wrapped_error = ProcessingError(str(error))
            return self.handle_processing_error(
                wrapped_error, kwargs.get("entity_name")
            )


class SkipAndLogRecovery(RecoveryStrategy):
    """
    Recovery strategy that logs errors and continues processing.

    This strategy is useful for batch processing where a few records
    failing shouldn't stop the entire process.
    """

    def __init__(self, log_level: int = logging.WARNING):
        """
        Initialize with the specified log level.

        Args:
            log_level: Logging level to use for error messages
        """
        self.log_level = log_level

    def handle_parsing_error(
        self, error: ParsingError, source: Optional[str] = None
    ) -> None:
        """
        Log parsing errors and return None.

        Args:
            error: The parsing error
            source: Optional source identifier

        Returns:
            None to indicate skipping this record
        """
        if source:
            logger.warning(f"Skipping parsing of {source}: {error}")
        else:
            logger.warning(f"Skipping record due to parsing error: {error}")
        return None

    def handle_processing_error(
        self, error: ProcessingError, entity_name: Optional[str] = None
    ) -> None:
        """
        Log processing errors and return None.

        Args:
            error: The processing error
            entity_name: Optional entity name

        Returns:
            None to indicate skipping this record
        """
        context = f" for {entity_name}" if entity_name else ""
        logger.warning(f"Skipping record due to error{context}: {error}")
        return None

    def handle_circular_reference(
        self, error: CircularReferenceError, path: List[str]
    ) -> None:
        """
        Log circular reference errors and return None.

        Args:
            error: The circular reference error
            path: Path where the circular reference was found

        Returns:
            None to indicate skipping this record
        """
        path_str = " -> ".join(path) if path else "unknown"
        logger.warning(f"Skipping circular reference at {path_str}: {error}")
        return None

    def handle_file_error(
        self, error: FileError, file_path: Optional[str] = None
    ) -> None:
        """
        Log file errors and return None.

        Args:
            error: The file error
            file_path: Optional path to the file

        Returns:
            None to indicate skipping this file
        """
        if file_path:
            logger.warning(f"Skipping file {file_path}: {error}")
        else:
            logger.warning(f"Skipping file: {error}")
        return None

    def recover(self, error: Exception, **kwargs: Any) -> Any:
        """
        Recover from any exception by logging and returning appropriate empty values.

        For list input data, return an empty list. Otherwise, return an empty dict.

        Args:
            error: The exception to recover from
            **kwargs: Additional context for error handling

        Returns:
            Empty list if input was a list, otherwise empty dict
        """
        # Check if data is a list to determine appropriate return type
        data = kwargs.get("data", [])
        if isinstance(data, list):
            # For batch operations, return empty list
            if isinstance(error, ParsingError):
                source_info = (
                    f" in {kwargs.get('source', '')}" if kwargs.get("source") else ""
                )
                logger.log(
                    self.log_level,
                    f"Skipping record due to parsing error{source_info}: {error}",
                )
            elif isinstance(error, ProcessingError):
                entity_info = (
                    f" for entity '{kwargs.get('entity_name', '')}'"
                    if kwargs.get("entity_name")
                    else ""
                )
                logger.log(
                    self.log_level,
                    f"Skipping record due to processing error{entity_info}: {error}",
                )
            elif isinstance(error, CircularReferenceError):
                path = kwargs.get("path", [])
                path_str = " > ".join(path) if path else "unknown"
                logger.log(
                    self.log_level,
                    f"Circular reference detected at path {path_str}: {error}",
                )
            elif isinstance(error, FileError):
                file_info = (
                    f" with file '{kwargs.get('file_path', '')}'"
                    if kwargs.get("file_path")
                    else ""
                )
                logger.log(self.log_level, f"File operation failed{file_info}: {error}")
            else:
                logger.log(self.log_level, f"Skipping record due to error: {error}")
            return []
        else:
            # Forward to the appropriate handler based on error type
            return super().recover(error, **kwargs)


class PartialProcessingRecovery(RecoveryStrategy):
    """
    Recovery strategy that attempts to recover partial results.

    This strategy is useful for complex processing operations where
    some parts may fail but partial results are still valuable.
    """

    def __init__(self, log_level: int = logging.WARNING):
        """
        Initialize with a log level.

        Args:
            log_level: Logging level for error messages
        """
        self.log_level = log_level

    def handle_parsing_error(
        self, error: ParsingError, source: Optional[str] = None
    ) -> Any:
        """
        Handle parsing errors by attempting to extract partial valid JSON.

        If the input is completely invalid, returns an empty dict. Otherwise,
        tries to extract any valid parts from the malformed JSON.

        Args:
            error: The parsing error
            source: Optional source identifier

        Returns:
            Partially recovered data or empty dict
        """
        source_info = f" in {source}" if source else ""
        logger.log(
            self.log_level,
            f"Attempting partial recovery from parsing error{source_info}: {error}",
        )

        # Get the raw data from the error if available
        raw_data = getattr(error, "raw_data", None)
        if not raw_data:
            return {}

        # Try to extract any valid JSON objects or arrays
        try:
            # For simplicity, just attempt to parse the content up to the error point
            if hasattr(error, "line") and hasattr(error, "pos"):
                # If we have line and position information, try to parse up to that point
                valid_portion = "\n".join(raw_data.split("\n")[: error.line])
                if valid_portion:
                    try:
                        return json.loads(valid_portion)
                    except json.JSONDecodeError:
                        pass  # Fall through to returning empty dict
            return {}
        except Exception as e:
            logger.debug(f"Failed to partially recover JSON: {e}")
            return {}

    def handle_processing_error(
        self, error: ProcessingError, entity_name: Optional[str] = None
    ) -> Any:
        """
        Handle processing errors by extracting any available data.

        Attempts to return the data that was being processed, even if
        it's incomplete or partially malformed.

        Args:
            error: The processing error
            entity_name: Optional entity name

        Returns:
            Partially processed data or empty dict
        """
        entity_info = f" for entity '{entity_name}'" if entity_name else ""
        logger.log(
            self.log_level,
            f"Attempting partial recovery from processing error{entity_info}: {error}",
        )

        # Get the data from the error if available
        data = getattr(error, "data", None)
        if data is None:
            return {"_partial_error": str(error), "error": str(error)}

        # Return the data as is, even if it's incomplete
        if isinstance(data, dict):
            # If we have a dict, add an error marker field
            result = data.copy()
            result["_partial_error"] = str(error)
            result["error"] = str(error)
            return result
        elif isinstance(data, (list, tuple)):
            # If we have a list/tuple, return it as is with metadata in first item if possible
            result = list(data)
            if result and isinstance(result[0], dict):
                result[0]["_partial_error"] = str(error)
                result[0]["error"] = str(error)
            return result
        else:
            # Convert to string representation as fallback
            return {
                "_partial_error": str(error),
                "_value": str(data),
                "error": str(error),
            }

    def handle_circular_reference(
        self, error: CircularReferenceError, path: List[str]
    ) -> Any:
        """
        Handle circular reference by truncating at the reference point.

        Returns an object with the path where the circular reference was
        detected, but without the circular part.

        Args:
            error: The circular reference error
            path: Path where circular reference was detected

        Returns:
            Dict with metadata about the circular reference
        """
        path_str = " > ".join(path) if path else "unknown"
        logger.log(
            self.log_level,
            f"Truncating circular reference at path {path_str}: {error}",
        )

        # Return a marker indicating where the circular reference was
        return {
            "_circular_reference": True,
            "_path": path_str,
            "_error": str(error),
        }

    def handle_file_error(
        self, error: FileError, file_path: Optional[str] = None
    ) -> Any:
        """
        Handle file errors by attempting to read any available data.

        If the file exists but has formatting issues, tries to read as
        much as possible. For other errors (missing file, permissions),
        returns empty list.

        Args:
            error: The file error
            file_path: Path to the file that caused the error

        Returns:
            Partially read data or empty list
        """
        file_info = f" with file '{file_path}'" if file_path else ""
        logger.log(
            self.log_level,
            f"Attempting partial recovery from file error{file_info}: {error}",
        )

        # If no file path, we can't do recovery
        if not file_path or not os.path.exists(file_path):
            return []

        # Check if the error is about file format/content
        if "format" in str(error).lower() or "content" in str(error).lower():
            try:
                # Try to read the file as text
                with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()

                # Return the raw content as a list of lines
                return [
                    {"_line": i + 1, "_content": line}
                    for i, line in enumerate(content.splitlines())
                ]
            except Exception as e:
                logger.debug(f"Failed to recover file content: {e}")

        # For other file errors, return empty list
        return []

    def recover(self, error: Exception, **kwargs: Any) -> Any:
        """
        Generic method to recover from any exception based on its type.

        This method maps different exception types to their specific handlers.

        Args:
            error: The exception to recover from
            **kwargs: Additional context for error handling

        Returns:
            Recovery result based on the error type

        Raises:
            Exception: If the error type is not handled by this strategy
        """
        # For KeyError, IndexError, AttributeError, etc., return partial results with error info
        if isinstance(error, (KeyError, IndexError, AttributeError)):
            logger.warning(
                f"Attempting partial recovery from {type(error).__name__}: {error}"
            )
            return {
                "_partial_error": str(error),
                "_error_type": type(error).__name__,
            }

        # For other types, use the standard recovery method
        if isinstance(error, ParsingError):
            return self.handle_parsing_error(error, kwargs.get("source"))
        elif isinstance(error, ProcessingError):
            return self.handle_processing_error(error, kwargs.get("entity_name"))
        elif isinstance(error, CircularReferenceError):
            return self.handle_circular_reference(error, kwargs.get("path", []))
        elif isinstance(error, FileError):
            return self.handle_file_error(error, kwargs.get("file_path"))
        elif isinstance(error, Exception):
            # For other exceptions, wrap as a processing error
            wrapped_error = ProcessingError(str(error))
            return self.handle_processing_error(
                wrapped_error, kwargs.get("entity_name")
            )


# Predefined recovery strategies for common use cases
STRICT = StrictRecovery()
DEFAULT = SkipAndLogRecovery()
LENIENT = PartialProcessingRecovery()


def with_recovery(
    strategy: Optional[RecoveryStrategy] = None,
    fallback_value: Any = None,
    **options: Any,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to apply recovery strategy to a function.

    Args:
        strategy: Recovery strategy to use
        fallback_value: Value to return if recovery fails
        **options: Additional options for recovery

    Returns:
        Decorator function
    """
    # Use skip and log strategy by default
    if strategy is None:
        strategy = SkipAndLogRecovery()

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Log the error
                logger.warning(
                    f"Skipping record due to processing error: {e}",
                    exc_info=settings.get_option("debug_mode"),
                )

                # Try to recover
                try:
                    result = strategy.recover(e, **options)
                    if result is not None:
                        return result
                except Exception as recovery_error:
                    # Recovery failed, log this too
                    logger.error(
                        f"Recovery failed for {func.__name__}: {recovery_error}",
                        exc_info=settings.get_option("debug_mode"),
                    )

                # Return fallback value if recovery fails
                return fallback_value

        return wrapper

    return decorator
