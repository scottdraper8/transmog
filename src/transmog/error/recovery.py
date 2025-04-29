"""
Recovery strategies for handling errors during processing.

This module provides a framework for error recovery during data processing operations,
enabling robust handling of malformed or unexpected data. Recovery strategies define
how the system should respond to different types of errors, allowing for flexible
error handling approaches ranging from strict (fail fast) to lenient (best effort).

Key concepts:
- Recovery strategies define how to handle different error types
- Multiple predefined strategies are available for common use cases
- Custom strategies can be created by extending the base RecoveryStrategy class
- The with_recovery decorator applies strategies to any function

Usage example:
    ```python
    from transmog.error import SkipAndLogRecovery, with_recovery

    # Create a recovery strategy
    recovery = SkipAndLogRecovery(log_level=logging.WARNING)

    # Apply recovery to a function that might fail
    @with_recovery(strategy=recovery)
    def process_data(data):
        # Processing that might raise exceptions
        return transform_data(data)

    # Or apply recovery inline
    result = with_recovery(process_data, strategy=recovery, data=input_data)
    ```
"""

import logging
import json
import os
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Union
import functools

from .exceptions import (
    CircularReferenceError,
    FileError,
    ParsingError,
    ProcessingError,
    TransmogError,
    ValidationError,
)
from .handling import logger

T = TypeVar("T")


class RecoveryStrategy:
    """
    Base class for recovery strategies.

    Recovery strategies provide a consistent way to handle errors during processing
    and decide how to proceed when encountering malformed or invalid data. They
    implement methods for handling different types of errors, each returning an
    appropriate recovery value or re-raising the exception.

    To create a custom recovery strategy, extend this class and implement all
    the required methods to define your custom error handling behavior.

    Example:
        ```python
        class CustomRecovery(RecoveryStrategy):
            def handle_parsing_error(self, error, source=None):
                # Custom logic to handle parsing errors
                logger.error(f"Parsing error in {source}: {error}")
                return {"error": str(error)}  # Return fallback value
        ```
    """

    def handle_parsing_error(
        self, error: ParsingError, source: Optional[str] = None
    ) -> Any:
        """
        Handle JSON parsing errors.

        Args:
            error: The parsing error that occurred
            source: Optional source identifier (e.g., file name)

        Returns:
            Recovery value or re-raises the exception
        """
        raise NotImplementedError()

    def handle_processing_error(
        self, error: ProcessingError, entity_name: Optional[str] = None
    ) -> Any:
        """
        Handle data processing errors.

        Args:
            error: The processing error that occurred
            entity_name: Optional entity name being processed

        Returns:
            Recovery value or re-raises the exception
        """
        raise NotImplementedError()

    def handle_circular_reference(
        self, error: CircularReferenceError, path: List[str]
    ) -> Any:
        """
        Handle circular reference errors.

        Args:
            error: The circular reference error that occurred
            path: Path where the circular reference was detected

        Returns:
            Recovery value or re-raises the exception
        """
        raise NotImplementedError()

    def handle_file_error(
        self, error: FileError, file_path: Optional[str] = None
    ) -> Any:
        """
        Handle file-related errors.

        Args:
            error: The file error that occurred
            file_path: Optional file path that caused the error

        Returns:
            Recovery value or re-raises the exception
        """
        raise NotImplementedError()


class StrictRecovery(RecoveryStrategy):
    """
    Strict recovery strategy that re-raises all errors.

    This strategy is useful when you want to ensure all data is processed
    correctly and prefer to fail fast on any errors. Use this strategy when:

    - Data quality is critical and errors indicate significant problems
    - You want to address issues immediately rather than continuing with partial data
    - During development or testing when you want to catch all issues

    Example:
        ```python
        processor = Processor(recovery_strategy=StrictRecovery())
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


class SkipAndLogRecovery(RecoveryStrategy):
    """
    Recovery strategy that skips errors and logs them.

    This strategy is useful for batch processing when you want to continue
    even if some records fail, logging the errors for later analysis. Use this strategy when:

    - Processing large datasets where some errors are expected
    - You prefer to get partial results rather than no results
    - You want to log errors for later review without stopping processing

    Example:
        ```python
        # Create a processor with skip-and-log recovery
        strategy = SkipAndLogRecovery(log_level=logging.WARNING)
        processor = Processor(recovery_strategy=strategy)

        # Process data, continuing even when errors occur
        result = processor.process(data)
        ```
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
    ) -> Any:
        """Log parsing errors and return an empty dict."""
        source_info = f" in {source}" if source else ""
        logger.log(
            self.log_level,
            f"Skipping record due to parsing error{source_info}: {error}",
        )
        return {}  # Empty dict as fallback

    def handle_processing_error(
        self, error: ProcessingError, entity_name: Optional[str] = None
    ) -> Any:
        """Log processing errors and return an empty dict."""
        entity_info = f" for entity '{entity_name}'" if entity_name else ""
        logger.log(
            self.log_level,
            f"Skipping record due to processing error{entity_info}: {error}",
        )
        return {}  # Empty dict as fallback

    def handle_circular_reference(
        self, error: CircularReferenceError, path: List[str]
    ) -> Any:
        """Log circular reference errors and return an empty dict."""
        path_str = " > ".join(path) if path else "unknown"
        logger.log(
            self.log_level,
            f"Circular reference detected at path {path_str}: {error}",
        )
        return {}  # Empty dict as fallback

    def handle_file_error(
        self, error: FileError, file_path: Optional[str] = None
    ) -> Any:
        """Log file errors and return an empty list."""
        file_info = f" with file '{file_path}'" if file_path else ""
        logger.log(self.log_level, f"File operation failed{file_info}: {error}")
        return []  # Empty list as fallback


class PartialProcessingRecovery(RecoveryStrategy):
    """
    Recovery strategy that attempts to extract partial data when possible.

    This strategy is more sophisticated than SkipAndLogRecovery, as it tries to
    extract as much useful data as possible from a record, even if parts of it
    have errors. Use this strategy when:

    - You're dealing with complex nested structures where some parts may be invalid
    - You want to recover partial data rather than discarding entire records
    - You need a balance between data quality and completeness

    Example:
        ```python
        # Create a processor with partial processing recovery
        strategy = PartialProcessingRecovery()
        processor = Processor(recovery_strategy=strategy)

        # Process data, recovering partial records when possible
        result = processor.process(data)
        ```
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
            return {}

        # Return the data as is, even if it's incomplete
        if isinstance(data, dict):
            # If we have a dict, add an error marker field
            result = data.copy()
            result["_error"] = str(error)
            return result
        elif isinstance(data, (list, tuple)):
            # If we have a list/tuple, return it as is
            return list(data)
        else:
            # Convert to string representation as fallback
            return {"_error": str(error), "_value": str(data)}

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


# Predefined recovery strategies for common use cases
STRICT = StrictRecovery()
DEFAULT = SkipAndLogRecovery()
LENIENT = PartialProcessingRecovery()


def with_recovery(
    func: Optional[Callable[..., T]] = None,
    strategy: RecoveryStrategy = STRICT,
    *args: Any,
    **kwargs: Any,
) -> T:
    """
    Apply recovery strategy to a function call.

    This can be used as a decorator or as a function wrapper at the call site.

    Decorator usage:
        ```python
        @with_recovery(strategy=SkipAndLogRecovery())
        def process_data(data):
            # Process data with built-in error recovery
            pass
        ```

    Function wrapper usage:
        ```python
        # Apply recovery when calling the function
        result = with_recovery(process_data, strategy=LENIENT, data=input_data)
        ```

    Args:
        func: Function to decorate or call
        strategy: Recovery strategy to apply
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function

    Returns:
        Function result or recovery value if an exception occurred
    """
    # Used as a decorator with parameters
    if func is None:

        def decorator(target_func):
            @functools.wraps(target_func)
            def wrapper(*inner_args, **inner_kwargs):
                try:
                    return target_func(*inner_args, **inner_kwargs)
                except ParsingError as e:
                    source = inner_kwargs.get("source")
                    if source is None and inner_args:
                        # Try to get source from first positional arg if it seems to be a file path
                        first_arg = inner_args[0]
                        if isinstance(first_arg, str) and (
                            os.path.exists(first_arg)
                            or "." in os.path.basename(first_arg)
                        ):
                            source = first_arg
                    return strategy.handle_parsing_error(e, source)
                except ProcessingError as e:
                    entity_name = inner_kwargs.get("entity_name")
                    if entity_name is None and len(inner_args) > 1:
                        # Try to get entity_name from second positional arg if it's a string
                        if isinstance(inner_args[1], str):
                            entity_name = inner_args[1]
                    return strategy.handle_processing_error(e, entity_name)
                except CircularReferenceError as e:
                    path = getattr(e, "path", [])
                    return strategy.handle_circular_reference(e, path)
                except FileError as e:
                    file_path = getattr(e, "file_path", None)
                    if file_path is None and inner_args:
                        # Try to get file_path from first positional arg if it seems to be a file path
                        first_arg = inner_args[0]
                        if isinstance(first_arg, str) and (
                            os.path.exists(first_arg)
                            or "." in os.path.basename(first_arg)
                        ):
                            file_path = first_arg
                    return strategy.handle_file_error(e, file_path)
                except Exception as e:
                    # For other exceptions, treat as processing errors
                    logger.debug(
                        f"Unhandled exception in with_recovery: {type(e).__name__}: {e}"
                    )
                    wrapped_error = ProcessingError(str(e))
                    return strategy.handle_processing_error(wrapped_error)

            return wrapper

        return decorator

    # Used as a function wrapper
    try:
        return func(*args, **kwargs)
    except ParsingError as e:
        source = kwargs.get("source")
        if source is None and args:
            # Try to get source from first positional arg if it seems to be a file path
            first_arg = args[0]
            if isinstance(first_arg, str) and (
                os.path.exists(first_arg) or "." in os.path.basename(first_arg)
            ):
                source = first_arg
        return strategy.handle_parsing_error(e, source)
    except ProcessingError as e:
        entity_name = kwargs.get("entity_name")
        if entity_name is None and len(args) > 1:
            # Try to get entity_name from second positional arg if it's a string
            if isinstance(args[1], str):
                entity_name = args[1]
        return strategy.handle_processing_error(e, entity_name)
    except CircularReferenceError as e:
        path = getattr(e, "path", [])
        return strategy.handle_circular_reference(e, path)
    except FileError as e:
        file_path = getattr(e, "file_path", None)
        if file_path is None and args:
            # Try to get file_path from first positional arg if it seems to be a file path
            first_arg = args[0]
            if isinstance(first_arg, str) and (
                os.path.exists(first_arg) or "." in os.path.basename(first_arg)
            ):
                file_path = first_arg
        return strategy.handle_file_error(e, file_path)
    except Exception as e:
        # For other exceptions, treat as processing errors
        logger.debug(f"Unhandled exception in with_recovery: {type(e).__name__}: {e}")
        wrapped_error = ProcessingError(str(e))
        return strategy.handle_processing_error(wrapped_error)
