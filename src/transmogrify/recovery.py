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
    from transmogrify.recovery import SkipAndLogRecovery, with_recovery

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
    TransmogrifyError,
    ValidationError,
)
from .core.error_handling import logger

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
        except TransmogrifyError as e:
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

        # Check how many records were processed vs. expected
        print(f"Processed {len(result.get_main_table())} of {len(data)} records")
        ```
    """

    def __init__(self, log_level: int = logging.WARNING):
        """
        Initialize with configurable log level.

        Args:
            log_level: Logging level for errors (default: WARNING)
        """
        self.log_level = log_level

    def handle_parsing_error(
        self, error: ParsingError, source: Optional[str] = None
    ) -> Any:
        """
        Log parsing errors and return None.

        This allows the process to continue with other records.
        """
        source_info = f" from {source}" if source else ""
        logger.log(
            self.log_level, f"Skipping due to parsing error{source_info}: {error}"
        )
        return None

    def handle_processing_error(
        self, error: ProcessingError, entity_name: Optional[str] = None
    ) -> Any:
        """
        Log processing errors and return empty dict.

        This allows the process to continue with other records.
        """
        entity_info = f" for {entity_name}" if entity_name else ""
        logger.log(
            self.log_level,
            f"Skipping record{entity_info} due to processing error: {error}",
        )
        return {}

    def handle_circular_reference(
        self, error: CircularReferenceError, path: List[str]
    ) -> Any:
        """
        Log circular reference errors and return empty dict.

        This allows the process to continue with other records.
        """
        path_str = " > ".join(path)
        logger.log(
            self.log_level, f"Skipping circular reference at path '{path_str}': {error}"
        )
        return {}

    def handle_file_error(
        self, error: FileError, file_path: Optional[str] = None
    ) -> Any:
        """
        Log file errors and return None.

        This allows the process to continue with other files.
        """
        file_info = f" '{file_path}'" if file_path else ""
        logger.log(self.log_level, f"Skipping file{file_info} due to error: {error}")
        return None


class PartialProcessingRecovery(RecoveryStrategy):
    """
    Recovery strategy that attempts to extract partial data from errors.

    This strategy is useful when you want to extract as much data as possible,
    even if parts of the structure are invalid or malformed. Use this strategy when:

    - Some data is better than no data in your use case
    - The dataset contains a mix of well-formed and poorly-formed sections
    - You're working with data from untrusted or varied sources

    Example:
        ```python
        # Create a processor with partial recovery
        strategy = PartialProcessingRecovery(log_level=logging.INFO)
        processor = Processor(recovery_strategy=strategy)

        # Process data, attempting to extract partial data even from malformed records
        result = processor.process(data)

        # Even partial data will be included in the result
        print(f"Extracted {len(result.get_main_table())} records with possible partial data")
        ```
    """

    def __init__(self, log_level: int = logging.WARNING):
        """
        Initialize with configurable log level.

        Args:
            log_level: Logging level for errors (default: WARNING)
        """
        self.log_level = log_level

    def handle_parsing_error(
        self, error: ParsingError, source: Optional[str] = None
    ) -> Any:
        """
        Try to extract partial data from malformed JSON.

        This uses a best-effort approach to recover what data can be salvaged.
        """
        logger.log(self.log_level, f"Attempting to recover partial data from: {error}")

        if error.source and os.path.exists(error.source):
            try:
                # Attempt to read and process lines individually
                partial_data = []
                with open(error.source, "r") as f:
                    for i, line in enumerate(f):
                        try:
                            if line.strip():
                                partial_data.append(json.loads(line))
                        except json.JSONDecodeError:
                            logger.debug(f"Skipping invalid JSON at line {i + 1}")

                if partial_data:
                    logger.info(
                        f"Recovered {len(partial_data)} records from malformed source"
                    )
                    return partial_data
            except Exception as e:
                logger.debug(f"Failed to recover partial data: {e}")

        return {}

    def handle_processing_error(
        self, error: ProcessingError, entity_name: Optional[str] = None
    ) -> Any:
        """
        Extract partial data from processing errors.

        Args:
            error: The processing error
            entity_name: Optional entity name

        Returns:
            Partial data or empty dict if recovery fails
        """
        logger.log(self.log_level, f"Attempting to extract partial data from: {error}")

        # If we have the original data, try to extract basic fields
        if hasattr(error, "data") and isinstance(error.data, dict):
            try:
                # Extract only top-level scalar fields
                partial_data = {
                    k: v
                    for k, v in error.data.items()
                    if not isinstance(v, (dict, list))
                }

                if partial_data:
                    logger.info(
                        f"Recovered {len(partial_data)} fields from record with "
                        f"processing error"
                    )
                    return partial_data
            except Exception as e:
                logger.debug(f"Failed to extract partial data: {e}")

        return {}

    def handle_circular_reference(
        self, error: CircularReferenceError, path: List[str]
    ) -> Any:
        """
        Break circular references by replacing with a placeholder.

        Args:
            error: The circular reference error
            path: Path where the circular reference was detected

        Returns:
            Dict with reference info
        """
        path_str = " > ".join(path)
        logger.log(self.log_level, f"Breaking circular reference at path '{path_str}'")

        # Return a placeholder indicating circular reference
        return {"__circular_reference": True, "__reference_path": path_str}

    def handle_file_error(
        self, error: FileError, file_path: Optional[str] = None
    ) -> Any:
        """
        Try to recover partial data from files with errors.

        Args:
            error: The file error
            file_path: Optional file path

        Returns:
            Partial data or None if recovery fails
        """
        if not file_path or not os.path.exists(file_path):
            return None

        logger.log(self.log_level, f"Attempting to recover data from file: {file_path}")

        try:
            # Check file extension to determine format
            if file_path.endswith(".json"):
                with open(file_path, "r") as f:
                    return json.load(f)
            elif file_path.endswith(".jsonl"):
                # Process line by line
                partial_data = []
                with open(file_path, "r") as f:
                    for i, line in enumerate(f):
                        try:
                            if line.strip():
                                partial_data.append(json.loads(line))
                        except json.JSONDecodeError:
                            logger.debug(f"Skipping invalid JSON at line {i + 1}")
                return partial_data
        except Exception as e:
            logger.debug(f"Failed to recover data from file: {e}")

        return None


# Create commonly used recovery instances for convenience
STRICT = StrictRecovery()
SKIP_AND_LOG = SkipAndLogRecovery()
PARTIAL = PartialProcessingRecovery()


def with_recovery(
    func: Callable[..., T],
    strategy: RecoveryStrategy = STRICT,
    *args: Any,
    **kwargs: Any,
) -> T:
    """
    Apply a recovery strategy to a function call.

    This utility function wraps a function call with error recovery, handling
    various types of errors according to the specified recovery strategy.

    The function can be used in two ways:
    1. As a decorator: @with_recovery(strategy=SKIP_AND_LOG)
    2. As a wrapper: with_recovery(process_data, strategy=SKIP_AND_LOG, data=input_data)

    Args:
        func: The function to call with recovery
        strategy: Recovery strategy to use (default: STRICT)
        *args: Positional arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function

    Returns:
        The function's return value or a recovery value based on the strategy

    Example:
        ```python
        # As a function wrapper
        try:
            result = with_recovery(
                process_data,
                strategy=SkipAndLogRecovery(),
                data=input_data,
                entity_name="customer"
            )
        except TransmogrifyError:
            # Only raised for errors the strategy didn't handle
            print("Critical error occurred")

        # As a decorator
        @with_recovery(strategy=PartialProcessingRecovery())
        def process_batch(records):
            # Processing that might fail
            return [transform(record) for record in records]
        ```
    """
    try:
        # Check if being used as a decorator (no args provided)
        if not args and not kwargs and not isinstance(func, type):
            # Return a decorator function
            def decorator(target_func):
                @functools.wraps(target_func)
                def wrapper(*inner_args, **inner_kwargs):
                    return with_recovery(
                        target_func, strategy, *inner_args, **inner_kwargs
                    )

                return wrapper

            return decorator

        # Otherwise, execute the function directly with recovery
        return func(*args, **kwargs)
    except ParsingError as e:
        # Get source from kwargs if available
        source = kwargs.get("source")
        if source is None and len(args) > 0 and isinstance(args[0], str):
            # If first arg is a string, it might be a source identifier
            source = args[0]
        return strategy.handle_parsing_error(e, source=source)
    except ProcessingError as e:
        # Get entity_name from kwargs if available
        entity_name = kwargs.get("entity_name")
        return strategy.handle_processing_error(e, entity_name=entity_name)
    except CircularReferenceError as e:
        # Get path from the error itself
        path = getattr(e, "path", [])
        return strategy.handle_circular_reference(e, path=path)
    except FileError as e:
        # Get file_path from kwargs or error
        file_path = kwargs.get("file_path")
        if file_path is None and hasattr(e, "file_path"):
            file_path = e.file_path
        return strategy.handle_file_error(e, file_path=file_path)
    except Exception as e:
        # For any other exception, wrap it in a ProcessingError and handle
        if not isinstance(e, TransmogrifyError):
            # Only wrap non-Transmogrify errors
            error = ProcessingError(f"Unexpected error: {str(e)}", cause=e)
        else:
            error = e
        entity_name = kwargs.get("entity_name")
        return strategy.handle_processing_error(error, entity_name=entity_name)
