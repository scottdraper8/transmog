"""
Example demonstrating advanced error recovery strategies in Transmog.

This example shows how to implement and use custom error recovery strategies
when processing problematic JSON data that might have inconsistencies or errors.
"""

import json
import os
import sys
import logging
import traceback
import functools
from typing import Any, Dict, List, Optional, Tuple, Callable

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from transmog import Processor
from transmog.recovery import RecoveryStrategy, STRICT
from transmog.exceptions import (
    ProcessingError,
    ValidationError,
    ParsingError,
    CircularReferenceError,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("error_recovery_example")


# Sample data with various issues
PROBLEMATIC_DATA = [
    # Valid record
    {
        "id": 1,
        "name": "Good Record",
        "details": {"status": "active", "score": 95},
    },
    # Record with circular reference
    {
        "id": 2,
        "name": "Circular Reference",
        "details": {},  # Will be set to self-reference
    },
    # Record with invalid nested values
    {
        "id": 3,
        "name": "Invalid Nested Value",
        "details": {"status": float("nan"), "tags": ["good", float("inf")]},
    },
    # Record with extremely deep nesting
    {
        "id": 4,
        "name": "Too Deep",
        "details": {},  # Will be nested 500 levels deep
    },
    # Valid record
    {
        "id": 5,
        "name": "Another Good Record",
        "details": {"status": "inactive", "score": 75},
    },
]


def prepare_problematic_data() -> List[Dict[str, Any]]:
    """Prepare problematic data with specific issues."""
    data = PROBLEMATIC_DATA.copy()

    # Create circular reference in record #1
    data[1]["details"]["self"] = data[1]

    # Create extremely deep nesting in record #3
    deep_obj = {}
    current = deep_obj
    for i in range(500):
        current["level"] = i
        current["next"] = {}
        current = current["next"]
    data[3]["details"] = deep_obj

    return data


class CustomRecoveryStrategy(RecoveryStrategy):
    """
    Custom recovery strategy with specialized handling for different error types.

    This strategy:
    1. Logs all errors with different severity based on type
    2. Provides replacement values for specific error cases
    3. Tracks statistics about encountered errors
    """

    def __init__(self):
        """Initialize the recovery strategy with counters."""
        self.error_counts = {
            "circular_reference": 0,
            "validation": 0,
            "parsing": 0,
            "max_depth": 0,
            "other": 0,
            "total": 0,
        }

    def recover(
        self, error: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Any]:
        """
        Attempt to recover from the error.

        Args:
            error: The exception that was raised
            context: Additional context about the error

        Returns:
            Tuple of (recovered, replacement_value)
            - recovered: Whether recovery was successful
            - replacement_value: Value to use if recovered
        """
        self.error_counts["total"] += 1

        # Extract context information
        path = context.get("path", "") if context else ""
        record_id = context.get("record_id") if context else "unknown"

        # Different handling based on error type
        if isinstance(error, CircularReferenceError):
            self.error_counts["circular_reference"] += 1
            logger.warning(
                f"Circular reference detected at {path} in record {record_id}"
            )
            # Replace with a simple string reference indicator
            return True, {"__circular_reference": True}

        elif isinstance(error, ValidationError):
            self.error_counts["validation"] += 1
            logger.info(
                f"Validation error: {str(error)} at {path} in record {record_id}"
            )

            # For validation errors on numeric fields, provide default values
            if "nan" in str(error) or "inf" in str(error):
                return True, None  # Replace with null

            return True, "[invalid value]"  # Generic replacement

        elif isinstance(error, ParsingError):
            self.error_counts["parsing"] += 1
            logger.error(f"Parsing error: {str(error)} at {path} in record {record_id}")
            return False, None  # Cannot recover, skip entire record

        elif "maximum recursion depth exceeded" in str(error) or "max_depth" in str(
            error
        ):
            self.error_counts["max_depth"] += 1
            logger.warning(f"Max depth exceeded at {path} in record {record_id}")
            return True, {"__max_depth_exceeded": True}

        else:
            self.error_counts["other"] += 1
            logger.error(
                f"Unhandled error: {str(error)} at {path} in record {record_id}"
            )
            logger.debug(traceback.format_exc())
            return False, None  # Cannot recover, skip entire record

    def handle_processing_error(
        self, error: ProcessingError, entity_name: Optional[str] = None
    ) -> Any:
        """Handle processing errors with our custom recovery."""
        logger.warning(f"Handling processing error: {str(error)}")
        recovered, value = self.recover(error, {"entity_name": entity_name})
        if recovered:
            return value
        raise error

    def handle_circular_reference(
        self, error: CircularReferenceError, path: List[str]
    ) -> Any:
        """Handle circular reference errors with our custom recovery."""
        logger.warning(
            f"Handling circular reference at: {'.'.join(path) if path else 'unknown'}"
        )
        recovered, value = self.recover(error, {"path": ".".join(path) if path else ""})
        if recovered:
            return value
        raise error

    def get_report(self) -> Dict[str, int]:
        """Get a report of encountered errors."""
        return self.error_counts


def run_with_custom_recovery(data: List[Dict[str, Any]]) -> None:
    """
    Process data using a custom recovery strategy.

    Args:
        data: The data to process
    """
    # Create recovery strategy
    recovery_strategy = CustomRecoveryStrategy()

    # Create processor with custom recovery
    processor = Processor(
        cast_to_string=True,
        include_empty=False,
        recovery_strategy=recovery_strategy,
        allow_malformed_data=True,  # Allow and attempt to recover from malformed data
        max_nesting_depth=50,  # Set a reasonable max depth
    )

    try:
        # Process data with recovery
        result = processor.process(data=data, entity_name="problematic_records")

        # Report success
        logger.info(f"Successfully processed {len(result.get_main_table())} records")

        # Report errors
        error_report = recovery_strategy.get_report()
        logger.info(f"Error report: {error_report}")

        # Create output directory
        output_dir = os.path.join(os.path.dirname(__file__), "output")
        os.makedirs(output_dir, exist_ok=True)

        # Write results
        output_path = os.path.join(output_dir, "recovered_output.json")
        with open(output_path, "w") as f:
            json.dump(result.get_main_table(), f, indent=2)

        logger.info(f"Results written to {output_path}")

    except ProcessingError as e:
        logger.error(f"Critical processing error: {str(e)}")


# Create a custom recovery strategy for the decorator example
decorator_recovery = CustomRecoveryStrategy()


# Define our own with_recovery decorator function for the example
def with_recovery_decorator(func: Callable) -> Callable:
    """Custom decorator that wraps a function with error recovery."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Error in {func.__name__}: {str(e)}")

            # Use our recovery strategy
            if isinstance(e, ProcessingError):
                return decorator_recovery.handle_processing_error(e)
            elif isinstance(e, CircularReferenceError):
                path = getattr(e, "path", [])
                return decorator_recovery.handle_circular_reference(e, path)
            else:
                # Wrap other exceptions in ProcessingError
                wrapped = ProcessingError(f"Error processing data: {str(e)}")
                return decorator_recovery.handle_processing_error(wrapped)

    return wrapper


@with_recovery_decorator
def process_single_record(data: Dict[str, Any], record_id: str) -> Dict[str, Any]:
    """
    Process a single record with error recovery decorator.

    This demonstrates using a custom recovery decorator for fine-grained
    control at the function level.

    Args:
        data: The record to process
        record_id: Identifier for the record

    Returns:
        Processed record
    """
    # Simulate processing that might fail
    processed = {}

    for key, value in data.items():
        if isinstance(value, dict):
            try:
                # This could fail with circular references or max depth
                processed[key] = process_single_record(value, f"{record_id}.{key}")
            except Exception as e:
                # The @with_recovery decorator will handle this
                raise ProcessingError(f"Failed to process {key}: {str(e)}")
        else:
            processed[key] = str(value) if value is not None else None

    return processed


def run_with_decorator_recovery(data: List[Dict[str, Any]]) -> None:
    """
    Process data using the @with_recovery decorator.

    Args:
        data: The data to process
    """
    results = []

    for record in data:
        try:
            processed = process_single_record(
                record, f"record_{record.get('id', 'unknown')}"
            )
            results.append(processed)
        except ProcessingError as e:
            logger.error(f"Failed to process record: {str(e)}")

    logger.info(f"Processed {len(results)} records with decorator-based recovery")

    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    # Write results
    output_path = os.path.join(output_dir, "decorator_recovered_output.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Results written to {output_path}")

    # Report errors
    error_report = decorator_recovery.get_report()
    logger.info(f"Decorator error report: {error_report}")


def main():
    """Run the example."""
    logger.info("Preparing problematic data...")
    data = prepare_problematic_data()

    logger.info("\n\n=== Running with Custom Recovery Strategy ===\n")
    run_with_custom_recovery(data)

    logger.info("\n\n=== Running with Decorator-based Recovery ===\n")
    run_with_decorator_recovery(data)


if __name__ == "__main__":
    main()
