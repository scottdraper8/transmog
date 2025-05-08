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
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Import from src package
from transmog import Processor
from transmog.error import (
    RecoveryStrategy,
    STRICT,
    ProcessingError,
    ValidationError,
    ParsingError,
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
    # Record with non-serializable object
    {
        "id": 2,
        "name": "Non-serializable Object",
        "details": {
            "status": "active",
            "object": None,
        },  # Will be replaced with a non-serializable object
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

    # Create non-serializable object in record #1
    data[1]["details"]["object"] = object()  # Non-serializable Python object

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
            "serialization": 0,
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
        if "is not JSON serializable" in str(error):
            self.error_counts["serialization"] += 1
            logger.warning(
                f"Non-serializable object detected at {path} in record {record_id}"
            )
            # Replace with a simple string reference indicator
            return True, {"__non_serializable": True}

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


# Create a global decorator recovery strategy instance for the example
decorator_recovery = CustomRecoveryStrategy()


def with_recovery_decorator(func: Callable) -> Callable:
    """Decorator to apply custom recovery to a function."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        """Wrapped function with error recovery."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Try to determine the record ID from args/kwargs
            record_id = None
            if len(args) > 1 and isinstance(args[1], str):
                record_id = args[1]
            elif "record_id" in kwargs:
                record_id = kwargs["record_id"]

            # Extract path if available
            path = getattr(e, "path", [])

            # Log the error
            logger.warning(f"Error processing record {record_id}: {str(e)}")

            # Try to recover based on error type
            if isinstance(e, ProcessingError):
                return decorator_recovery.handle_processing_error(e)
            else:
                # Wrap other exceptions in ProcessingError
                wrapped_error = ProcessingError(str(e))
                return decorator_recovery.handle_processing_error(
                    wrapped_error,
                    entity_name=f"record_{record_id}" if record_id else None,
                )

    return wrapper


@with_recovery_decorator
def process_single_record(data: Dict[str, Any], record_id: str) -> Dict[str, Any]:
    """
    Process a single record with error recovery.

    This function will be wrapped with our custom recovery decorator,
    which will catch and handle any errors that occur during processing.

    Args:
        data: The record to process
        record_id: Identifier for the record

    Returns:
        Processed record
    """
    # This could fail with various errors
    logger.info(f"Processing record {record_id}")

    # Validate record structure
    if not isinstance(data, dict):
        raise ValidationError("Record must be a dictionary")

    if "id" not in data:
        raise ValidationError("Record must have an 'id' field")

    # Process nested data
    if "details" in data and data["details"]:
        try:
            # Convert to JSON and back to trigger serialization errors
            json_data = json.dumps(data["details"])
            data["details"] = json.loads(json_data)
        except Exception as e:
            # Let the recovery strategy handle this error
            raise ProcessingError(f"Failed to serialize details: {str(e)}")

    # Add a processed marker
    data["__processed"] = True
    return data


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
