# Error Handling and Recovery

This guide explains the error handling system in Transmog and how to implement robust error recovery strategies in your applications.

## Overview

Transmog provides a comprehensive error handling system that:

1. Defines specific exception types for different error categories
2. Provides detailed error messages with context
3. Offers recovery strategies for common error scenarios
4. Includes utilities for configuring error handling behavior

## Exception Hierarchy

All Transmog exceptions inherit from the base `TransmogError` class:

```
TransmogError
├── ProcessingError      - Errors during data processing
├── ValidationError      - Input validation failures
├── ParsingError         - JSON parsing problems
├── FileError            - File operations issues
├── CircularReferenceError - Circular reference detection
├── MissingDependencyError - Missing optional dependencies
├── ConfigurationError   - Configuration problems
└── OutputError          - Errors writing output
```

## Basic Error Handling

Here's a simple example of handling Transmog errors:

```python
import transmog as tm
from transmog.exceptions import TransmogError, ProcessingError, ParsingError

try:
    processor = tm.Processor()
    result = processor.process(my_data, entity_name="customers")
except ParsingError as e:
    print(f"JSON parsing error: {e}")
    # Handle parsing error specifically
except ProcessingError as e:
    print(f"Processing error: {e}")
    # Handle processing error specifically
except TransmogError as e:
    print(f"Other Transmog error: {e}")
    # Handle other Transmog errors
except Exception as e:
    print(f"Unexpected error: {e}")
    # Handle unexpected errors
```

## Built-in Recovery Strategies

Transmog includes several built-in recovery strategies:

### `StrictRecovery`

The default strategy that raises all errors without recovery, ensuring data integrity.

```python
from transmog.recovery import StrictRecovery

processor = tm.Processor(
    recovery_strategy=StrictRecovery(),
    # ... other options
)
```

### `SkipAndLogRecovery`

Logs errors and skips problematic records, continuing with the remaining data.

```python
from transmog.recovery import SkipAndLogRecovery

processor = tm.Processor(
    recovery_strategy=SkipAndLogRecovery(log_level="WARNING"),
    allow_malformed_data=True,  # Required for recovery to work
    # ... other options
)
```

### `PartialProcessingRecovery`

Attempts to process parts of records while skipping problematic sections.

```python
from transmog.recovery import PartialProcessingRecovery

processor = tm.Processor(
    recovery_strategy=PartialProcessingRecovery(),
    allow_malformed_data=True,
    # ... other options
)
```

## The Recovery Context

When an error occurs, Transmog provides a context dictionary containing information about the error:

```python
context = {
    "path": "employees.0.address.zipCode",  # Path where error occurred
    "value": "12AB34",                      # Value that caused the error
    "record_id": "emp123",                  # ID of the record being processed
    "parent_path": "employees.0",           # Path to parent object
    "error_type": "validation",             # Type of error
    "stage": "flattening"                   # Processing stage
}
```

Not all fields are available for all errors, but this context can help determine the appropriate recovery action.

## Creating Custom Recovery Strategies

To implement a custom recovery strategy, subclass `RecoveryStrategy` and implement the `recover` method:

```python
from transmog.recovery import RecoveryStrategy
from typing import Any, Dict, Optional, Tuple

class CustomRecoveryStrategy(RecoveryStrategy):
    def __init__(self):
        # Initialize counters, loggers, or other state
        self.error_count = 0
    
    def recover(self, error: Exception, context: Optional[Dict[str, Any]] = None) -> Tuple[bool, Any]:
        """
        Attempt to recover from an error.
        
        Args:
            error: The exception that was raised
            context: Additional context information
            
        Returns:
            Tuple containing:
              - Boolean indicating if recovery was successful
              - Value to use as replacement if recovered
        """
        self.error_count += 1
        
        # Different handling based on error type
        if isinstance(error, CircularReferenceError):
            # Replace circular references with placeholder
            return True, {"__circular_reference": True}
            
        elif isinstance(error, ValidationError):
            # Skip invalid values
            return True, None
            
        # Cannot recover from other types
        return False, None
```

You can also create strategies for specific error types:

```python
class MyCustomRecovery(RecoveryStrategy):
    """Custom recovery strategy with fallback data."""
    
    def __init__(self, fallback_data=None):
        self.fallback_data = fallback_data or {}
    
    def handle_parsing_error(self, error, source=None):
        """Return fallback data on parsing errors."""
        print(f"Using fallback data due to parsing error: {error}")
        return self.fallback_data
    
    def handle_processing_error(self, error, entity_name=None):
        """Return fallback data on processing errors."""
        print(f"Using fallback data due to processing error: {error}")
        return self.fallback_data
```

## Using the `with_recovery` Decorator

For fine-grained control, you can use the `with_recovery` decorator on specific functions:

```python
from transmog.recovery import with_recovery

@with_recovery
def process_zip_codes(data):
    # This function will use the global recovery strategy
    # or can be configured with a specific one
    for record in data:
        # Processing that might fail
        pass
```

You can also specify a custom recovery strategy for specific functions:

```python
from transmog.recovery import with_recovery, SkipAndLogRecovery

# Create a specific recovery strategy for this function
zip_recovery = SkipAndLogRecovery(log_level="WARNING")

@with_recovery(recovery_strategy=zip_recovery)
def process_zip_codes(data):
    # This function will use zip_recovery
    # rather than the global strategy
    pass
```

The utility can also be used with arbitrary functions:

```python
# Process with recovery
result = with_recovery(
    processor.process,
    strategy=PARTIAL,
    data=potentially_problematic_data,
    entity_name="customers"
)
```

## Handling Specific Error Types

Different types of errors require different recovery strategies:

### Handling Circular References

```python
# In your custom recovery strategy:
if isinstance(error, CircularReferenceError):
    path = context.get("path", "")
    logger.warning(f"Circular reference detected at {path}")
    return True, {"__circular": True}

# Or with try/except:
try:
    result = processor.process(
        circular_data,
        entity_name="recursive_structure"
    )
except CircularReferenceError as e:
    print(f"Circular reference detected: {e}")
    if e.path:
        print(f"Path to circular reference: {' > '.join(e.path)}")
```

### Handling Invalid Values

```python
if isinstance(error, ValidationError) and "invalid number" in str(error):
    logger.info(f"Invalid number at {context.get('path', '')}")
    return True, None  # Replace with null
```

### Handling File Errors

```python
try:
    result = processor.process_file("data.json", entity_name="records")
except FileError as e:
    print(f"File error: {e}")
    if e.file_path:
        print(f"Problem file: {e.file_path}")
    if e.operation:
        print(f"During operation: {e.operation}")
```

## Configuring Logging

Transmog uses Python's standard logging module. You can configure it like this:

```python
import logging
from transmog.core.error_handling import setup_logging

# Configure logging with custom level and file
setup_logging(
    level=logging.DEBUG,
    log_file="transmog.log",
    log_format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
```

## Error Context Decorator

The `error_context` decorator adds context to exceptions:

```python
from transmog.core.error_handling import error_context

@error_context("Failed during customer import")
def import_customers(data):
    # Function implementation
    pass
```

## Best Practices

1. **Choose an appropriate recovery strategy** for your use case:
   - Use `StrictRecovery` for data pipelines where quality is critical
   - Use `SkipAndLogRecovery` for ETL processes where some data loss is acceptable
   - Use `PartialProcessingRecovery` for exploratory analysis where partial data is useful

2. **Log all errors** for later review and debugging

3. **Use context information** to make intelligent recovery decisions

4. **Gracefully degrade** rather than failing completely when possible

5. **Monitor error rates** to detect systemic issues

## Complete Example

Here's a complete example of a custom recovery strategy:

```python
import logging
import transmog as tm
from transmog.recovery import RecoveryStrategy
from transmog.exceptions import CircularReferenceError, ValidationError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("error_recovery_example")

class CustomRecoveryStrategy(RecoveryStrategy):
    def __init__(self):
        self.error_counts = {
            "circular": 0,
            "validation": 0,
            "other": 0
        }
    
    def recover(self, error, context=None):
        if isinstance(error, CircularReferenceError):
            self.error_counts["circular"] += 1
            path = context.get("path", "unknown") if context else "unknown"
            logger.warning(f"Circular reference at {path}")
            return True, {"__circular": True}
            
        if isinstance(error, ValidationError):
            self.error_counts["validation"] += 1
            path = context.get("path", "unknown") if context else "unknown"
            logger.info(f"Validation error at {path}")
            return True, None
        
        self.error_counts["other"] += 1
        logger.error(f"Unrecoverable error: {error}")
        return False, None
    
    def report(self):
        """Report error statistics"""
        total = sum(self.error_counts.values())
        if total > 0:
            logger.info(f"Recovery summary: {total} errors handled")
            for error_type, count in self.error_counts.items():
                logger.info(f"  - {error_type}: {count}")

# Usage
recovery = CustomRecoveryStrategy()
processor = tm.Processor(
    recovery_strategy=recovery,
    allow_malformed_data=True
)

# Process data
result = processor.process(complex_data)

# Report error statistics
recovery.report()
```

## Example: Customized Error Handling

For a complete working example, see the [error handling examples](../examples/basic.md#error-handling) in the examples directory. 