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

## Error Handling Configuration

Transmog provides flexible error handling configuration through the `ErrorHandlingConfig` class:

```python
import transmog as tm

# Create a configuration with error handling settings
config = (
    tm.TransmogConfig.default()
    .with_error_handling(
        allow_malformed_data=True,  # Allow malformed data
        recovery_strategy="skip",   # "strict", "skip", or "partial"
        max_retries=3,             # Maximum retry attempts
        error_log_path="errors.log" # Path for error logging
    )
)

# Use the configuration
processor = tm.Processor(config=config)
```

### Recovery Strategies

Transmog supports three built-in recovery strategies:

1. **Strict** (`"strict"`)
   - Raises all errors without recovery
   - Ensures data integrity
   - Default strategy

2. **Skip** (`"skip"`)
   - Skips problematic records
   - Continues with remaining data
   - Logs errors for review

3. **Partial** (`"partial"`)
   - Attempts to process parts of records
   - Skips problematic sections
   - Useful for complex nested data

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

To implement a custom recovery strategy, create a function that handles errors and returns a tuple of (success, value):

```python
def custom_recovery_strategy(error: Exception, context: dict) -> tuple[bool, Any]:
    """
    Custom recovery strategy that handles specific error types.
    
    Args:
        error: The exception that was raised
        context: Additional context information
        
    Returns:
        Tuple containing:
          - Boolean indicating if recovery was successful
          - Value to use as replacement if recovered
    """
    if isinstance(error, CircularReferenceError):
        # Replace circular references with placeholder
        return True, {"__circular_reference": True}
        
    elif isinstance(error, ValidationError):
        # Skip invalid values
        return True, None
        
    # Cannot recover from other types
    return False, None

# Use the custom strategy
config = (
    tm.TransmogConfig.default()
    .with_error_handling(
        recovery_strategy=custom_recovery_strategy,
        allow_malformed_data=True
    )
)

processor = tm.Processor(config=config)
```

## Using the `with_recovery` Decorator

For fine-grained control, you can use the `with_recovery` decorator on specific functions:

```python
from transmog.recovery import with_recovery

@with_recovery
def process_zip_codes(data):
    # This function will use the configured recovery strategy
    for record in data:
        # Processing that might fail
        pass
```

You can also specify a custom recovery strategy for specific functions:

```python
from transmog.recovery import with_recovery

def custom_recovery(error, context):
    print(f"Error at {context.get('path', 'unknown')}: {error}")
    return True, None

@with_recovery(recovery_strategy=custom_recovery)
def process_zip_codes(data):
    # This function will use custom_recovery
    pass
```

## Handling Specific Error Types

Different types of errors require different recovery strategies:

### Handling Circular References

```python
def handle_circular_references(error: Exception, context: dict) -> tuple[bool, Any]:
    if isinstance(error, CircularReferenceError):
        path = context.get("path", "")
        print(f"Circular reference detected at {path}")
        return True, {"__circular": True}
    return False, None

# Use the strategy
config = (
    tm.TransmogConfig.default()
    .with_error_handling(
        recovery_strategy=handle_circular_references,
        allow_malformed_data=True
    )
)

processor = tm.Processor(config=config)
```

### Handling Invalid Values

```python
def handle_invalid_values(error: Exception, context: dict) -> tuple[bool, Any]:
    if isinstance(error, ValidationError) and "invalid number" in str(error):
        print(f"Invalid number at {context.get('path', '')}")
        return True, None
    return False, None

# Use the strategy
config = (
    tm.TransmogConfig.default()
    .with_error_handling(
        recovery_strategy=handle_invalid_values,
        allow_malformed_data=True
    )
)

processor = tm.Processor(config=config)
```

## Best Practices

1. **Choose Appropriate Strategy**
   - Use "strict" for data integrity
   - Use "skip" for large datasets
   - Use "partial" for complex nested data

2. **Configure Logging**
   - Set `error_log_path` for persistent error tracking
   - Use appropriate log levels
   - Include context in error messages

3. **Handle Specific Errors**
   - Create custom strategies for known error patterns
   - Use context information for better recovery
   - Consider data type and structure

4. **Monitor Error Rates**
   - Track error frequency
   - Set appropriate retry limits
   - Adjust strategy based on error patterns

5. **Test Recovery Strategies**
   - Test with known problematic data
   - Verify recovery behavior
   - Check data integrity after recovery

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