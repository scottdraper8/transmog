---
title: Error Handling
---

# Error Handling

Transmog provides error handling for managing errors during data processing. This guide covers strategies
for handling different types of errors.

## Error Types

Transmog defines several exception types for specific error conditions:

```text
TransmogError              - Base class for all Transmog errors
├── ProcessingError        - Errors during data processing
├── ValidationError        - Input validation failures
├── ParsingError           - JSON parsing problems
├── FileError              - File operations issues
├── MissingDependencyError - Missing optional dependencies
├── ConfigurationError     - Configuration problems
└── OutputError            - Errors writing output
```

## Basic Error Handling

Example of handling Transmog errors:

```python
import transmog as tm
from transmog.error import TransmogError, ProcessingError, ParsingError

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

Error handling configuration using the `ErrorHandlingConfig` class:

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

## Recovery Strategy Classes

Error recovery implemented through strategy classes:

```python
from transmog.error import (
    RecoveryStrategy,       # Abstract base class
    StrictRecovery,         # No recovery (raises errors)
    SkipAndLogRecovery,     # Skip and log problematic records
    PartialProcessingRecovery, # Extract valid portions of problematic records
)

# Using a recovery strategy directly
from transmog.error import with_recovery

@with_recovery(PartialProcessingRecovery())
def process_data(data):
    # Processing that might fail
    return processed_data
```

## Recovery Strategy Constants

Constants for common recovery strategies:

```python
from transmog import Processor
from transmog.error import STRICT, DEFAULT, LENIENT

# Create processors with different strategies
strict_processor = Processor().with_error_handling(recovery_strategy=STRICT)
default_processor = Processor().with_error_handling(recovery_strategy=DEFAULT)
lenient_processor = Processor().with_error_handling(recovery_strategy=LENIENT)
```

These constants map to:

- `STRICT`: StrictRecovery - raises all errors
- `DEFAULT`: SkipAndLogRecovery - skips problematic records
- `LENIENT`: PartialProcessingRecovery - extracts valid portions

## Factory Method for Partial Recovery

Factory method for partial recovery:

```python
# Create a processor with partial recovery configuration
processor = tm.Processor.with_partial_recovery()

# This is equivalent to:
processor = tm.Processor(
    config=tm.TransmogConfig.default()
    .with_error_handling(
        recovery_strategy=LENIENT,
        allow_malformed_data=True
    )
    .with_processing(
        cast_to_string=True  # Enable string casting to handle numeric type issues
    )
)
```

## Recovery Strategies in Depth

Transmog provides three built-in recovery strategies:

### 1. Strict Recovery (`StrictRecovery`)

The strict recovery strategy re-raises all errors without attempting recovery. This ensures data integrity
but fails processing if issues are encountered.

**When to use:**

- When data integrity is critical and errors indicate problems
- During development and testing to catch issues
- For applications where data quality must be guaranteed
- When processing sensitive information

**Configuration:**

```python
# Using string configuration
config = tm.TransmogConfig.default().with_error_handling(recovery_strategy="strict")

# Or using the strategy class
from transmog.error import StrictRecovery
config = tm.TransmogConfig.default().with_error_handling(recovery_strategy=StrictRecovery())

# Or using the direct strategy constant
from transmog.error import STRICT
config = tm.TransmogConfig.default().with_error_handling(recovery_strategy=STRICT)
```

### 2. Skip and Log Recovery (`SkipAndLogRecovery`)

This strategy skips problematic records and logs the errors for later analysis. Processing continues
with the remaining records.

**When to use:**

- For batch processing of large datasets where errors are expected
- When partial results are acceptable
- For data exploration tasks where complete coverage isn't required
- When processing non-critical data

**Configuration:**

```python
# Using string configuration
config = tm.TransmogConfig.default().with_error_handling(recovery_strategy="skip")

# Or using the strategy class
from transmog.error import SkipAndLogRecovery
config = tm.TransmogConfig.default().with_error_handling(recovery_strategy=SkipAndLogRecovery())

# Or using the direct strategy constant
from transmog.error import DEFAULT  # DEFAULT is the skip-and-log strategy
config = tm.TransmogConfig.default().with_error_handling(recovery_strategy=DEFAULT)
```

### 3. Partial Processing Recovery (`PartialProcessingRecovery`)

The partial recovery strategy attempts to salvage valid data from problematic records. It maintains the
structure of the original data while marking problematic sections.

**When to use:**

- For nested data structures where isolated parts may be problematic
- During data migrations from legacy systems
- When processing API responses or external data sources
- When you need to maximize data yield
- For exploratory data analysis

**Configuration:**

```python
# Using string configuration
config = tm.TransmogConfig.default().with_error_handling(recovery_strategy="partial")

# Or using the strategy class
from transmog.error import PartialProcessingRecovery
config = tm.TransmogConfig.default().with_error_handling(recovery_strategy=PartialProcessingRecovery())

# Or using the direct strategy constant
from transmog.error import LENIENT  # LENIENT is the partial recovery strategy
config = tm.TransmogConfig.default().with_error_handling(recovery_strategy=LENIENT)

# Or using the factory method
processor = tm.Processor.with_partial_recovery()
```

**Example with partial recovery:**

```python
from transmog import Processor
from transmog.error import LENIENT

# Data with problematic parts
data = {
    "id": 123,
    "name": "Test",
    "metrics": {
        "valid": 42,
        "invalid": float('nan'),  # This would normally cause an error
    }
}

# Create processor with partial recovery
processor = Processor.with_partial_recovery()

# Process data - with partial recovery, this won't fail
result = processor.process(data, entity_name="test")

# Check the results
main_table = result.get_main_table()[0]
print(f"ID: {main_table['id']}")
print(f"Name: {main_table['name']}")
print(f"Valid metric: {main_table['metrics_valid']}")
# The NaN value will have an error marker
if "_error" in main_table:
    print(f"Error: {main_table['_error']}")
```

## Strategy Comparison Table

| Feature                      | StrictRecovery    | SkipAndLogRecovery | PartialProcessingRecovery     |
|------------------------------|-------------------|-------------------|--------------------------------|
| **Error behavior**           | Raises exceptions | Skips entire record | Preserves valid portions        |
| **Data integrity**           | Highest           | Medium            | Medium (marked errors)          |
| **Data completeness**        | Lowest (fails)    | Medium            | Highest                         |
| **Processing reliability**   | Fails on any error | Continues        | Continues                       |
| **Error identification**     | Exception message | Log messages      | Error markers in data           |
| **Complexity**               | Simple            | Medium            | Complex                         |
| **Constants**                | `STRICT`          | `DEFAULT`         | `LENIENT`                       |
| **String identifier**        | `"strict"`        | `"skip"`          | `"partial"`                     |
| **Best for**                 | Critical data     | Batch processing  | Complex data migration          |

## The Error Context

Transmog provides an `error_context` decorator for wrapping functions with error handling:

```python
from transmog.error import error_context, LENIENT

@error_context("Processing customer data", recovery_strategy=LENIENT)
def process_customer_data(data):
    # This function will use the LENIENT recovery strategy
    # Any errors will be caught and handled according to the strategy
    return processed_data
```

## Error Logging

Transmog includes a logging framework for tracking errors:

```python
from transmog.error import setup_logging, logger

# Configure logging
setup_logging(log_file="transmog.log", level="INFO")

# Use the logger
try:
    result = processor.process(data)
except Exception as e:
    logger.error(f"Failed to process data: {e}")
```

## Handling Specific Error Types

Different strategies for different error types:

```python
from transmog import Processor, TransmogConfig
from transmog.error import ParsingError, LENIENT

# Create a processor that handles errors gracefully
processor = Processor(
    config=TransmogConfig.default()
    .with_error_handling(
        recovery_strategy=LENIENT,
        allow_malformed_data=True
    )
)

try:
    # Try processing with more lenient recovery
    result = processor.process(data)
except ParsingError:
    # Fall back to text processing for parsing errors
    result = process_as_text(data)
```

## Best Practices

1. **Match the strategy to the data quality**
   - Use `STRICT` for well-structured, high-quality data
   - Use `DEFAULT` for batch processing where some errors are acceptable
   - Use `LENIENT` for data migration or processing external data sources

2. **Add proper error logging**
   - Configure logging to capture error contexts
   - Log both the error and the problematic data when possible

3. **Combine strategies with other options**
   - Pair `LENIENT` with `allow_malformed_data=True` for maximum resilience
   - Use `cast_to_string=True` to handle numeric type issues

4. **Use the error context in custom code**
   - Wrap custom processing functions with `@error_context`
   - Specify the appropriate recovery strategy

5. **Inspect error markers in recovered data**
   - Look for fields with `_error` to identify problematic values

6. **Use factory methods for common configurations**
   - `Processor.with_partial_recovery()` for maximum data yield

## Real-World Use Cases for Recovery Strategies

### Strict Recovery

- Financial data processing where accuracy is critical
- Regulatory compliance systems
- Critical infrastructure monitoring
- Systems of record for master data

### Skip and Log Recovery

- Large-scale batch ETL jobs
- Initial data exploration
- Non-critical data collection processes
- Scenarios where data loss is preferable to inaccurate data

### Partial Recovery

- Data migration from legacy systems
- Processing API responses with inconsistent structures
- Social media or user-generated content analysis
- Cross-system data synchronization
- Historical data integration with schema evolution
- Extracting information from corrupted files

## Recovery Strategy Best Practices

1. **Choose the Right Strategy for Your Use Case**
   - Consider data criticality, volume, and structure
   - Balance completeness vs. correctness requirements
   - For critical data, start with strict recovery and adjust as needed
   - Use partial recovery when data salvage is the priority

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
   - Validate that error markers are properly handled downstream

6. **Simplify Config with Strategy Constants**
   - Use `STRICT`, `DEFAULT`, and `LENIENT` constants for clarity
   - Document the chosen strategy in your code

7. **Combine with Type Conversion**
   - Set `cast_to_string=True` when using partial recovery to handle type mismatches
   - This allows recovery from numeric type issues like NaN/Infinity values

## Examples

### Factory Usage Pattern

Here's a pattern for creating pre-configured processors with different recovery strategies:

```python
from transmog import Processor, TransmogConfig
from transmog.error import STRICT, DEFAULT, LENIENT

def create_processor(strategy="skip", **kwargs):
    """
    Factory function to create processors with different error handling strategies.

    Args:
        strategy: One of "strict", "skip", or "partial"
        **kwargs: Additional processor configuration options

    Returns:
        Configured Processor instance
    """
    strategy_map = {
        "strict": STRICT,
        "skip": DEFAULT,
        "partial": LENIENT
    }

    recovery_strategy = strategy_map.get(strategy, DEFAULT)

    config = TransmogConfig.default().with_error_handling(
        recovery_strategy=recovery_strategy,
        allow_malformed_data=(strategy != "strict")
    )

    return Processor(config=config, **kwargs)

# Usage
strict_processor = create_processor("strict")
skip_processor = create_processor("skip")
partial_processor = create_processor("partial")
```

For more examples of partial recovery in action, see the `partial_recovery_example.py` file in the examples directory.
