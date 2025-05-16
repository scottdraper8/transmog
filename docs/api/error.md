# Error API

> **User Guide**: For usage guidance and examples, see the [Error Handling Guide](../user/advanced/error-handling.md).

This document describes the error handling functionality in Transmog.

## Error Types

### TransmogError

```python
from transmog import TransmogError
```

Base exception class for all Transmog errors.

### Specialized Errors

```python
from transmog.error import (
    ProcessingError,
    ValidationError,
    ParsingError,
    FileError,
    MissingDependencyError,
    ConfigurationError,
    OutputError
)
```

| Error Type | Description |
|------------|-------------|
| `ProcessingError` | Raised when data processing fails |
| `ValidationError` | Raised when data validation fails |
| `ParsingError` | Raised when parsing input data fails |
| `FileError` | Raised when file operations fail |
| `MissingDependencyError` | Raised when a required dependency is missing |
| `ConfigurationError` | Raised when there is an issue with configuration |
| `OutputError` | Raised when output generation fails |

## Recovery Strategies

```python
from transmog.error import (
    RecoveryStrategy,
    StrictRecovery,
    SkipAndLogRecovery,
    PartialProcessingRecovery
)
```

### RecoveryStrategy

Base class for all recovery strategies.

### StrictRecovery

Strategy that raises exceptions immediately when errors occur.

### SkipAndLogRecovery

Strategy that skips problematic records and logs errors.

### PartialProcessingRecovery

Strategy that attempts to recover partial data from records with errors.

## Predefined Strategy Constants

```python
from transmog.error import STRICT, DEFAULT, LENIENT
```

| Constant | Description |
|----------|-------------|
| `STRICT` | Strict recovery strategy instance |
| `DEFAULT` | Default recovery strategy instance |
| `LENIENT` | Lenient (partial) recovery strategy instance |

## Error Context Utility

The `error_context` decorator provides context for errors:

```python
from transmog.error import error_context

@error_context("Processing data failed", log_exceptions=True)
def process_data(data):
    # Processing code here
    pass
```

## Recovery Wrapper

The `with_recovery` decorator applies a recovery strategy:

```python
from transmog.error import with_recovery, SkipAndLogRecovery

@with_recovery(SkipAndLogRecovery())
def process_record(record):
    # Processing code here
    pass
```

## Logging Setup

Configure logging for error handling:

```python
from transmog.error import setup_logging

setup_logging(level="INFO")
```
