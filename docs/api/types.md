# Types API

> **User Guide**: For a user-friendly overview of the type system, see [Data Structures and Flow](../user/essentials/data-structures.md).

This document provides the technical details of Transmog's type system.

## Public Types

Transmog's public API exposes a minimal set of types that users need to interact with:

```python
import transmog as tm

# Result type
result = tm.flatten(data, name="customer")
isinstance(result, tm.FlattenResult)  # True

# Error handling options
result = tm.flatten(data, name="customer", error_handling="skip")
# Valid options: "raise", "skip", "warn"
```

## Core Data Types

The core data types used in Transmog are simple Python dictionaries and lists:

- Input data: `dict[str, Any]` or `list[dict[str, Any]]`
- Output tables: `dict[str, list[dict[str, Any]]]`
- Table records: `list[dict[str, Any]]`

## Internal Types

> Note: These types are considered internal implementation details. Most users should use the main API functions instead.

For advanced users who need to work with the internal types:

```python
from transmog.types.base import JsonDict, FlatDict, ArrayDict
from transmog.types.processing_types import ProcessingMode, ConversionMode
from transmog.types.result_types import TableDict, TableFormat
from transmog.types.io_types import InputFormat, OutputFormat
```

### Processing Types

```python
from transmog.types.processing_types import ProcessingMode

# Processing modes
mode = ProcessingMode.STANDARD       # Default mode
mode = ProcessingMode.LOW_MEMORY     # Memory-optimized mode
mode = ProcessingMode.PERFORMANCE    # Performance-optimized mode
```

### Result Types

```python
from transmog.types.result_types import ConversionMode

# Conversion modes for result handling
mode = ConversionMode.EAGER           # Convert and cache immediately
mode = ConversionMode.LAZY            # Convert only when needed
mode = ConversionMode.MEMORY_EFFICIENT  # Minimize memory usage
```

### IO Types

```python
from transmog.types.io_types import InputFormat, OutputFormat

# Input formats
fmt = InputFormat.JSON       # Standard JSON
fmt = InputFormat.JSONL      # JSON Lines
fmt = InputFormat.CSV        # CSV
fmt = InputFormat.AUTO       # Auto-detect

# Output formats
fmt = OutputFormat.JSON      # JSON
fmt = OutputFormat.CSV       # CSV
fmt = OutputFormat.PARQUET   # Apache Parquet
```

## Type Aliases

For reference, here are the main type aliases used internally:

```python
# Base types
JsonDict = dict[str, Any]              # Any JSON-compatible dictionary
FlatDict = dict[str, Any]              # Flattened record
ArrayDict = dict[str, list[Any]]       # Dictionary with array values

# Result types
TableDict = dict[str, list[dict[str, Any]]]  # Dictionary of tables
TableFormat = Literal["json", "csv", "parquet"]  # Output format

# Error handling
ErrorHandling = Literal["raise", "skip", "warn"]  # Error handling options
```
