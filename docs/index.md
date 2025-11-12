---
hide-toc: false
---

# Transmog Documentation

Transforms nested JSON data into flat, tabular formats while preserving relationships between parent and child records.

## Overview

Transmog flattens nested objects, extracts arrays into separate tables,
generates unique identifiers, and supports CSV and Parquet output formats
with configurable error handling and batch processing.

## Example

```python
import transmog as tm

data = {"company": "TechCorp", "employees": [{"name": "Alice", "role": "Engineer"}]}
result = tm.flatten(data, name="companies")

print(result.main)    # Main company data
print(result.tables)  # Employee data in separate table
```

## Documentation

- **[Getting Started](getting_started.md)** - Installation and tutorial
- **[Configuration](configuration.md)** - Configuration parameters
- **[Array Handling](arrays.md)** - Array processing modes
- **[ID Management](ids.md)** - ID generation strategies
- **[Error Handling](errors.md)** - Error recovery modes
- **[Output Formats](outputs.md)** - CSV and Parquet output
- **[Streaming](streaming.md)** - Large dataset processing
- **[API Reference](api.md)** - Function and class documentation
- **[Contributing](contributing.md)** - Development guide

```{toctree}
:maxdepth: 2
:hidden:

getting_started
configuration
arrays
ids
errors
outputs
streaming
api
contributing
```
