---
hide-toc: false
---

# Transmog Documentation

Transform nested JSON data into flat, tabular formats while preserving relationships
between parent and child records.

## What is Transmog?

Transmog transforms complex, nested data structures into flat tables suitable for databases,
analytics, and data processing. The system:

- Flattens nested objects using configurable path notation
- Extracts arrays into separate relational tables
- Generates unique identifiers for record tracking
- Supports multiple output formats (CSV, Parquet)
- Includes unified error handling with consistent recovery strategies
- Provides memory-efficient processing with configurable batch sizes

## Quick Example

```python
import transmog as tm

# Transform nested data with one function call
data = {"company": "TechCorp", "employees": [{"name": "Alice", "role": "Engineer"}]}
result = tm.flatten(data, name="companies")

# Get flat tables with preserved relationships
print(result.main)          # Main company data
print(result.tables)        # Employee data in separate table
```

## Documentation Sections

### Getting Started

Start here for installation, basic concepts, and first steps.

- **Quick introduction** to data flattening concepts
- **10-minute tutorial** to get up and running
- **Essential configuration** options
- **Common use case patterns**

[**→ Getting Started Guide**](getting_started.md)

### User Guide

Comprehensive guide covering all functionality with practical examples.

- **Core Functions** - `flatten()`, `flatten_file()`, `flatten_stream()`
- **Configuration** - All parameters and presets
- **Array Handling** - SMART, SEPARATE, INLINE, SKIP modes
- **ID Management** - Natural IDs, deterministic IDs, discovery
- **Error Handling** - STRICT and SKIP recovery modes
- **Output Formats** - CSV and Parquet
- **Integration Examples** - Database import, analytics pipelines

[**→ User Guide**](user_guide.md)

### API Reference

Complete technical documentation of all functions, classes, and parameters. Detailed reference for every public API component.

- **Functions** - `flatten()`, `flatten_file()`, `flatten_stream()`
- **Classes** - `FlattenResult` with all methods and properties
- **Error Types** - Exception classes and error handling
- **Type Definitions** - Complete type annotations

[**→ API Reference**](api_reference/api.md)

### Developer Guide

Contributing and advanced usage patterns. Resources for developers wanting to contribute or use advanced features.

- **Contributing Guidelines** - How to contribute to the project
- **Streaming Processing** - Memory-efficient processing techniques
- **Custom Configuration** - Advanced configuration options

[**→ Developer Guide**](developer_guide/contributing.md)

## Support and Community

- **GitHub Issues**: [Bug reports and feature requests](https://github.com/scottdraper8/transmog/issues)
- **Questions**: Use GitHub issues for questions and community support
- **Documentation**: Complete guides and reference materials

```{toctree}
:maxdepth: 2
:hidden:
:caption: Getting Started

getting_started
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: User Guide

user_guide
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: API Reference

api_reference/api
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: Developer Guide

developer_guide/contributing
developer_guide/streaming
developer_guide/custom-configuration
```
