---
hide-toc: false
---

# Transmog Documentation

Welcome to the Transmog documentation. Transmog is a Python library for transforming complex nested JSON data into flat, structured formats.

:::info Important Update
**Transmog 0.2.0 Release - Major Refactoring**

Transmog has undergone a significant refactoring in version 0.2.0 with:
- A new `TransmogConfig` system with a fluent API
- Processing strategies for different data sources
- Enhanced streaming capabilities
- Improved memory efficiency options
- Consistent error handling strategies
- New bytes serialization options

This documentation has been updated to reflect these changes.
:::

## Overview

Transmog enables you to:

- Flatten complex nested JSON/dict structures
- Extract arrays into separate tables with parent-child relationships
- Transform values during processing
- Generate consistent IDs for records across processing runs
- Convert to various output formats
- Handle large datasets with streaming processing
- Configure memory usage for different requirements

## Getting Started

- [Installation Guide](installation.md)
- [Quick Start Guide](user/getting-started.md)

## Core Concepts

### Flattening and Transformation

- [Flattening Nested Data](user/flattening.md)
- [Working with Arrays](user/arrays.md)
- [Deterministic ID Generation](user/deterministic-ids.md)
- [Error Handling](user/error-handling.md)

### Processing Strategies

- [Processing Strategies](user/strategies.md) - Learn about the different processing strategies
- [Streaming Processing](user/streaming.md) - Process data streams efficiently

### Flexible Output Formats

- [Output Format Options](user/output-formats.md) - Learn about the different output formats available
- [In-Memory Processing](user/in-memory-processing.md) - Process data entirely in memory

### Configuration System

- [Configuration Guide](user/configuration.md) - Configure Transmog for your needs
- [Error Recovery Strategies](user/error-handling.md) - Handle errors during processing

## API Reference

- [Processor API](api/processor.md)
- [ProcessingResult API](api/processing-result.md)
- [Configuration API](api/config.md) - Configuration classes and options
- [CSV Reader API](api/csv-reader.md) - Process CSV files efficiently

## Examples

- [Basic Usage](../examples/basic/simple_flatten.py)
- [Output Formats](../examples/basic/native_output_formats.py)
- [Data Processing](../examples/data/data_cleanup_example.py)
- [Advanced Features](../examples/advanced/advanced_usage.py)
- [Streaming Processing](../examples/streaming_example.py)
- [Processing Strategies](../examples/advanced/processing_strategies.py)

## For Developers

- [Contributing](dev/contributing.md)
- [Development Guide](dev/development-guide.md)
- [Testing Guide](dev/testing.md)
- [Architecture](dev/architecture.md) - Learn about Transmog's internal design

## Roadmap

See our [ROADMAP.md](https://github.com/scottdraper8/transmog/blob/main/ROADMAP.md) file for upcoming features and enhancements.

## Support & Community

- [GitHub Issues](https://github.com/scottdraper8/transmog/issues)
- [Discussions](https://github.com/scottdraper8/transmog/discussions)

## Key Features

### Configuration System

* Flexible configuration with `TransmogConfig` class
* Pre-configured modes for common use cases (`memory_optimized`, `performance_optimized`)
* Fluent API for easy configuration (`with_naming`, `with_processing`, etc.)
* Separate configuration components for different aspects (`NamingConfig`, `ProcessingConfig`, etc.)

### Processing Strategies

* Different strategies for different data sources and requirements
* `InMemoryStrategy` for small datasets
* `FileStrategy` for processing files
* `BatchStrategy` for batch processing
* `ChunkedStrategy` for large datasets
* `CSVStrategy` for CSV processing

### Flexible Input Handling

* Process nested JSON/dict structures
* Handle arrays and nested arrays
* Extract arrays to child tables with parent-child relationships
* Support for streaming data processing with iterators

### ID Generation Options

* Random UUIDs (default behavior)
* Field-based deterministic IDs using specified fields at configurable hierarchy levels
* Custom function-based ID generation for advanced use cases

### Multiple Output Formats

* Native Data Structures:
  * Python dictionaries (`to_dict()`)
  * JSON-serializable objects (`to_json_objects()`)
  * PyArrow Tables (`to_pyarrow_tables()`)
  
* Bytes Serialization:
  * JSON bytes (`to_json_bytes()`)
  * CSV bytes (`to_csv_bytes()`)
  * Parquet bytes (`to_parquet_bytes()`)
  
* File Output:
  * JSON files (`write_all_json()`)
  * CSV files (`write_all_csv()`)
  * Parquet files (`write_all_parquet()`)

* Memory Management with `ConversionMode`:
  * Eager conversion - immediately converts and caches data (`EAGER`)
  * Lazy conversion - converts only when needed (`LAZY`)
  * Memory-efficient conversion - minimizes memory usage (`MEMORY_EFFICIENT`)

### Error Recovery Strategies

* **Robust Error Recovery:** Multiple recovery strategies for malformed data:
  * Strict recovery enforces data integrity by raising errors
  * Skip-and-log recovery continues processing by skipping problematic records
  * Partial recovery extracts valid portions of records with errors
  * Comprehensive logging and error tracking
  * Configurable error handling with custom strategies

### Performance Optimization

* Memory-efficient processing with configurable modes
* Processing modes for memory/performance trade-offs
* Streaming data processing through iterators
* Processing of multiple records in batches

```{toctree}
:maxdepth: 1
:hidden:
:caption: Getting Started

user/getting-started
installation
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: User Guide

user/configuration
user/flattening
user/arrays
user/streaming
user/strategies
user/output-formats
user/in-memory-processing
user/deterministic-ids
user/error-handling
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: API Reference

api/processor
api/processing-result
api/config
api/csv-reader
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: Developer Guide

dev/architecture
dev/extending
dev/testing
dev/code-style
dev/release-process
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: Examples

../examples/README
```

## Indices and Tables

* {ref}`genindex`
* {ref}`modindex`
* {ref}`search`
