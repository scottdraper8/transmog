---
hide-toc: false
---

# Transmog Documentation

Welcome to the Transmog documentation. Transmog is a Python library for transforming complex nested JSON data into flat, structured formats.

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

### Flexible Output Formats

- [Output Format Options](user/output-formats.md) - Learn about the different output formats available
- [In-Memory Processing](user/in-memory-processing.md) - Process data entirely in memory
- [Streaming Processing](user/streaming.md) - Process data streams efficiently

## API Reference

- [Processor API](api/processor.md)
- [ProcessingResult API](api/processing-result.md)
- [CSV Reader API](api/csv-reader.md) - Process CSV files efficiently

## Examples

- [Basic Examples](examples/basic.md)
- [Output Formats Example](examples/output-formats-example.md) - Demonstrates all output formats
- [CSV Processing Example](examples/csv-processing.md) - Process and transform CSV data
- [Deterministic ID Generation](examples/deterministic-id-example.md) - Generate consistent IDs across processing runs

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

### Performance Optimization

* Memory-efficient processing with configurable modes
* Processing modes for memory/performance trade-offs
* Streaming data processing through iterators
* Processing of multiple records in batches

### Error Handling

* Configurable error recovery strategies
* Detailed error reporting

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

user/flattening
user/arrays
user/deterministic-ids
user/streaming
user/error-handling
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: API Reference

api/processor
api/processing-result
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

examples/basic
examples/csv-processing
examples/deterministic-id-example
examples/streaming-example
```

## Indices and Tables

* {ref}`genindex`
* {ref}`modindex`
* {ref}`search` 