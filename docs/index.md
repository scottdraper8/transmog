---
hide-toc: false
---

# Transmog Documentation

Transmog is a Python library for transforming nested JSON data into flat, structured formats.

:::info Version Note
**Transmog 0.1.2.5 - Current Release**

Features include:

- `TransmogConfig` system with a fluent API
- Processing strategies for different data sources
- Streaming capabilities
- Memory efficiency options
- Error handling strategies
- Multiple output format options
:::

## Overview

Transmog features:

- Flattening nested JSON/dict structures
- Extracting arrays into separate tables with parent-child relationships
- Transforming values during processing
- Generating IDs for records
- Converting to various output formats
- Processing large datasets
- Configuring memory usage

## Getting Started

- [Installation Guide](installation.md)
- [Quick Start Guide](user/getting-started.md)

## Core Concepts

### Flattening and Transformation

- [Flattening Nested Data](user/flattening.md)
- [Working with Arrays](user/arrays.md)
- [Metadata Management](user/metadata.md)
- [Deterministic ID Generation](user/deterministic-ids.md)
- [Error Handling](user/error-handling.md)

### Processing Strategies

- [Processing Strategies](user/strategies.md) - Different processing strategies
- [Streaming Processing](user/streaming.md) - Process data streams

### Output Formats

- [Output Format Options](user/output-formats.md) - Available output formats
- [In-Memory Processing](user/in-memory-processing.md) - In-memory processing

### Configuration System

- [Configuration Guide](user/configuration.md) - Configuration options
- [Error Recovery Strategies](user/error-handling.md) - Error handling during processing

## API Reference

- [Processor API](api/processor.md)
- [ProcessingResult API](api/processing-result.md)
- [Configuration API](api/config.md) - Configuration classes and options
- [CSV Reader API](api/csv-reader.md) - CSV file processing

## Examples

- Streaming Processing
- Error Recovery
- [Partial Recovery](../examples/partial_recovery_example.py)

## For Developers

- Contributing
- Development Guide
- [Testing Guide](dev/testing.md)
- [Benchmarking Guide](dev/benchmarking.md)
- [Architecture](dev/architecture.md) - Internal design

## Roadmap

See our [ROADMAP.md](https://github.com/scottdraper8/transmog/blob/main/ROADMAP.md) file for upcoming features.

## Support & Community

- [GitHub Issues](https://github.com/scottdraper8/transmog/issues)

## Key Features

### Configuration System

- Configuration with `TransmogConfig` class
- Pre-configured modes (`memory_optimized`, `performance_optimized`)
- Fluent API for configuration (`with_naming`, `with_processing`, etc.)
- Configuration components for different aspects (`NamingConfig`, `ProcessingConfig`, etc.)

### Processing Strategies

- Strategies for different data sources:
- `InMemoryStrategy` for small datasets
- `FileStrategy` for processing files
- `BatchStrategy` for batch processing
- `ChunkedStrategy` for large datasets
- `CSVStrategy` for CSV processing

### Input Handling

- Process nested JSON/dict structures
- Handle arrays and nested arrays
- Extract arrays to child tables with parent-child relationships
- Support for streaming data processing

### ID Generation Options

- Random UUIDs (default)
- Field-based deterministic IDs using specified fields
- Custom function-based ID generation

### Output Formats

- Native Data Structures:
  - Python dictionaries (`to_dict()`)
  - JSON-serializable objects (`to_json_objects()`)
  - PyArrow Tables (`to_pyarrow_tables()`)

- Bytes Serialization:
  - JSON bytes (`to_json_bytes()`)
  - CSV bytes (`to_csv_bytes()`)
  - Parquet bytes (`to_parquet_bytes()`)

- File Output:
  - JSON files (`write_all_json()`)
  - CSV files (`write_all_csv()`)
  - Parquet files (`write_all_parquet()`)

- Memory Management with `ConversionMode`:
  - Eager conversion - converts and caches data (`EAGER`)
  - Lazy conversion - converts when needed (`LAZY`)
  - Memory-efficient conversion - minimizes memory (`MEMORY_EFFICIENT`)

### Error Recovery Strategies

- Error recovery options:
  - Strict recovery - raises errors
  - Skip-and-log recovery - skips problematic records
  - Partial recovery - extracts valid portions of records with errors
  - Logging and error tracking
  - Configurable error handling

### Performance Options

- Memory-efficient processing modes
- Processing modes for memory/performance trade-offs
- Streaming data processing
- Batch processing

```{toctree}
:maxdepth: 1
:hidden:
:caption: Getting Started

user/getting-started
installation
README
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: User Guide

user/configuration
user/flattening
user/arrays
user/metadata
user/deterministic-ids
user/streaming
user/strategies
user/output-formats
user/in-memory-processing
user/error-handling
user/json-processing
user/json-transform
user/csv-processing
user/streaming-parquet
user/schema
user/naming
user/transforms
user/caching
user/data-flow
user/hierarchy
user/concurrency
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: API Reference

api/processor
api/processing-result
api/config
api/csv-reader
api/core
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: Developer Guide

dev/architecture
dev/extending
dev/testing
dev/benchmarking
dev/code-style
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: Tutorials

tutorials/data-transformation
```

## Indices and Tables

- {ref}`genindex`
- {ref}`modindex`
- {ref}`search`
