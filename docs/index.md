---
hide-toc: false
---

# Transmog Documentation

Transmog is a Python library for transforming nested JSON data into flat, structured formats.

:::info Version Note
**Transmog 1.0.3 - Current Release**

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

## Documentation Structure

For an overview of how the documentation is organized, see the [Documentation Map](documentation_map.md).

## Getting Started

- [Installation Guide](installation.md)
- [Getting Started Guide](user/essentials/getting-started.md)
- [Basic Concepts](user/essentials/basic-concepts.md)
- [Data Structures](user/essentials/data-structures.md)

## Quick Start Tutorials

- [Transform Nested JSON](tutorials/basic/transform-nested-json.md)
- [Flatten and Normalize](tutorials/basic/flatten-and-normalize.md)

## Core User Guides

### Essentials

- [Configuration](user/essentials/configuration.md)
- [Dependencies and Features](user/essentials/dependencies-and-features.md)

### Processing

- [Processing Overview](user/processing/processing-overview.md)
- [Data Transformation](user/processing/data-transformation.md)
- [JSON Handling](user/processing/json-handling.md)
- [CSV Processing](user/processing/csv-processing.md)
- [File Processing](user/processing/file-processing.md)
- [Metadata](user/processing/metadata.md)
- [Naming](user/processing/naming.md)
- [Transforms](user/processing/transforms.md)
- [IO Operations](user/processing/io.md)

### Advanced

- [Streaming](user/advanced/streaming.md)
- [Performance Optimization](user/advanced/performance-optimization.md)
- [Error Handling](user/advanced/error-handling.md)
- [Deterministic ID Generation](user/advanced/deterministic-ids.md)

### Output

- [Output Formats](user/output/output-formats.md)

## API Reference

- [Processor API](api/processor.md)
- [ProcessingResult API](api/processing-result.md)
- [Process API](api/process.md)
- [Configuration API](api/config.md)
- [CSV Reader API](api/csv-reader.md)
- [Error Handling API](api/error.md)
- [IO API](api/io.md)
- [Naming API](api/naming.md)
- [Types API](api/types.md)

## Advanced Tutorials

- [Streaming Large Datasets](tutorials/intermediate/streaming-large-datasets.md)
- [Customizing ID Generation](tutorials/intermediate/customizing-id-generation.md)
- [Error Recovery Strategies](tutorials/advanced/error-recovery-strategies.md)
- [Optimizing Memory Usage](tutorials/advanced/optimizing-memory-usage.md)

## For Developers

- [Architecture](dev/architecture.md)
- [Extending Transmog](dev/extending.md)
- [Testing Guide](dev/testing.md)
- [Benchmarking Guide](dev/benchmarking.md)
- [Code Style Guide](dev/code-style.md)

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

user/essentials/getting-started
user/essentials/basic-concepts
user/essentials/data-structures
installation
documentation_map
README
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: User Guide

user/essentials/configuration
user/essentials/dependencies-and-features
user/processing/processing-overview
user/processing/data-transformation
user/processing/json-handling
user/processing/csv-processing
user/processing/file-processing
user/processing/metadata
user/processing/naming
user/processing/transforms
user/processing/io
user/advanced/streaming
user/advanced/performance-optimization
user/advanced/error-handling
user/advanced/deterministic-ids
user/output/output-formats
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: Tutorials

tutorials/basic/transform-nested-json
tutorials/basic/flatten-and-normalize
tutorials/intermediate/streaming-large-datasets
tutorials/intermediate/customizing-id-generation
tutorials/advanced/error-recovery-strategies
tutorials/advanced/optimizing-memory-usage
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: API Reference

api/processor
api/processing-result
api/process
api/config
api/csv-reader
api/core
api/error
api/io
api/naming
api/types
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

## Indices and Tables

- {ref}`
