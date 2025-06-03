---
hide-toc: false
---

# Transmog Documentation

Transmog is a Python library for transforming nested JSON data into flat, structured formats.

:::info Version Note
**Transmog 1.0.6 - Current Release**

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

## Quick Links

- [Installation Guide](installation.md)
- [Getting Started Guide](user/essentials/getting-started.md)
- [Basic Concepts](user/essentials/basic-concepts.md)
- [Data Structures](user/essentials/data-structures.md)

## Documentation Structure

The Transmog documentation is organized into several key sections:

### User Guides

User guides provide conceptual overviews, practical examples, and best practices for using Transmog.

#### Essentials

- [Getting Started](user/essentials/getting-started.md) - First steps with Transmog
- [Basic Concepts](user/essentials/basic-concepts.md) - Fundamental concepts
- [Data Structures](user/essentials/data-structures.md) - Input and output data structures
- [Configuration](user/essentials/configuration.md) - Options for customizing Transmog
- [Dependencies and Features](user/essentials/dependencies-and-features.md) - Optional dependencies and features

#### Processing

- [Processing Overview](user/processing/processing-overview.md) - General processing guide
- [Data Transformation](user/processing/data-transformation.md) - Transforming data structures
- [File Processing](user/processing/file-processing.md) - Working with files
- [JSON Handling](user/processing/json-handling.md) - Working with JSON data
- [CSV Processing](user/processing/csv-processing.md) - Working with CSV data
- [Naming](user/processing/naming.md) - Field and table naming
- [Array Handling](user/processing/array-handling.md) - Options for array processing
- [Metadata](user/processing/metadata.md) - Working with metadata
- [Transforms](user/processing/transforms.md) - Transformation functions and operations
- [IO Operations](user/processing/io.md) - Input/output operations

#### Advanced Topics

- [Streaming](user/advanced/streaming.md) - Processing large datasets
- [Performance Optimization](user/advanced/performance-optimization.md) - Optimizing for speed and memory
- [Error Handling](user/advanced/error-handling.md) - Dealing with problematic data
- [Deterministic IDs](user/advanced/deterministic-ids.md) - Generating consistent IDs

#### Output Options

- [Output Formats](user/output/output-formats.md) - Available output formats and when to use them

### API Reference

API reference documents provide technical details about the Transmog API.

### Developer Guides

Developer guides contain information for those contributing to Transmog or extending its functionality.

### Tutorials

Step-by-step guides to accomplish specific tasks with Transmog:

- [Transform Nested JSON](tutorials/basic/transform-nested-json.md)
- [Flatten and Normalize](tutorials/basic/flatten-and-normalize.md)
- [Streaming Large Datasets](tutorials/intermediate/streaming-large-datasets.md)
- [Customizing ID Generation](tutorials/intermediate/customizing-id-generation.md)
- [Error Recovery Strategies](tutorials/advanced/error-recovery-strategies.md)
- [Optimizing Memory Usage](tutorials/advanced/optimizing-memory-usage.md)

## Where to Start

- **New to Transmog?** Start with [Getting Started](user/essentials/getting-started.md) and [Basic Concepts](user/essentials/basic-concepts.md)
- **Need to solve a specific problem?** Try the tutorials section
- **Looking for detailed API information?** Check the API reference section

```{toctree}
:maxdepth: 1
:hidden:
:caption: Getting Started

installation
user/essentials/getting-started
user/essentials/basic-concepts
user/essentials/data-structures
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
user/processing/array-handling
user/processing/metadata
user/processing/naming
user/processing/transforms
user/processing/io
user/advanced/streaming
user/advanced/performance-optimization
user/advanced/error-handling
user/advanced/deterministic-ids
user/output/output-formats
user/examples
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
