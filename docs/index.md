---
hide-toc: false
---

# Transmog Documentation

Transmog is a Python library for transforming nested JSON data into flat, structured formats.

:::info API Overview
**Transmog - Simple API**

Transmog provides a simplified API while preserving all advanced functionality:

- **Simple API**: `tm.flatten()` for easy data transformation
- **Streaming Support**: `tm.flatten_stream()` for memory-efficient processing
- **File Processing**: `tm.flatten_file()` for direct file handling
- **Advanced Features**: Full `Processor` API available for complex use cases
- **Intuitive Results**: `FlattenResult` with `.main`, `.tables`, and `.save()` methods
:::

## Quick Start

```python
import transmog as tm

# Basic usage - transform nested data
result = tm.flatten({"name": "Product", "tags": ["sale", "clearance"]})
print(result.main)      # Main table
print(result.tables)    # Child tables
result.save("output.json")  # Save to file

# Stream large datasets
tm.flatten_stream(large_data, "output/", format="parquet")

# Process files directly
result = tm.flatten_file("data.json", name="products")
```

## Overview

Transmog features:

- **Simple API**: Easy-to-use functions for 90% of use cases
- **Powerful Processing**: Flattens nested JSON/dict structures into relational tables
- **Array Extraction**: Converts arrays into separate tables with parent-child relationships
- **Multiple Formats**: Output to JSON, CSV, Parquet, and more
- **Memory Efficient**: Streaming support for datasets that don't fit in memory
- **Flexible IDs**: Auto-generate IDs or use existing natural ID fields
- **Error Handling**: Robust error recovery for real-world data

## Quick Links

- [Installation Guide](installation.md)
- [Getting Started Guide](user/essentials/getting-started.md)
- [Basic Concepts](user/essentials/basic-concepts.md)
- [Data Structures](user/essentials/data-structures.md)

## API Overview

### Simple API - Recommended

For most use cases, the simple API is recommended:

```python
import transmog as tm

# Main functions
result = tm.flatten(data, name="products")           # Basic flattening
result = tm.flatten_file("data.json")                # Process files
tm.flatten_stream(data, "output/", format="json")    # Memory-efficient streaming

# Result manipulation
result.main              # Main table
result.tables           # Child tables dictionary
result.save("out.csv")  # Save to file

```

### Advanced API - For Complex Use Cases

For advanced features, import the Processor directly:

```python
from transmog.process import Processor
from transmog.config import TransmogConfig

# Full control over processing
config = TransmogConfig.default().with_naming(separator=".")
processor = Processor(config)
result = processor.process(data, entity_name="products")

# Advanced streaming
processor.stream_process(data, entity_name="products", output_format="parquet")
```

## Documentation Structure

The Transmog documentation is organized into several key sections:

### User Guides

User guides provide conceptual overviews, practical examples, and best practices for using Transmog.

#### Essentials

- [Getting Started](user/essentials/getting-started.md) - First steps with Transmog
- [Basic Concepts](user/essentials/basic-concepts.md) - Fundamental concepts
- [Data Structures](user/essentials/data-structures.md) - Input and output data structures
- [Configuration](user/essentials/configuration.md) - Simple and advanced configuration options
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

- [Streaming](user/advanced/streaming.md) - Processing large datasets with memory efficiency
- [Performance Optimization](user/advanced/performance-optimization.md) - Optimizing for speed and memory
- [Error Handling](user/advanced/error-handling.md) - Dealing with problematic data
- [Deterministic IDs](user/advanced/deterministic-ids.md) - Generating consistent IDs
- [Natural IDs](user/advanced/natural-ids.md) - Using existing ID fields in data

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
- [Using Natural IDs](tutorials/intermediate/using-natural-ids.md)
- [Error Recovery Strategies](tutorials/advanced/error-recovery-strategies.md)
- [Optimizing Memory Usage](tutorials/advanced/optimizing-memory-usage.md)

## Where to Start

- **Starting with Transmog?** Begin with [Getting Started](user/essentials/getting-started.md) and [Basic Concepts](user/essentials/basic-concepts.md)
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
user/advanced/natural-ids
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
tutorials/intermediate/using-natural-ids
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
