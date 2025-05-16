# Documentation Map

This document provides an overview of the Transmog documentation organization to help you find the information you need.

## Documentation Structure

The Transmog documentation is organized into several key sections:

### 1. User Guides

User guides provide conceptual overviews, practical examples, and best practices for using Transmog.
These guides are organized into logical categories:

#### Essentials (`/user/essentials/`)

- [Getting Started](user/essentials/getting-started.md) - First steps with Transmog
- [Basic Concepts](user/essentials/basic-concepts.md) - Core concepts and terminology
- [Configuration](user/essentials/configuration.md) - Configuring Transmog for your needs
- [Data Structures](user/essentials/data-structures.md) - Understanding data types and flow
- [Dependencies and Features](user/essentials/dependencies-and-features.md) - Optional dependencies and features

#### Processing (`/user/processing/`)

- [Processing Overview](user/processing/processing-overview.md) - Understanding the processing workflow
- [Data Transformation](user/processing/data-transformation.md) - Comprehensive guide to transforming data
- [JSON Handling](user/processing/json-handling.md) - Working with JSON data
- [CSV Processing](user/processing/csv-processing.md) - Working with CSV data
- [File Processing](user/processing/file-processing.md) - Processing data from files
- [Metadata](user/processing/metadata.md) - Working with metadata
- [Naming](user/processing/naming.md) - Naming conventions and strategies
- [Transforms](user/processing/transforms.md) - Transformation functions and operations
- [IO Operations](user/processing/io.md) - Input/output operations

#### Advanced (`/user/advanced/`)

- [Streaming](user/advanced/streaming.md) - Processing large datasets
- [Performance Optimization](user/advanced/performance-optimization.md) - Optimizing for speed and memory
- [Error Handling](user/advanced/error-handling.md) - Dealing with problematic data
- [Deterministic IDs](user/advanced/deterministic-ids.md) - Generating consistent IDs

#### Output (`/user/output/`)

- [Output Formats](user/output/output-formats.md) - Available output formats and when to use them

### 2. API Reference (`/api/`)

API reference documents provide technical details about the Transmog API, including:

- Class and function signatures
- Parameters and return types
- Method descriptions
- Implementation details

### 3. Developer Guides (`/dev/`)

Developer guides contain information for those contributing to Transmog or extending its functionality:

- [Architecture Overview](dev/architecture.md)
- [Testing Procedures](dev/testing.md)
- [Benchmarking](dev/benchmarking.md)
- [Code Style Guidelines](dev/code-style.md)
- [Extending Transmog](dev/extending.md)

### 4. Tutorials (`/tutorials/`)

Step-by-step guides to accomplish specific tasks with Transmog, organized by difficulty:

#### Basic Tutorials (`/tutorials/basic/`)

- [Transform Nested JSON](tutorials/basic/transform-nested-json.md)
- [Flatten and Normalize](tutorials/basic/flatten-and-normalize.md)

#### Intermediate Tutorials (`/tutorials/intermediate/`)

- [Streaming Large Datasets](tutorials/intermediate/streaming-large-datasets.md)
- [Customizing ID Generation](tutorials/intermediate/customizing-id-generation.md)

#### Advanced Tutorials (`/tutorials/advanced/`)

- [Error Recovery Strategies](tutorials/advanced/error-recovery-strategies.md)
- [Optimizing Memory Usage](tutorials/advanced/optimizing-memory-usage.md)

## When to Use Each Section

- **New to Transmog?** Start with [Getting Started](user/essentials/getting-started.md) and [Basic Concepts](user/essentials/basic-concepts.md)
- **Looking for specific method details?** Check the API reference
- **Want to contribute?** See the developer guides
- **Need to solve a specific problem?** Try the tutorials

## Related Documentation

Many topics have both a **user guide** and an **API reference**. For example:

| Topic | User Guide | API Reference |
|-------|------------|---------------|
| Processing | [Processing Overview](user/processing/processing-overview.md) | [Processor API](api/processor.md), [Process API](api/process.md) |
| Streaming | [Streaming](user/advanced/streaming.md) | [Process API](api/process.md) |
| File Processing | [File Processing](user/processing/file-processing.md) | [Process API](api/process.md) |
| Error Handling | [Error Handling](user/advanced/error-handling.md) | [Error API](api/error.md) |
| Metadata | [Metadata](user/processing/metadata.md) | [Core API](api/core.md) |
| Naming | [Naming](user/processing/naming.md) | [Naming API](api/naming.md) |
| IO Operations | [IO Operations](user/processing/io.md) | [IO API](api/io.md) |
| Results | [Output Formats](user/output/output-formats.md) | [ProcessingResult API](api/processing-result.md) |

The user guides focus on concepts and usage examples, while the API references provide comprehensive technical details.

## Navigation Tips

1. Use the table of contents on the left to browse by category
2. Use the search function to find specific topics
3. Look for cross-reference links between related documents
4. Check the "Related Documentation" sections at the bottom of each page

## Learning Path Suggestions

### For Beginners

1. [Getting Started](user/essentials/getting-started.md)
2. [Basic Concepts](user/essentials/basic-concepts.md)
3. [Transform Nested JSON Tutorial](tutorials/basic/transform-nested-json.md)
4. [Data Transformation Guide](user/processing/data-transformation.md)

### For Data Engineers

1. [Performance Optimization](user/advanced/performance-optimization.md)
2. [Streaming](user/advanced/streaming.md)
3. [Streaming Large Datasets Tutorial](tutorials/intermediate/streaming-large-datasets.md)
4. [Error Handling](user/advanced/error-handling.md)

### For Integration Developers

1. [Configuration](user/essentials/configuration.md)
2. [Output Formats](user/output/output-formats.md)
3. [Deterministic IDs](user/advanced/deterministic-ids.md)
4. [Customizing ID Generation Tutorial](tutorials/intermediate/customizing-id-generation.md)
