# Transmog Roadmap

This roadmap outlines the focused development trajectory for Transmog, a specialized tool for JSON normalization
and denormalization.

## Core Purpose

Transmog is designed to excel at a specific task: transforming complex nested JSON data into flat, structured
formats while preserving relationships. We aim to be the best-in-class solution for this specific niche
rather than a general-purpose ETL tool.

## Current Features

- Deep JSON flattening and normalization
- Nested array extraction to separate tables with relationship preservation
- Multiple output formats (JSON, CSV, Parquet)
- Error recovery mechanisms for handling problematic data
- Deterministic ID generation for consistent processing
- Memory-optimized processing for large datasets

## Short-term Goals (0-3 months)

1. **True Lazy Evaluation Pipeline**
   - Implement generator-based processing chain
   - Eliminate materialization of intermediate results
   - Maintain parent-child relationships in streaming mode
   - Optimize for constant memory usage regardless of input size

2. **Memory Optimization Refinements**
   - Record-by-record processing option for extreme memory constraints
   - Dynamic buffer sizing based on available system resources
   - Improved garbage collection hints and object lifecycle management

3. **Performance Profiling and Optimization**
   - Comprehensive benchmarking across various data shapes and sizes
   - Identify and optimize performance bottlenecks
   - Reduce CPU and memory overhead in core flattening operations

## Mid-term Goals (3-6 months)

1. **Enhanced Schema Management**
   - Improved type inference and handling
   - Schema evolution management for changing data structures
   - Optional schema enforcement with clear error reporting

2. **Integration Optimizations**
   - Streamlined integration with data processing frameworks (pandas, polars)
   - Better PyArrow interoperability and zero-copy operations where possible
   - Simplified usage patterns for common data workflows

3. **Specialized Array Handling**
   - Optimized processing for high-cardinality arrays
   - Smarter relationship tracking for deeply nested array structures
   - Options for selective array extraction based on path patterns

## Long-term Vision (6+ months)

1. **Advanced Transformation Capabilities**
   - User-defined transformation hooks at specific points in processing
   - Path-based filtering and transformation rules
   - Value normalization and standardization functions

2. **Enterprise-Ready Features**
   - Data lineage tracking for regulatory compliance
   - Detailed processing metrics and telemetry
   - Comprehensive documentation and usage patterns

## Development Philosophy

Rather than expanding scope indefinitely, we will focus on making Transmog the absolute best tool for
JSON transformation while ensuring it integrates seamlessly with other specialized tools in the data
ecosystem. We believe in doing one thing extremely well rather than many things adequately.

## Feature Requests and Contributions

Feature requests and contributions aligned with our core focus are welcome! Please feel free to open
issues or submit pull requests on GitHub to help Transmog excel at JSON transformation tasks.
