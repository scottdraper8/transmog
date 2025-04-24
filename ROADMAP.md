# Transmogrify Roadmap

This roadmap outlines the planned development trajectory for Transmogrify.

## Current State (0.1.0)

Transmogrify currently provides the following capabilities:

- Core JSON flattening and normalization
- Nested array extraction with parent-child relationships
- Memory-efficient processing for large datasets
- Multiple output formats (JSON, Parquet, CSV)
- High-performance tiered I/O implementations
- Advanced configuration system

## Upcoming in 0.2.0

### Enhanced Input/Output Capabilities

- Support additional input formats:
  - CSV files with nested JSON columns
  - Parquet files with nested structures
- Add more output formats:
  - Avro format support
  - ORC format support
- Improve existing format writers:
  - Add chunked writing for large datasets
  - Add table schema inference and validation
- Add direct DataFrame output:
  - Zero-copy output to PyArrow Tables

### Performance Improvements

- Further optimize memory usage for very large datasets
- Add streaming processing mode (process while reading)
- Improve path handling for deep nested structures
- Provide examples for external parallelization approaches

### Developer Experience

- Add command-line interface for standalone usage
- Improve error messages and diagnostics
- Add more comprehensive validation options
- Implement schema inference and validation

## Long-term Vision (0.3.0 and beyond)

### Advanced Transformation Features

- Bidirectional transformation (rebuild nested structure from flattened tables)
- Schema-aware transformation with validation
- Custom transformation pipelines
- Advanced data type handling
- Integration with data validation frameworks
- Record grouping and windowing operations

### Ecosystem Integration

- Integration with popular data processing frameworks:
  - Enhance PyArrow integration with native APIs
- Transformation pipeline connectors
- API for custom format writers
- Efficient transformation hooks for streaming data

### Documentation and Examples

- Comprehensive documentation with practical examples
- Benchmark suite for performance monitoring
- Interactive examples and tutorials
- Examples of integration with parallelization frameworks
- Usage patterns for large-scale data processing

## Contributing

We welcome contributions to help realize this roadmap. If you're interested in working on any of these features, please see our [CONTRIBUTING.md](CONTRIBUTING.md) for guidance on how to get started. 