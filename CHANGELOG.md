# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Renamed project from Transmog to Transmog for brevity

## [0.1.0] - 2023-11-15

### Added
- Initial release of Transmog
- Core flattening functionality with customizable delimiters
- Array extraction with parent-child relationship preservation
- Multiple output formats (JSON, CSV, Parquet)
- Memory-optimized processing for large datasets
- Comprehensive error handling and recovery strategies
- Deterministic ID generation for data consistency

### Added

- Core JSON/dict flattening functionality with customizable separators
- Nested array extraction to child tables with parent-child relationships
- Multiple output formats (Python dicts, JSON objects, PyArrow Tables)
- Bytes serialization for JSON, CSV, and Parquet
- File output support for JSON, CSV, and Parquet formats
- Flexible configuration system with global and per-instance settings
- Error handling with customizable recovery strategies
- Memory optimization for large datasets with chunked processing
- CSV input processing with type inference
- Table and field name abbreviation options 