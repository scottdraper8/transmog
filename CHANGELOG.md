# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Renamed project from Transmog to Transmog for brevity

### Added
- Streaming Parquet writing capability for memory-efficient processing of large datasets
- New `ParquetStreamingWriter` class that implements the `StreamingWriter` interface
- New `stream_to_parquet()` method in `ProcessingResult` class
- Row group management for optimal Parquet file organization
- Schema evolution handling for consistent Parquet schema across batches
- Example script for streaming Parquet usage

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

## [0.1.2.5] - 2023-07-15
### Added
- Support for custom time field names in metadata
- Advanced batch processing mode for handling large datasets

### Fixed
- Issue with nested array extraction in specific edge cases
- Memory management in streaming data extraction

## [0.1.2.4] - 2023-06-22
### Added 