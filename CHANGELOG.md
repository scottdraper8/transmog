# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.1.0] - 2023-01-15

### Added

- Initial release of Transmogrify
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