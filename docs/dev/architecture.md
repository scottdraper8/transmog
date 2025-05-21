# Architecture Overview

This document provides an overview of Transmog's internal architecture to help contributors understand
how the components fit together.

## High-Level Architecture

Transmog follows a modular design with several core components:

```{mermaid}
graph LR
    A[Input JSON/Dict] --> B[Processor]
    B --> C[Core Flattener]
    B --> D[Core Extractor]
    C --> E[Processing Strategy]
    D --> E
    E --> F[Processing Result]
    F --> G[Various Output Formats]
```

## Core Components

### Processor

The `Processor` class is the main entry point for users. It orchestrates the transformation process by:

1. Receiving input data
2. Selecting appropriate processing strategy
3. Applying the core transformation logic
4. Building and returning the processing result

Key files:

- `src/transmog/process/__init__.py` - Main Processor implementation
- `src/transmog/process/strategy.py` - Processing strategies
- `src/transmog/config/` - Configuration options

### Core Transformation

The core transformation system handles flattening nested structures and extracting arrays:

- **Flattener**: Converts nested objects to flat structures with path-based keys
- **Extractor**: Identifies and extracts arrays to separate tables
- **Hierarchy**: Processes the parent-child relationships between tables

Key files:

- `src/transmog/core/flattener.py` - Flattening implementation
- `src/transmog/core/extractor.py` - Array extraction logic
- `src/transmog/core/hierarchy.py` - Parent-child relationship management
- `src/transmog/core/metadata.py` - Metadata generation and management

### Processing Strategies

Different strategies for handling various data sources and processing approaches:

- `InMemoryStrategy`: For direct in-memory processing
- `FileStrategy`: For processing from files
- `BatchStrategy`: For batch processing
- `ChunkedStrategy`: For processing large datasets in chunks
- `CSVStrategy`: Specialized for CSV processing

Key files:

- `src/transmog/process/strategy.py` - Strategy implementations
- `src/transmog/process/data_iterators.py` - Data iteration utilities
- `src/transmog/process/file_handling.py` - File processing utilities

### Result Management

The `ProcessingResult` class manages the output data and provides methods to:

- Access transformed data
- Convert to different formats
- Handle errors and statistics
- Stream results for large datasets

Key files:

- `src/transmog/process/result.py` - Result implementation
- `src/transmog/io/` - Output format handlers

### I/O System

The I/O system handles reading input data and writing output in various formats:

- JSON (via various backends: orjson, stdlib)
- CSV
- Parquet (via PyArrow)

Key files:

- `src/transmog/io/readers/json.py` - JSON reading
- `src/transmog/io/readers/csv.py` - CSV reading
- `src/transmog/io/writers/json.py` - JSON writing
- `src/transmog/io/writers/csv.py` - CSV writing
- `src/transmog/io/writers/parquet.py` - Parquet writing
- `src/transmog/io/formats.py` - Format detection and handling

### Error Handling

The error handling system provides configurable strategies for dealing with errors:

- Strict mode (fail fast)
- Skip mode (ignore errors)
- Custom recovery strategies

Key files:

- `src/transmog/error/exceptions.py` - Error definitions
- `src/transmog/error/recovery.py` - Recovery strategies
- `src/transmog/error/handling.py` - Error context and handling utilities

## Data Flow

A typical data flow through the system works as follows:

1. **Input Phase**:
   - Raw JSON/Dict data enters the system through the `Processor`
   - Configuration options are applied
   - Appropriate processing strategy is selected

2. **Flattening Phase**:
   - Nested structures are flattened using the `flatten_json` function
   - Path-based keys are generated for each value
   - Values are processed according to configuration

3. **Array Extraction Phase**:
   - Arrays are identified and extracted using `extract_arrays`
   - Child tables are created for array items
   - Parent-child relationships are established

4. **Metadata Phase**:
   - Record IDs are generated (random or deterministic)
   - Parent references are established
   - Timestamps and other metadata are added

5. **Output Phase**:
   - Data is assembled into the `ProcessingResult`
   - Result is converted to requested format
   - Data is returned or written to files

## Extension Points

Transmog provides several extension points for customization:

1. **Custom Recovery Strategies**:

   ```python
   from transmog.error import RecoveryStrategy

   class MyRecoveryStrategy(RecoveryStrategy):
       def recover(self, error, context=None):
           # Custom recovery logic
           return recovery_result
   ```

2. **Custom ID Generation**:

   ```python
   def custom_id_strategy(record):
       # Generate ID based on record contents
       return f"CUSTOM-{record.get('id', 'unknown')}"

   # Use with processor
   processor = Processor.with_custom_id_generation(custom_id_strategy)
   ```

3. **Output Format Extensions**:

   ```python
   from transmog.io import DataWriter, register_writer

   class MyCustomWriter(DataWriter):
       def write(self, data, destination):
           # Custom writing logic
           pass

   # Register the writer
   register_writer("custom-format", MyCustomWriter)
   ```

4. **Processing Configuration**:

   ```python
   from transmog import TransmogConfig

   # Create custom configuration
   config = (
       TransmogConfig.default()
       .with_naming(separator=".", deep_nesting_threshold=4)
       .with_processing(cast_to_string=True, skip_null=True)
       .with_metadata(id_field="custom_id")
   )
   ```

## Performance Considerations

Transmog includes several performance techniques:

1. **Caching**:
   - Value processing results are cached
   - Configuration options for cache size and behavior

2. **Chunked Processing**:
   - Data is processed in configurable chunks
   - Memory usage is controlled for large datasets

3. **Streaming API**:
   - Stream-based processing for memory efficiency
   - Direct output to files without intermediate structures

4. **Optional PyArrow Integration**:
   - Efficient columnar data format
   - Zero-copy operations where possible

## Testing Approach

The codebase uses a multi-faceted testing approach:

1. **Unit Tests**: Testing individual components in isolation
2. **Integration Tests**: Testing interactions between components
3. **Performance Tests**: Measuring and comparing performance characteristics
4. **Property-Based Tests**: Using random inputs to identify edge cases

When contributing new code, please follow this testing approach for consistency.

## Output Formats

Transmog supports multiple output formats:

- JSON (objects and strings)
- CSV
- Parquet (via PyArrow)
- PyArrow Table
- Python Dictionary
