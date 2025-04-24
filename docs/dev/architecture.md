# Architecture Overview

This document provides an overview of Transmogrify's internal architecture to help contributors understand how the components fit together.

## High-Level Architecture

Transmogrify follows a modular design with several core components:

```{mermaid}
graph TD
    A[Input JSON/Dict] --> B[Transformer]
    B --> C[Path Resolver]
    B --> D[Value Processors]
    C --> E[Result Builder]
    D --> E
    E --> F[Output Formatter]
    F --> G[Various Output Formats]
```

## Core Components

### Transformer

The `Transformer` class is the main entry point for users. It orchestrates the transformation process by:

1. Receiving input data
2. Applying path resolution to navigate nested structures
3. Processing values through transformation functions
4. Building the result object

Key files:
- `src/transmogrify/transformer.py` - Main implementation
- `src/transmogrify/config.py` - Configuration options

### Path Resolution

The path resolution system handles the traversal of nested data structures based on path expressions:

- Simple paths: `user.profile.name`
- Wildcard paths: `users.*.address`
- Index access: `orders.0.items`
- Advanced selectors: `data[?(@.active==true)].name` (JSONPath-like)

Key files:
- `src/transmogrify/path.py` - Path expression parsing
- `src/transmogrify/resolver.py` - Path resolution logic

### Value Processing

Value processors transform raw data values during extraction. This system allows for:

- Type conversion
- Formatting
- Validation
- Custom transformations

Key files:
- `src/transmogrify/processors.py` - Built-in processors
- `src/transmogrify/hooks.py` - Extension points for custom processors

### Result Management

The `TransformResult` class manages the output data and provides methods to:

- Access transformed data
- Convert to different formats
- Handle errors and statistics
- Stream results for large datasets

Key files:
- `src/transmogrify/result.py` - Result implementation
- `src/transmogrify/io/` - Output format handlers

### I/O System

The I/O system handles reading input data and writing output in various formats:

- JSON (via various backends: orjson, stdlib)
- CSV
- Parquet (via PyArrow)

Key files:
- `src/transmogrify/io/json.py` - JSON handling
- `src/transmogrify/io/csv.py` - CSV handling
- `src/transmogrify/io/parquet.py` - Parquet handling

### Error Handling

The error handling system provides configurable strategies for dealing with errors:

- Strict mode (fail fast)
- Skip mode (ignore errors)
- Custom recovery strategies

Key files:
- `src/transmogrify/errors.py` - Error definitions
- `src/transmogrify/recovery.py` - Recovery strategies

## Data Flow

A typical data flow through the system works as follows:

1. **Input Phase**: 
   - Raw JSON/Dict data enters the system
   - Configuration options are applied

2. **Resolution Phase**: 
   - Path expressions are resolved against the input data
   - Nested paths are navigated to extract values

3. **Processing Phase**: 
   - Extracted values are passed through processors
   - Transformations are applied (type conversion, formatting, etc.)

4. **Collection Phase**: 
   - Processed key-value pairs are collected into the result
   - Metadata is attached (if enabled)

5. **Output Phase**: 
   - Result is formatted into the requested output format
   - Data is returned or written to files

## Extension Points

Transmogrify provides several extension points for customization:

1. **Custom Value Processors**: 
   ```python
   def my_processor(value, path=None, context=None):
       # Custom transformation logic
       return transformed_value
   ```

2. **Custom Path Expressions**: 
   ```python
   class MyPathResolver(BasePathResolver):
       def resolve(self, data, path_expression):
           # Custom path resolution logic
           return resolved_values
   ```

3. **Custom Output Formats**: 
   ```python
   class MyOutputFormatter(BaseFormatter):
       def format(self, data):
           # Custom formatting logic
           return formatted_data
   ```

4. **Custom Recovery Strategies**: 
   ```python
   class MyRecoveryStrategy(RecoveryStrategy):
       def recover(self, error, context=None):
           # Custom recovery logic
           return recovery_result
   ```

## Performance Considerations

Transmogrify includes several performance techniques:

1. **Implementation Approaches**: 
   - Specific optimizations for common cases
   - Alternative implementations for less common scenarios

2. **Evaluation Patterns**: 
   - Path expressions are evaluated when needed
   - Results are generated on-demand when possible

3. **Chunked Processing**: 
   - Data processing in chunks to manage memory usage
   - Incremental result building for large datasets

4. **Result Reuse**: 
   - Path resolution results may be cached
   - Compiled path expressions can be reused

## Testing Approach

The codebase uses a multi-faceted testing approach:

1. **Unit Tests**: Testing individual components in isolation
2. **Integration Tests**: Testing interactions between components
3. **Performance Tests**: Measuring and comparing performance characteristics
4. **Property-Based Tests**: Using random inputs to identify edge cases

When contributing new code, please follow this testing approach for consistency.

### Output Formats

- JSON (objects and strings)
- CSV
- Parquet (via PyArrow)
- PyArrow Table
- Python Dictionary 