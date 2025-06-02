# Transmog Examples Catalog

This page provides a comprehensive catalog of example code for the Transmog library. Each example
demonstrates specific features and use cases to help you understand how to use Transmog effectively.

## Examples by Category

### Data Processing

#### Basic Processing

| Example | Description | Difficulty |
|---------|-------------|------------|
| [Flattening Basics](../../examples/data_processing/basic/flattening_basics.py) | Core functionality for flattening nested structures | Basic |
| [Naming Example](../../examples/data_processing/basic/naming_example.py) | Field and table naming conventions | Basic |
| [Primitive Arrays](../../examples/data_processing/basic/primitive_arrays_example.py) | Handling arrays of primitive values | Basic |

#### File Formats

| Example | Description | Difficulty |
|---------|-------------|------------|
| [CSV Processing](../../examples/data_processing/file_formats/csv_processing.py) | Working with CSV data | Intermediate |
| [JSON Processing](../../examples/data_processing/file_formats/json_processing.py) | Working with JSON data | Intermediate |
| [Multiple Formats](../../examples/data_processing/file_formats/multiple_formats_example.py) | Converting between different formats | Intermediate |

#### Advanced Processing

| Example | Description | Difficulty |
|---------|-------------|------------|
| [Error Handling](../../examples/data_processing/advanced/error_handling.py) | Handling errors and recovery strategies | Advanced |
| [Performance Optimization](../../examples/data_processing/advanced/performance_optimization.py) | Optimizing for different performance goals | Advanced |
| [Streaming Processing](../../examples/data_processing/advanced/streaming_processing.py) | Memory-efficient streaming data processing | Advanced |

### Data Transformation

| Example | Description | Difficulty |
|---------|-------------|------------|
| [Data Cleanup](../../examples/data_transformation/basic/data_cleanup_example.py) | Cleaning and standardizing data | Intermediate |
| [Data Validation](../../examples/data_transformation/basic/data_validation_example.py) | Validating data during processing | Intermediate |
| [Deep Nesting](../../examples/data_transformation/advanced/deep_nesting_example.py) | Handling deeply nested structures | Advanced |
| [Deterministic IDs](../../examples/data_transformation/advanced/deterministic_ids.py) | Consistent ID generation | Advanced |

### Configuration

| Example | Description | Difficulty |
|---------|-------------|------------|
| [Basic Configuration](../../examples/configuration/basic/configuration.py) | Options for configuring the processor | Basic |

## Examples by Difficulty

### Basic Examples

- [Flattening Basics](../../examples/data_processing/basic/flattening_basics.py)
- [Naming Example](../../examples/data_processing/basic/naming_example.py)
- [Primitive Arrays](../../examples/data_processing/basic/primitive_arrays_example.py)
- [Basic Configuration](../../examples/configuration/basic/configuration.py)

### Intermediate Examples

- [CSV Processing](../../examples/data_processing/file_formats/csv_processing.py)
- [JSON Processing](../../examples/data_processing/file_formats/json_processing.py)
- [Multiple Formats](../../examples/data_processing/file_formats/multiple_formats_example.py)
- [Data Cleanup](../../examples/data_transformation/basic/data_cleanup_example.py)
- [Data Validation](../../examples/data_transformation/basic/data_validation_example.py)

### Advanced Examples

- [Error Handling](../../examples/data_processing/advanced/error_handling.py)
- [Performance Optimization](../../examples/data_processing/advanced/performance_optimization.py)
- [Streaming Processing](../../examples/data_processing/advanced/streaming_processing.py)
- [Deep Nesting](../../examples/data_transformation/advanced/deep_nesting_example.py)
- [Deterministic IDs](../../examples/data_transformation/advanced/deterministic_ids.py)

## Examples by Related Documentation

### Tutorials

| Tutorial | Related Example |
|----------|-----------------|
| [Transform Nested JSON](../tutorials/basic/transform-nested-json.md) | [Flattening Basics](../../examples/data_processing/basic/flattening_basics.py) |
| [Flatten and Normalize](../tutorials/basic/flatten-and-normalize.md) | [Flattening Basics](../../examples/data_processing/basic/flattening_basics.py) |
| [Streaming Large Datasets](../tutorials/intermediate/streaming-large-datasets.md) | [Streaming Processing](../../examples/data_processing/advanced/streaming_processing.py) |
| [Customizing ID Generation](../tutorials/intermediate/customizing-id-generation.md) | [Deterministic IDs](../../examples/data_transformation/advanced/deterministic_ids.py) |
| [Error Recovery Strategies](../tutorials/advanced/error-recovery-strategies.md) | [Error Handling](../../examples/data_processing/advanced/error_handling.py) |
| [Optimizing Memory Usage](../tutorials/advanced/optimizing-memory-usage.md) | [Performance Optimization](../../examples/data_processing/advanced/performance_optimization.py) |

### User Guides

| User Guide | Related Examples |
|------------|------------------|
| [Basic Concepts](../user/essentials/basic-concepts.md) | [Flattening Basics](../../examples/data_processing/basic/flattening_basics.py) |
| [Configuration](../user/essentials/configuration.md) | [Basic Configuration](../../examples/configuration/basic/configuration.py) |
| [JSON Handling](../user/processing/json-handling.md) | [JSON Processing](../../examples/data_processing/file_formats/json_processing.py) |
| [CSV Processing](../user/processing/csv-processing.md) | [CSV Processing](../../examples/data_processing/file_formats/csv_processing.py) |
| [Naming](../user/processing/naming.md) | [Naming Example](../../examples/data_processing/basic/naming_example.py) |
| [Array Handling](../user/processing/array-handling.md) | [Primitive Arrays](../../examples/data_processing/basic/primitive_arrays_example.py) |
| [Error Handling](../user/advanced/error-handling.md) | [Error Handling](../../examples/data_processing/advanced/error_handling.py) |
| [Performance Optimization](../user/advanced/performance-optimization.md) | [Performance Optimization](../../examples/data_processing/advanced/performance_optimization.py) |
| [Streaming](../user/advanced/streaming.md) | [Streaming Processing](../../examples/data_processing/advanced/streaming_processing.py) |
| [Deterministic IDs](../user/advanced/deterministic-ids.md) | [Deterministic IDs](../../examples/data_transformation/advanced/deterministic_ids.py) |

## Running the Examples

To run any example:

1. Clone the Transmog repository
2. Navigate to the repository root
3. Run the example using Python:

```bash
python examples/data_processing/basic/flattening_basics.py
```

Examples that generate output will create files in their respective output directories under `examples/output/`.

For examples requiring additional dependencies:

```bash
pip install transmog[all]  # Install all optional dependencies
```
