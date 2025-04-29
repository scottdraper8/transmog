# Transmog Examples

This directory contains examples and sample code for using the Transmog library.

## Directory Structure

- **basic/** - Basic usage examples for getting started with Transmog
  - `simple_flatten.py` - How to flatten nested JSON structures
  - `config_example.py` - Configuration options for the Processor
  - `native_output_formats.py` - Working with different output formats

- **advanced/** - Advanced usage patterns for more complex scenarios
  - `advanced_usage.py` - Complex transformation examples
  - `abbreviation_example.py` - Field name abbreviation strategies
  - `error_recovery_example.py` - Error handling and recovery strategies
  - `parallel_processing.py` - Processing data in parallel
  - `performance_comparison.py` - Performance benchmarks and optimizations
  - `deterministic_id_examples.py` - Working with deterministic IDs
  - `custom_naming_strategies.py` - Custom field naming approaches
  - `abbreviation_test.py` - Testing abbreviation strategies

- **data/** - Examples for working with different data formats
  - `data_cleanup_example.py` - Cleaning and preprocessing data
  - `data_transform_example.py` - Data transformation techniques
  - `data_aggregation_example.py` - Aggregating data
  - `csv_to_json_example.py` - Converting between CSV and JSON
  - `data_validation_example.py` - Validating data during processing
  - `multiple_formats_example.py` - Working with multiple data formats

- **output/** - Directory for example outputs (ignored by git)

## Running the Examples

Most examples can be run directly:

```bash
python examples/basic/simple_flatten.py
```

Some examples may require additional dependencies to be installed:

```bash
pip install transmog[all]  # Install all optional dependencies
```

For detailed explanation of each example, see the inline comments in each example file. 