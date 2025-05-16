# Transmog Examples

This directory contains examples and sample code for using the Transmog library.

## Directory Structure

- **Basic Usage**
  - `simple_flatten.py` - How to flatten nested JSON structures
  - `config_example.py` - Configuring Transmog with the TransmogConfig API
  - `native_output_formats.py` - Working with different output formats
  - `conversion_modes_example.py` - Memory management with different conversion modes

- **Advanced Features**
  - `advanced_usage.py` - Complex transformation examples
  - `streaming_example.py` - Memory-efficient streaming processing
  - `partial_recovery_example.py` - Recovering data from problematic sources
  - `error_recovery_example.py` - Error handling strategies
  - `recovery_strategy_comparison.py` - Comparing different recovery strategies
  - `deterministic_id_examples.py` - Working with deterministic IDs
  - `parallel_processing.py` - Processing data in parallel
  - `performance_comparison.py` - Performance benchmarks and optimizations

- **Customization**
  - `abbreviation_example.py` - Field name abbreviation strategies
  - `custom_naming_strategies.py` - Custom field naming approaches

- **output/** - Directory for example outputs (ignored by git)

## Running the Examples

Most examples can be run directly:

```bash
python examples/simple_flatten.py
```

Some examples may require additional dependencies to be installed:

```bash
pip install transmog[all]  # Install all optional dependencies
```

## Example Categories

### Configuration and Setup

- `config_example.py` - Demonstrates the TransmogConfig system with its fluent API
- `deterministic_id_examples.py` - Shows how to configure deterministic ID generation

### Processing Data

- `simple_flatten.py` - Basic flattening of nested JSON structures
- `advanced_usage.py` - More complex flattening scenarios
- `streaming_example.py` - Processing large datasets with streaming

### Output Formats

- `native_output_formats.py` - Working with different output formats
- `conversion_modes_example.py` - Memory-efficient output handling

### Error Handling

- `partial_recovery_example.py` - Recovering data from problematic sources
- `error_recovery_example.py` - Different error handling strategies
- `recovery_strategy_comparison.py` - Comparing strategy performance

### Performance

- `parallel_processing.py` - Processing data in parallel
- `performance_comparison.py` - Performance benchmarks and optimizations

For detailed explanation of each example, see the inline comments in each example file.
