# Transmog Debugger

A unified debugging tool for the Transmog package to help diagnose issues with the flattener, processor, and other components.

## Overview

The transmog_debugger.py script provides several debugging capabilities:

- Test and verify the flattener with various configurations
- Debug the processor's handling of different data structures
- Compare original vs. fixed implementations
- Test custom data handling scenarios

## Usage

```bash
python tests/transmog_debugger.py [component] [options]
```

### Components

- `flatten` - Debug the flattener component
- `process` - Debug the processor component
- `compare` - Compare original vs. fixed implementations

### Options

- `--verbose` - Show detailed debug output
- `--compare` - Compare original vs. fixed implementations
- `--in-place` - Test in-place modifications
- `--data-type TYPE` - Specify the test data type (simple, array, scalar, complex, circular)

## Examples

### Basic Flattener Debugging

```bash
# Test the flattener with simple data
python tests/transmog_debugger.py flatten

# Test with complex data structure
python tests/transmog_debugger.py flatten --data-type complex

# Test with verbose output
python tests/transmog_debugger.py flatten --verbose
```

### Processor Debugging

```bash
# Test the processor with simple data
python tests/transmog_debugger.py process

# Test with array-containing data
python tests/transmog_debugger.py process --data-type array
```

### Implementation Comparison

```bash
# Compare implementations with simple data
python tests/transmog_debugger.py compare

# Compare implementations with complex data
python tests/transmog_debugger.py compare --data-type complex --verbose
```

## Test Data Types

The debugger includes several predefined test data types:

- `simple` - Basic nested structure
- `array` - Structure with array of objects
- `scalar` - Structure with various scalar types
- `complex` - Complex nested structure with arrays and objects
- `circular` - Structure with circular references

## Future Extensions

To extend the debugger with new components:

1. Add a new debug function in the main script (following the pattern of existing debug functions)
2. Add the component to the argument parser choices
3. Update the main function to call your new debug function

## Using the Debugger for Issue Resolution

When encountering testing failures:

1. Run the debugger with the appropriate component and data type
2. Compare the actual output with the expected output in tests
3. Use the `--compare` flag to check if the fixed implementation resolves the issue
4. Use the `--verbose` flag to trace execution flow and identify where failures occur

## Contributing

When adding custom test data or debugging features:

1. Use the `get_test_data()` function to add new test data types
2. Follow the established pattern for debug functions
3. Add clear output formatting for better readability 