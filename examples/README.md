# Transmog Examples

This directory contains examples demonstrating how to use Transmog for data transformation.

## Directory Structure

- **essentials/** - Core functionality examples
  - Flattening basics
  - Configuration options
  - Processing modes

- **processing/** - Data processing examples
  - JSON processing
  - CSV processing
  - File handling

- **advanced/** - Advanced functionality
  - Streaming processing
  - Performance optimization
  - Error handling
  - Deterministic IDs

- **output/** - Output format examples
  - Different output formats
  - Formatting options
  - File writing options

- **data/** - Data files and example outputs
  - Input data files for examples
  - Output directory for example results

## Example Format

Each example follows a standard format:

```python
"""
Example Name: [Name]

Demonstrates: [Specific functionality demonstrated]

Related Documentation:
- [Link to related documentation section]

Learning Objectives:
- [What the user will learn from this example]
"""

# Standard imports
import transmog as tm

# Example code with clear, factual comments

def main():
    # Example implementation with clear output

if __name__ == "__main__":
    main()
```

## Running Examples

Most examples can be run directly:

```bash
python examples/essentials/flattening_basics.py
```

When examples generate output files, they are saved in the `examples/data/output` directory.

Optional features may require additional dependencies:

```bash
pip install transmog[all]  # Install all optional dependencies
```

## Example Categories

### Essentials

- **Flattening Basics**: Core functionality for flattening nested structures
- **Configuration**: Options for configuring the processor
- **Processing Modes**: Different modes for processing data

### Processing

- **JSON Processing**: Working with JSON data
- **CSV Processing**: Working with CSV data
- **File Handling**: Reading from and writing to files

### Advanced

- **Streaming Processing**: Memory-efficient streaming data processing
- **Error Handling**: Handling errors and recovery strategies
- **Performance Optimization**: Optimizing for different performance goals
- **Deterministic IDs**: Consistent ID generation across processing runs

### Output

- **Output Formats**: Different output format options

For detailed explanation of each example, see the inline comments in each example file.
