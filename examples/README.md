# Transmog Examples

This directory contains example code demonstrating various features and use cases of Transmog.

## Directory Structure

```text
examples/
├── data_processing/
│   ├── basic/              # Basic data processing examples
│   │   ├── flattening_basics.py
│   │   ├── naming_example.py
│   │   └── primitive_arrays_example.py
│   ├── file_formats/       # File format specific examples
│   │   ├── csv_processing.py
│   │   ├── json_processing.py
│   │   └── multiple_formats.py
│   ├── advanced/           # Advanced processing examples
│   │   ├── streaming_processing.py
│   │   ├── error_handling.py
│   │   └── performance_optimization.py
│   └── data/              # Example data files
├── data_transformation/
│   ├── basic/             # Basic transformation examples
│   │   ├── data_cleanup_example.py
│   │   └── data_validation_example.py
│   ├── advanced/          # Advanced transformation examples
│   │   ├── deep_nesting_example.py
│   │   └── deterministic_ids.py
│   └── data/             # Example data files
├── configuration/
│   ├── basic/            # Basic configuration examples
│   │   └── config_examples.py
│   ├── advanced/         # Advanced configuration examples
│   │   └── optimization.py
│   └── data/            # Example data files
└── output/              # Example output directory
    ├── data_processing/  # Outputs from data processing examples
    │   ├── basic/
    │   ├── file_formats/
    │   └── advanced/
    ├── data_transformation/  # Outputs from transformation examples
    │   ├── basic/
    │   └── advanced/
    └── configuration/    # Outputs from configuration examples
        ├── basic/
        └── advanced/
```

## Output Directory

Each example writes its output to a dedicated subdirectory in the `output/` directory,
organized by example category and type:

- `data_processing/`: Outputs from data processing examples
  - `basic/`: Basic processing example outputs
  - `file_formats/`: File format specific outputs
  - `advanced/`: Advanced processing outputs
- `data_transformation/`: Outputs from transformation examples
  - `basic/`: Basic transformation outputs
  - `advanced/`: Advanced transformation outputs
- `configuration/`: Outputs from configuration examples
  - `basic/`: Basic configuration outputs
  - `advanced/`: Advanced configuration outputs

Each example's output directory contains the following format subdirectories:

- `json/`: JSON output files
- `csv/`: CSV output files
- `parquet/`: Parquet output files
- `streaming/`: Streaming output files

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

## Array Processing

### Primitive Arrays

Transmog now processes arrays of primitive values (strings, numbers, booleans) as child tables,
similar to how it processes arrays of objects. Each primitive value in an array becomes a record
in a child table with a `value` field containing the primitive value.

Key points about primitive array handling:

- Arrays of primitive values are extracted to their own child tables
- By default, null values in arrays are skipped (controlled by the `skip_null` parameter)
- To include null values, set `skip_null=False` when processing
- Each primitive value is stored in a `value` field in the resulting record
- Parent-child relationships are maintained with appropriate metadata

Example:

```python
import transmog as tm

# Create data with primitive arrays
data = {
    "id": 123,
    "name": "Example",
    "tags": ["red", "green", "blue"],
    "scores": [95, 87, 92]
}

# Process the data
processor = tm.Processor()
result = processor.process(data, entity_name="example")

# Access the primitive array tables
tags_table = result.get_child_table("example_tags")
scores_table = result.get_child_table("example_scores")

# Each tag is now a record with a "value" field
for tag in tags_table:
    print(f"Tag: {tag['value']}")
```

See the [primitive_arrays_example.py](basic/primitive_arrays_example.py) for a complete working example.
