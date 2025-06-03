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

## Related Documentation

For a comprehensive guide to all examples and their documentation, see the [Examples Catalog](https://transmog.readthedocs.io/en/latest/user/examples.html).

### Data Processing Examples

| Example | Documentation |
|---------|---------------|
| [flattening_basics.py](data_processing/basic/flattening_basics.py) | [Transform Nested JSON Tutorial](https://transmog.readthedocs.io/en/latest/tutorials/basic/transform-nested-json.html), [Flatten and Normalize Tutorial](https://transmog.readthedocs.io/en/latest/tutorials/basic/flatten-and-normalize.html) |
| [naming_example.py](data_processing/basic/naming_example.py) | [Naming Guide](https://transmog.readthedocs.io/en/latest/user/processing/naming.html) |
| [primitive_arrays_example.py](data_processing/basic/primitive_arrays_example.py) | [Array Handling Guide](https://transmog.readthedocs.io/en/latest/user/processing/array-handling.html) |
| [csv_processing.py](data_processing/file_formats/csv_processing.py) | [CSV Processing Guide](https://transmog.readthedocs.io/en/latest/user/processing/csv-processing.html) |
| [json_processing.py](data_processing/file_formats/json_processing.py) | [JSON Handling Guide](https://transmog.readthedocs.io/en/latest/user/processing/json-handling.html) |
| [streaming_processing.py](data_processing/advanced/streaming_processing.py) | [Streaming Guide](https://transmog.readthedocs.io/en/latest/user/advanced/streaming.html), [Streaming Large Datasets Tutorial](https://transmog.readthedocs.io/en/latest/tutorials/intermediate/streaming-large-datasets.html) |
| [error_handling.py](data_processing/advanced/error_handling.py) | [Error Handling Guide](https://transmog.readthedocs.io/en/latest/user/advanced/error-handling.html), [Error Recovery Strategies Tutorial](https://transmog.readthedocs.io/en/latest/tutorials/advanced/error-recovery-strategies.html) |
| [performance_optimization.py](data_processing/advanced/performance_optimization.py) | [Performance Optimization Guide](https://transmog.readthedocs.io/en/latest/user/advanced/performance-optimization.html), [Optimizing Memory Usage Tutorial](https://transmog.readthedocs.io/en/latest/tutorials/advanced/optimizing-memory-usage.html) |

### Data Transformation Examples

| Example | Documentation |
|---------|---------------|
| [data_cleanup_example.py](data_transformation/basic/data_cleanup_example.py) | [Data Transformation Guide](https://transmog.readthedocs.io/en/latest/user/processing/data-transformation.html) |
| [data_validation_example.py](data_transformation/basic/data_validation_example.py) | [Data Transformation Guide](https://transmog.readthedocs.io/en/latest/user/processing/data-transformation.html) |
| [deep_nesting_example.py](data_transformation/advanced/deep_nesting_example.py) | [Data Transformation Guide](https://transmog.readthedocs.io/en/latest/user/processing/data-transformation.html) |
| [deterministic_ids.py](data_transformation/advanced/deterministic_ids.py) | [Deterministic IDs Guide](https://transmog.readthedocs.io/en/latest/user/advanced/deterministic-ids.html), [Customizing ID Generation Tutorial](https://transmog.readthedocs.io/en/latest/tutorials/intermediate/customizing-id-generation.html) |

### Configuration Examples

| Example | Documentation |
|---------|---------------|
| [configuration.py](configuration/basic/configuration.py) | [Configuration Guide](https://transmog.readthedocs.io/en/latest/user/essentials/configuration.html) |

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
- [Link to related tutorial]
- [Link to related API reference]

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

### 📊 Performance Examples

Located in `performance/`:

- **csv_reader_benchmark.py**: Benchmarks different CSV reader implementations
  - Compares adaptive vs native reader performance
  - Tests across different file sizes
  - Shows when to use each reader
  - Demonstrates environment variable optimization

## 🚀 Getting Started
