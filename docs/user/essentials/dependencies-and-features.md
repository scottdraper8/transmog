# Dependencies and Features Management

This document explains how Transmog handles optional dependencies and detects available features at runtime.

## Part 1: Understanding Optional Dependencies

Transmog is designed with a modular architecture that supports optional dependencies for extended functionality.

### Dependency Manager

Transmog uses a dependency management system to handle optional dependencies:

```python
from transmog.dependencies import DependencyManager
```

### Checking Feature Availability

```python
# Check if a feature is available
parquet_available = DependencyManager.check_feature_available("parquet")
```

### Required Packages for Features

```python
# Get the required packages for a feature
packages = DependencyManager.get_required_packages_for_feature("parquet")
# Returns ["pyarrow"]
```

### Feature Mapping

The dependency manager maps features to their required packages:

| Feature | Required Packages |
|---------|------------------|
| `parquet` | `pyarrow` |
| `pyarrow` | `pyarrow` |
| `fast_json` | `orjson` |

### Importing Optional Modules

```python
# Import an optional module
pyarrow = DependencyManager.import_optional_module("pyarrow")
```

### Checking Package Installation

```python
# Check if a package is installed
is_installed = DependencyManager.check_package_installed("pyarrow")
```

## Part 2: Working with Runtime Features

The `Features` class provides a higher-level interface for checking available functionality.

### Features System

```python
from transmog.features import Features
```

### Checking Available Features

```python
# Check if PyArrow is available
if Features.has_pyarrow():
    # Use PyArrow-dependent features
    result.to_pyarrow_tables()
    result.to_parquet_bytes()
    result.write_all_parquet("output_dir")
```

### Available Optional Features

#### PyArrow Features

PyArrow enables:

- Parquet file output
- PyArrow Table output
- Efficient memory handling for large datasets

```python
# Check PyArrow availability
has_pyarrow = Features.has_pyarrow()
```

#### Fast JSON Processing

orjson enables:

- Faster JSON parsing and serialization
- Memory-efficient JSON handling

```python
# Check orjson availability
has_orjson = Features.has_orjson()
```

### Feature Fallbacks

When optional features are not available, Transmog falls back to built-in alternatives:

- Without PyArrow: Uses built-in formats (JSON, CSV)
- Without orjson: Uses standard json module

## Part 3: Handling Missing Dependencies

### Error Handling

When a feature requiring an optional dependency is used, but the dependency is not installed:

```python
from transmog.error import MissingDependencyError

try:
    # Code that uses an optional dependency
    processor.process_to_format(data, "parquet", "output.parquet")
except MissingDependencyError as e:
    print(f"Missing dependency: {e}")
    # Handle the missing dependency
```

### Dynamic Feature Detection

Features are detected at runtime, allowing for:

- Dynamic adaptation to available dependencies
- Graceful degradation when dependencies are missing
- Clear error messages for unavailable features

```python
try:
    # Try using a feature that requires PyArrow
    result.to_parquet_bytes()
except ImportError:
    # Fall back to another format
    json_bytes = result.to_json_bytes()
```

### Installation for Additional Features

To enable optional features, install the required packages:

```bash
# For PyArrow and Parquet support
pip install pyarrow

# For faster JSON processing
pip install orjson
```

## Part 4: Best Practices

### Checking Before Using

Always check feature availability before using optional features:

```python
from transmog.features import Features

# Check and use optional features conditionally
if Features.has_pyarrow():
    # Use PyArrow/Parquet features
    result.write_all_parquet("output/data")
else:
    # Fall back to a built-in format
    result.write_all_json("output/data")
```

### Providing Helpful Error Messages

When developing applications that use Transmog, consider providing helpful error messages:

```python
try:
    result.to_parquet_bytes()
except MissingDependencyError:
    print("PyArrow is required for Parquet output. Please install with 'pip install pyarrow'.")
    # Offer alternative or exit gracefully
```

### Installing Feature Groups

For production use, consider installing all dependencies for a feature group:

```bash
# For all data science features
pip install transmog[datascience]  # Includes pyarrow

# For all performance features
pip install transmog[performance]  # Includes orjson, pyarrow
```
