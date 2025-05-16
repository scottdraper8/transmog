# Installation Guide

This document covers installation options for Transmog.

## Basic Installation

Standard installation:

```bash
pip install transmog
```

Installs Transmog with required dependencies.

## Installation Options

### Minimal Installation

For environments with strict requirements:

```bash
pip install transmog[minimal]
```

Installs Transmog with minimal dependencies.

### Development Installation

For contributors and developers:

```bash
pip install transmog[dev]
```

Includes tools for testing, development, and linting.

### Full Installation

Install all optional dependencies:

```bash
pip install transmog[all]
```

## Installing from Source

To install the development version:

```bash
git clone https://github.com/scottdraper8/transmog.git
cd transmog
pip install -e .
```

For development with dev dependencies:

```bash
pip install -e ".[dev]"
```

## Optional Dependencies

Optional packages:

- **pyarrow**: For Parquet file support and PyArrow Table output
- **orjson**: For faster JSON processing
- **pandas**: For CSV processing with pandas

To install optional dependencies:

```bash
pip install pyarrow orjson pandas
```

## System Requirements

- Python 3.9 or higher
- Compatible with Windows, macOS, and Linux

## Verifying Installation

To verify installation:

```python
import transmog

# Check version
print(transmog.__version__)

# Check available features
from transmog.features import Features
print(f"PyArrow available: {Features.has_pyarrow()}")
print(f"Orjson available: {Features.has_orjson()}")
print(f"Pandas available: {Features.has_pandas()}")
```

## Troubleshooting

### Missing Optional Dependencies

Check dependencies for features:

```python
from transmog.dependencies import DependencyManager

# Check which features require which packages
DependencyManager.get_required_packages_for_feature("parquet")  # Returns ["pyarrow"]
DependencyManager.check_feature_available("parquet")  # Returns True/False
```

### Missing Compiler Tools

If you encounter build errors:

**Windows:**

- Install Visual C++ Build Tools

**macOS:**

- Install command-line tools: `xcode-select --install`

**Linux:**

- Install required packages: `apt-get install build-essential python3-dev`

### Version Conflicts

For dependency conflicts:

```bash
pip install --upgrade transmog --no-deps
pip install -r <(pip freeze | grep -v transmog)
```

### Memory Errors During Installation

For memory-limited environments:

```bash
pip install --no-cache-dir transmog
```

## Next Steps

After installation, see the [Getting Started Guide](user/getting-started.md).
