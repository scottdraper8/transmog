# Installation Guide

This document covers installation options for Transmog.

## Basic Installation

Standard installation:

```bash
pip install transmog
```

Installs Transmog with all required dependencies:

- **orjson**: Fast JSON processing
- **pyarrow**: Parquet file support and PyArrow Table output
- **polars**: High-performance CSV processing for medium to large files
- **typing-extensions**: Python typing compatibility

## Installation Options

### Minimal Installation

For environments with strict requirements:

```bash
pip install transmog[minimal]
```

Installs Transmog without any external dependencies (limited functionality).

### Development Installation

For contributors and developers:

```bash
pip install transmog[dev]
```

Includes tools for testing, development, and linting.

#### Development Setup Script

The easiest way to set up a complete development environment is to use the provided setup script:

```bash
python scripts/setup_dev.py
```

This script automates the development setup process by:

1. Creating and activating a virtual environment
2. Checking Python version compatibility
3. Installing development dependencies
4. Setting up pre-commit and pre-push hooks
5. Running pre-commit hooks against all files
6. Setting up documentation dependencies and testing the build
7. Validating optional dependencies for performance optimization
8. Creating required directories for benchmarks and documentation

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

## System Requirements

- Python 3.9 or higher
- Compatible with Windows, macOS, and Linux

## Verifying Installation

To verify installation:

```python
import transmog

# Check version
print(transmog.__version__)

# Check available features (all should be True with standard installation)
from transmog.features import Features
print(f"PyArrow available: {Features.has_pyarrow()}")
print(f"Orjson available: {Features.has_orjson()}")
print(f"Polars available: {Features.has_polars()}")
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

After installation, see the [Getting Started Guide](./user/essentials/getting-started.md).
