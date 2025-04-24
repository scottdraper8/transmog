# Installation Guide

This guide covers the different ways to install Transmogrify.

## Basic Installation

For most users, the standard installation is recommended:

```bash
pip install transmogrify
```

This installs Transmogrify with its required dependencies.

## Installation Options

### Minimal Installation

For environments with strict dependency requirements:

```bash
pip install transmogrify[minimal]
```

This installs Transmogrify with minimal dependencies.

### Development Installation

For contributors and developers:

```bash
pip install transmogrify[dev]
```

This includes additional tools for testing, development, and linting.

### Full Installation

To install all optional dependencies:

```bash
pip install transmogrify[all]
```

## Installing from Source

To install the latest development version:

```bash
git clone https://github.com/scottdraper8/transmogrify.git
cd transmogrify
pip install -e .
```

## Optional Dependencies

Transmogrify can use these optional packages:

- **pyarrow**: For Parquet file support
- **orjson**: For faster JSON processing

Install any of these directly:

```bash
pip install pyarrow
```

## System Requirements

- Python 3.7 or higher
- Compatible with Windows, macOS, and Linux

## Verifying Installation

To verify your installation:

```python
import transmogrify

# Check version
print(transmogrify.__version__)
```

## Troubleshooting

### Missing Compiler Tools

If you encounter build errors, you may need compiler tools:

**Windows:**
- Install Visual C++ Build Tools

**macOS:**
- Install command-line tools: `xcode-select --install`

**Linux:**
- Install required packages: `apt-get install build-essential python3-dev`

### Version Conflicts

If you experience dependency conflicts:

```bash
pip install --upgrade transmogrify --no-deps
pip install -r <(pip freeze | grep -v transmogrify)
```

### Memory Errors During Installation

For memory-limited environments:

```bash
pip install --no-cache-dir transmogrify
```

## Next Steps

After installation, refer to the [Getting Started Guide](user/getting-started.md) for usage information. 