# Transmog Documentation

This directory contains the documentation for Transmog, a Python library for transforming nested JSON
data into flat, structured formats.

## Documentation Structure

The documentation is organized into a hierarchical structure:

- **User Guides**
  - **Essentials** (`/user/essentials/`) - Basic concepts, getting started, configuration
  - **Processing** (`/user/processing/`) - Data transformation, file processing, specific formats
  - **Advanced** (`/user/advanced/`) - Streaming, optimization, error handling, IDs
  - **Output** (`/user/output/`) - Output formats and options

- **API Reference** (`/api/`) - Detailed API documentation

- **Developer Guides** (`/dev/`) - Information for contributors

- **Tutorials** - Step-by-step guides
  - **Basic** (`/tutorials/basic/`) - Introductory examples
  - **Intermediate** (`/tutorials/intermediate/`) - More complex use cases
  - **Advanced** (`/tutorials/advanced/`) - Advanced techniques and optimizations

## Development Setup

The easiest way to set up a development environment is to use the provided setup script:

```bash
python scripts/setup_dev.py
```

This script:
1. Creates and activates a virtual environment
2. Installs development dependencies
3. Sets up pre-commit hooks
4. Prepares the documentation environment
5. Creates required directories for benchmarks
6. Validates dependencies

## Building the Documentation

The documentation is built using [Sphinx](https://www.sphinx-doc.org/) with the MyST parser for Markdown support.

To build the documentation:

1. Install the required dependencies:

```bash
pip install -r docs/requirements.txt
```

2. Build the documentation:

```bash
cd docs
make html
```

3. View the built documentation:

```bash
open _build/html/index.html
```

## Contributing to Documentation

Please see the [CONTRIBUTING.md](https://github.com/scottdraper8/transmog/blob/main/CONTRIBUTING.md) file 
for guidelines on contributing to the documentation.

## Documentation Map

For a comprehensive overview of the documentation structure, please see the [Documentation Map](documentation_map.md).

## Next Steps for Documentation Improvement

1. Complete the content migration from the old structure to the new one
2. Validate all code examples against the current API
3. Apply consistent tone and style guidelines
4. Test all tutorials in a clean environment
5. Remove old documentation files once migration is complete
