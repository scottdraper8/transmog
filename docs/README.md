# Transmog Documentation

This directory contains documentation for Transmog, a Python library for transforming nested JSON data into flat,
structured formats.

## Building the Documentation

Documentation is built using [Sphinx](https://www.sphinx-doc.org/) with the
[MyST Parser](https://myst-parser.readthedocs.io/) extension for Markdown support.

To build locally:

1. Install development dependencies:

   ```bash
   pip install -e ".[dev]"
   ```

2. Navigate to the docs directory:

   ```bash
   cd docs
   ```

3. Build the documentation:

   ```bash
   make html
   ```

4. View the documentation:

   ```bash
   open _build/html/index.html  # On macOS
   # Or
   xdg-open _build/html/index.html  # On Linux
   # Or
   start _build/html/index.html  # On Windows
   ```

## Documentation Structure

- `index.md`: Main entry point and overview
- `installation.md`: Installation instructions
- `user/`: User guides and tutorials
  - `getting-started.md`: Quick start guide
  - `configuration.md`: Configuration options
  - `flattening.md`: Flattening nested data
  - `arrays.md`: Working with arrays
  - `metadata.md`: Metadata features
  - `deterministic-ids.md`: Deterministic ID generation
  - `streaming.md`: Streaming processing
  - `error-handling.md`: Error handling strategies
  - `output-formats.md`: Output format options
- `api/`: API reference
  - `processor.md`: Processor API
  - `processing-result.md`: ProcessingResult API
  - `config.md`: Configuration API
- `dev/`: Developer documentation
  - `architecture.md`: System architecture
  - `extending.md`: Extending Transmog
  - `testing.md`: Testing guide

## Contributing to the Documentation

1. Make changes to the Markdown files
2. Build the documentation to preview changes
3. Submit a pull request

See the Contributing Guide in the repository root for more information.

## Documentation Conventions

- Use Markdown (.md) files for documentation
- Place code examples in fenced code blocks with language tags
- Use relative links for internal references
- Include section headers for major sections
- Follow PEP 8 style for Python code examples

## Current Version

This documentation reflects Transmog v0.1.2.5.
