# Transmogrify Documentation

This directory contains the documentation for the Transmogrify project. The documentation is built using Sphinx with MyST Markdown.

## Documentation Structure

- **Getting Started**
  - `user/getting-started.md`: Quick start guide and overview
  - `installation.md`: Installation instructions

- **User Guide**
  - `user/flattening.md`: Guide to JSON flattening
  - `user/arrays.md`: Working with arrays
  - `user/error-handling.md`: Error handling and recovery strategies
  - `user/concurrency.md`: Parallel processing with Transmogrify
  
- **API Reference**
  - `api/processor.md`: Processor class documentation
  - `api/processing-result.md`: ProcessingResult class documentation

- **Developer Guide**
  - `../CONTRIBUTING.md`: Contributing guidelines (main file in repository root)
  - `dev/architecture.md`: Architectural overview
  - `dev/code-style.md`: Code style guidelines
  - `dev/extending.md`: How to extend Transmogrify
  - `dev/release-process.md`: Release process documentation
  - `dev/testing.md`: Testing guidelines

- **Examples**
  - `examples/basic.md`: Basic usage examples

## Files To Be Created

The following files still need to be created to complete the documentation:

### API Reference
- `api/recovery.md`: Error recovery strategies
- `api/core.md`: Core functionality
- `api/io.md`: Input/output utilities
- `api/config.md`: Configuration options

### Examples
- `examples/advanced.md`: Advanced usage examples
- `examples/large_datasets.md`: Handling large datasets
- `examples/concurrency.md`: Parallel processing examples

## Building the Documentation

To build the documentation:

1. Install the documentation dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Build the HTML documentation:
   ```bash
   sphinx-build -b html . _build/html
   ```

3. View the documentation:
   ```bash
   open _build/html/index.html  # On macOS
   # or
   start _build/html/index.html  # On Windows
   # or
   xdg-open _build/html/index.html  # On Linux
   ```

## Documentation Standards

- Use MyST Markdown for all documentation files
- Follow the Google style for code examples and docstrings
- Include practical examples in all guides
- Maintain consistent terminology throughout the documentation
- Use kebab-case for file names (e.g., `getting-started.md` instead of `getting_started.md`)
- Keep file paths and import statements up-to-date

## Contributing to Documentation

If you'd like to improve the documentation:

1. Check the "Files To Be Created" section above for high-priority documentation needs
2. Follow the contributing guidelines in the main CONTRIBUTING.md file
3. Submit a pull request with your documentation changes

Thank you for helping improve Transmogrify's documentation! 