# Contributing to Transmogrify

Thank you for your interest in contributing to Transmogrify! This document provides guidelines and instructions for contributing.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** to your local machine
3. **Create a feature branch**: `git checkout -b feature/your-feature-name`
4. **Install development dependencies**: `pip install -e ".[dev]"`

## Development Workflow

1. **Check the roadmap**: See [ROADMAP.md](ROADMAP.md) for planned features
2. **Look at open issues**: Find something that needs attention
3. **Make your changes**: Implement your feature or fix
4. **Add tests**: Ensure your code is tested
5. **Run tests**: `pytest tests/`
6. **Update documentation**: Make sure docs reflect your changes

## Code Style

- Follow PEP 8 guidelines
- Use type hints where appropriate
- Write docstrings for functions and classes (Google style)
- Formatting and linting is automatically handled by pre-commit hooks when you commit

## Pull Request Process

1. **Update your fork**: Merge any changes from the main repository
2. **Create a pull request**: From your feature branch to the main repository
3. **Describe your changes**: Provide a clear description of what your PR does
4. **Reference issues**: Link to any related issues
5. **Be responsive**: Address review comments promptly

## Feature Implementation Guidelines

When implementing features, please consider:

1. **Memory efficiency**: Transmogrify works with large datasets
2. **Error handling**: Provide meaningful error messages and recovery options
3. **Backward compatibility**: Avoid breaking existing APIs if possible
4. **Documentation**: Update relevant documentation with examples

## Focus Areas

These areas would particularly benefit from contributions:

1. Enhanced testing and performance benchmarks
2. Documentation and tutorials
3. Performance optimizations and memory efficiency
4. Industry-specific transformation patterns
5. Connector development for popular file formats and databases

## Questions?

If you have questions or need help, please open an issue with the "question" label.

## Pre-commit hooks

We use pre-commit hooks to ensure code quality and consistency. To set up pre-commit hooks:

1. Install pre-commit:
   ```bash
   pip install pre-commit
   ```

2. Set up the git hooks:
   ```bash
   pre-commit install
   ```

3. (Optional) Run against all files:
   ```bash
   pre-commit run --all-files
   ```

The pre-commit hooks will run automatically when you commit changes. The hooks include:
- Code formatting and linting (ruff)
- Type checking (mypy)
- Documentation coverage checking (interrogate)
- Security checks (bandit, safety)
- Prevention of committing directly to main branch

If a hook fails, the commit will be aborted. Fix the issues and try committing again.

## Documentation

### Building Documentation

We use Sphinx with MyST for documentation:

```bash
# Install documentation dependencies
pip install -e ".[docs]"

# Build the documentation
cd docs
sphinx-build -b html . _build/html

# View the documentation in your browser
open _build/html/index.html  # On macOS
# or
start _build/html/index.html  # On Windows
# or
xdg-open _build/html/index.html  # On Linux
```

### Documentation Structure

- **API Reference** (`api/`): Auto-generated from docstrings
- **User Guides** (`user/`): Step-by-step instructions for using features
- **Examples** (`examples/`): Complete, runnable examples with explanations
- **Developer Guide** (`dev/`): Information for contributors and developers

When adding new features, please update:
1. Docstrings in the code (Google style)
2. API reference if adding new classes/functions
3. User guides if changing behavior
4. Examples if relevant

### Documentation Standards

- Use MyST Markdown for all documentation files
- Follow the kebab-case naming convention for files (e.g., `getting-started.md`)
- Include practical examples in all guides
- Maintain consistent terminology throughout the documentation
- Documentation will be automatically checked during pre-commit

## Running Examples

The `examples/` directory contains standalone examples that demonstrate various features:

```bash
# Run a basic example
python examples/basic_flattening.py

# Run a more complex example
python examples/large_dataset_processing.py
```

These examples are meant to be educational and provide real-world usage patterns. 