# Contributing to Transmog

Contributions to Transmog are welcome and appreciated. This guide covers everything needed to get started contributing to the project.

## Quick Start

### Prerequisites

- Python 3.8 or higher
- Git

### Development Setup

1. **Fork and clone the repository**

```bash
git clone https://github.com/your-username/transmog.git
cd transmog
```

2. **Create a virtual environment**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install development dependencies**

```bash
pip install -e ".[dev]"
```

4. **Install pre-commit hooks**

```bash
pre-commit install
```

The pre-commit hooks handle code formatting, linting, and style checks automatically. No manual formatting is needed.

5. **Verify the setup**

```bash
python -m pytest tests/
```

## Development Workflow

### Making Changes

1. **Create a feature branch**

```bash
git checkout -b feature/your-feature-name
```

2. **Make your changes**

Write code, add tests, update documentation as needed.

3. **Run tests locally**

```bash
# Run all tests
python -m pytest

# Run specific test file
python -m pytest tests/test_specific.py

# Run with coverage
python -m pytest --cov=transmog
```

4. **Commit your changes**

Pre-commit hooks will automatically format and check code.

```bash
git add .
git commit -m "Add feature: brief description"
```

5. **Push and create pull request**

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub.

## Types of Contributions

### Bug Reports

When reporting bugs, include:

- Python version and operating system
- Transmog version
- Minimal code example that reproduces the issue
- Expected vs actual behavior
- Full error traceback if applicable

### Feature Requests

For new features:

- Describe the use case and problem being solved
- Provide examples of how the feature would be used
- Consider if the feature fits Transmog's scope and design principles

### Code Contributions

**Good first contributions:**
- Documentation improvements
- Test coverage improvements
- Bug fixes with clear reproduction steps
- Performance optimizations with benchmarks

**Larger contributions:**
- New data format support
- Processing optimizations
- API enhancements

## Development Standards

### Code Style

Code style is automatically enforced by pre-commit hooks. The setup includes:

- **Black** for code formatting
- **isort** for import sorting
- **flake8** for linting
- **mypy** for type checking

No manual formatting is required - just commit changes and the hooks handle the rest.

### Testing

- Write tests for new functionality
- Maintain or improve test coverage
- Use descriptive test names that explain what is being tested
- Include both unit tests and integration tests where appropriate

#### Test Structure

```python
def test_flatten_nested_objects():
    """Test that nested objects are properly flattened."""
    data = {"user": {"name": "Alice", "age": 30}}
    result = tm.flatten(data)

    assert len(result.main) == 1
    assert result.main[0]["user_name"] == "Alice"
    assert result.main[0]["user_age"] == 30
```

#### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=transmog --cov-report=html

# Run specific test categories
pytest tests/unit/
pytest tests/integration/
pytest tests/performance/
```

### Documentation

- Update docstrings for new functions and classes
- Add examples to demonstrate usage
- Update relevant documentation files in `docs/`
- Ensure examples are runnable and accurate

## API Design Principles

When contributing code changes, follow these principles:

### Simplicity First

The main API should remain simple and intuitive:

```python
# Good: Simple, clear API
result = tm.flatten(data)

# Avoid: Overly complex APIs
result = tm.flatten(data, complex_config=ComplexConfigObject())
```

### Sensible Defaults

Parameters should have sensible defaults for common use cases:

```python
# Most users should get good results with defaults
result = tm.flatten(data)

# Advanced users can customize as needed
result = tm.flatten(data, arrays="inline", batch_size=5000)
```

### Backward Compatibility

- Avoid breaking changes to existing APIs
- Deprecate features before removing them
- Provide migration paths for major changes

### Performance Considerations

- Optimize for common use cases
- Provide memory-efficient options for large datasets
- Include benchmarks for performance-critical changes

## Pull Request Process

### Before Submitting

- [ ] Tests pass locally
- [ ] Code follows project conventions (enforced by pre-commit)
- [ ] Documentation is updated if needed
- [ ] Changes are focused and atomic
- [ ] Commit messages are clear and descriptive

### Pull Request Template

When creating a pull request, include:

**Description**
Brief description of changes and motivation.

**Type of Change**
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Code refactoring

**Testing**
- [ ] Tests added/updated
- [ ] All tests pass
- [ ] Manual testing performed

**Documentation**
- [ ] Documentation updated
- [ ] Examples added/updated
- [ ] API documentation updated

### Review Process

1. **Automated Checks**: CI runs tests and checks
2. **Code Review**: Maintainers review the code
3. **Discussion**: Address feedback and questions
4. **Approval**: Maintainer approves the changes
5. **Merge**: Changes are merged to main branch

## Advanced Topics

### Architecture Overview

Transmog follows a modular design with several core components:

- **Processor**: Main entry point for users, orchestrates transformation
- **Core Transformation**: Flattener and Extractor for data transformation
- **Configuration System**: Hierarchical configuration with factory methods
- **I/O System**: Handles reading input and writing output in various formats
- **Error Handling**: Configurable strategies for dealing with errors

### Extension Points

Transmog provides several extension points for customization:

1. **Custom Recovery Strategies**:

   ```python
   from transmog.error import RecoveryStrategy

   class MyRecoveryStrategy(RecoveryStrategy):
       def recover(self, error, context=None):
           # Custom recovery logic
           return recovery_result
   ```

2. **Custom ID Generation**:

   ```python
   def custom_id_strategy(record):
       # Generate ID based on record contents
       return f"CUSTOM-{record.get('id', 'unknown')}"

   # Use with processor
   processor = Processor.with_custom_id_generation(custom_id_strategy)
   ```

3. **Output Format Extensions**:

   ```python
   from transmog.io import DataWriter, register_writer

   class MyCustomWriter(DataWriter):
       def write(self, data, destination):
           # Custom writing logic
           pass

   # Register the writer
   register_writer("custom-format", MyCustomWriter)
   ```

### Performance Testing

When making performance-related changes:

- Use the benchmarking script: `python scripts/run_benchmarks.py`
- Compare before and after performance metrics
- Include benchmark results in pull request description
- Consider memory usage as well as processing speed

### Documentation Updates

When updating documentation:

- Follow the passive voice style guidelines
- Avoid personal pronouns (you, we, our, etc)
- No temporal language (previously, new version, etc)
- Provide complete, runnable examples
- Test all code examples for accuracy

## Release Process

Releases follow semantic versioning:

- **Patch** (1.0.1): Bug fixes and minor improvements
- **Minor** (1.1.0): New features, backward compatible
- **Major** (2.0.0): Breaking changes

## Getting Help

- **Documentation**: Check the [documentation](https://scottdraper.github.io/transmog/)
- **Issues**: Search existing [GitHub issues](https://github.com/scottdraper/transmog/issues)
- **Discussions**: Use [GitHub Discussions](https://github.com/scottdraper/transmog/discussions) for questions
- **Contact**: Reach out to maintainers for guidance on larger contributions

## Code of Conduct

Be respectful, inclusive, and constructive in all interactions. Focus on the code and ideas, not the person. Help create a welcoming environment for all contributors.

## Recognition

Contributors are recognized in:
- Release notes for significant contributions
- GitHub contributors list
- Project acknowledgments

Thank you for contributing to Transmog!
