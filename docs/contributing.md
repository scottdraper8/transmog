# Contributing to Transmog

Contributions to Transmog are welcome and appreciated. This guide covers
how to set up a development environment and submit changes.

## Getting Started

### Prerequisites

- Python 3.10 or higher
- Git
- [uv](https://docs.astral.sh/uv/) (for dependency and environment management)

### Installing uv

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

See the [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/) for other options.

## Development Setup

1. **Fork and clone the repository**

   ```bash
   git clone https://github.com/your-username/transmog.git
   cd transmog
   ```

2. **Install dependencies**

   ```bash
   uv sync --extra dev
   ```

   This will create a virtual environment and install all dependencies
   including development tools.

3. **Install pre-commit hooks**

   ```bash
   uv run pre-commit install
   uv run pre-commit install --hook-type pre-push
   ```

4. **Verify the setup**

   ```bash
   uv run pytest
   ```

## Making Changes

### Development Workflow

1. **Create a feature branch**

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**

   Write code, add tests, update documentation as needed.

3. **Run tests**

   ```bash
   # Run all tests
   uv run pytest

   # Run with coverage
   uv run pytest --cov=transmog

   # Run specific tests
   uv run pytest tests/unit/test_specific.py
   ```

4. **Commit your changes**

   Pre-commit hooks will automatically format and check code on commit.

   ```bash
   git add .
   git commit -m "Brief description of changes"
   ```

5. **Push and create a pull request**

```bash
git push origin feature/your-feature-name
```

Then open a pull request on GitHub with a description of your changes.

## Running Commands

All commands should be run through uv to use the project's virtual environment:

```bash
# Run tests
uv run pytest

# Run any tool
uv run ruff check src/
```

## Code Standards

Code style is automatically enforced by pre-commit hooks:

- **Ruff** for code formatting and linting
- **mypy** for type checking
- **interrogate** for docstring coverage
- **bandit** for security checks

No manual formatting is required - just commit and the hooks handle the rest.

## Codebase Structure

The codebase is organized into focused modules:

- **`src/transmog/api.py`** - Public API (`flatten`, `flatten_stream`, `FlattenResult`)
- **`src/transmog/config.py`** - Configuration dataclass
- **`src/transmog/flattening.py`** - Core flattening, metadata, and array extraction logic
- **`src/transmog/iterators.py`** - Input normalization for JSON and JSONL sources
- **`src/transmog/streaming.py`** - Streaming pipeline orchestration
- **`src/transmog/writers/`** - CSV, Parquet, ORC, and Avro writers (batch and streaming)
- **`src/transmog/exceptions.py`** - Exception hierarchy
- **`src/transmog/types.py`** - Shared enums and processing context
- **`tests/`** - Unit, integration, and performance test suites
- **`scripts/`** - Maintenance utilities (setup, release automation, benchmarks)

## Testing

- Write tests for new functionality
- Maintain or improve test coverage
- Use descriptive test names

```bash
# Run all tests
uv run pytest

# Run with coverage report
uv run pytest --cov=transmog --cov-report=html

# Run specific test categories
uv run pytest tests/unit/
uv run pytest tests/integration/
```

## Documentation

- Update docstrings for functions and classes
- Add examples to demonstrate usage
- Update relevant documentation files in `docs/`
- Keep examples runnable and accurate

Documentation style guidelines:

- Use passive voice
- Avoid personal pronouns (you, we, our)
- No temporal language (previously, new version)
- Provide complete, runnable examples

## Reporting Issues

When reporting bugs, include:

- Python version and operating system
- Transmog version
- Minimal code example that reproduces the issue
- Expected vs actual behavior
- Full error traceback if applicable

## Questions and Support

- **Issues**: [GitHub issues](https://github.com/scottdraper8/transmog/issues)
- **Documentation**: [scottdraper8.github.io/transmog](https://scottdraper8.github.io/transmog)

Thank you for contributing to Transmog!
