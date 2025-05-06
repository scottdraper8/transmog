# Transmog Test Directory Structure

## Overview

The tests for the Transmog package are organized using an interface-based testing approach. This means that tests are focused on behavior rather than implementation details, making them more resilient to internal changes.

## Directory Structure

- `benchmarks/` - Performance benchmark tests (excluded from normal test runs)
- `config/` - Tests for configuration management
- `core/` - Tests for core processing components
- `dependencies/` - Tests for dependency management
- `error/` - Tests for error handling
- `formats/` - Tests for format conversion
- `helpers/` - Shared test utilities and mixins
- `integration/` - Integration tests across multiple components
- `interfaces/` - Abstract test interfaces defining behavior expectations
- `readers/` - Tests for data readers
- `writers/` - Tests for data writers

## Running Tests

### Standard Tests

Run the standard test suite (excludes benchmarks):

```bash
pytest
```

### Benchmarks

Run benchmark tests only:

```bash
pytest -m benchmark
```

### Memory Tests

Run memory usage tests:

```bash
pytest -m memory
```

### Coverage

Generate a coverage report:

```bash
pytest --cov=transmog
```

## Test Organization

Tests are organized based on the component they test, not by test type. Each component has its own directory with tests that verify its behavior.

The `interfaces/` directory contains abstract test classes that define the expected behavior for each component. The concrete test implementations in other directories inherit from these interfaces.

This approach ensures that components are tested against their interfaces rather than implementation details, making the tests more resilient to refactoring. 