# Testing Guide

This guide provides instructions and best practices for testing contributions to Transmog.

> **Note**: A more concise summary of the test structure is also available in the tests directory.

## Testing Approach

Transmog uses these testing practices:

1. **Test coverage**: Test key functionalities and code paths
2. **Test-driven development**: Consider writing tests before implementing features
3. **Multiple test types**: Include unit, integration, and performance tests
4. **Test cases**: Use realistic examples where possible
5. **Continuous integration**: Tests run automatically in CI/CD
6. **Interface-based testing**: Tests focus on behavior rather than implementation details

## Test Structure

The test suite is organized by functionality, with each directory containing tests for specific components:

```text
tests/
├── advanced/             # Advanced feature tests
│   └── test_natural_ids.py
├── api/                  # Core API tests
│   ├── test_flatten_basic.py
│   └── test_flatten_result_core.py
├── arrays/               # Array processing tests
│   └── test_array_extraction.py
├── config/               # Configuration tests
│   └── test_transmog_config.py
├── core/                 # Core functionality tests
│   ├── test_flattener.py
│   └── test_metadata.py
├── error/                # Error handling tests
│   └── test_error_modes.py
├── fixtures/             # Test data files
│   ├── sample.csv
│   ├── sample_bad_format.csv
│   ├── sample_empty.csv
│   └── sample_with_nulls.csv
├── integration/          # End-to-end integration tests
│   ├── conftest.py
│   └── test_complete_workflows.py
├── writers/              # Writer and I/O tests
│   └── test_writer_factory.py
├── naming/               # Naming convention tests
│   └── test_naming_conventions.py
└── conftest.py           # Shared test configuration
```

### Test Organization Philosophy

Tests are organized by functionality, with each directory containing tests for specific components or features:

- **`api/`**: Tests for the main public API functions (`flatten`, `flatten_file`, etc.) and result objects
- **`core/`**: Tests for core processing logic (flattening, metadata generation)
- **`config/`**: Tests for configuration management and validation
- **`writers/`**: Tests for output writers and format detection
- **`error/`**: Tests for error handling strategies and recovery mechanisms
- **`arrays/`**: Tests for array processing and extraction logic
- **`naming/`**: Tests for field and table naming conventions
- **`advanced/`**: Tests for advanced features like natural ID discovery
- **`integration/`**: End-to-end tests that verify complete workflows
- **`fixtures/`**: Test data files used across multiple test modules

The test suite follows these key principles:

1. **Component Separation**: Each component has its own dedicated test modules
2. **Integration Coverage**: End-to-end tests ensure components work together correctly
3. **Realistic Data**: Tests use realistic examples and edge cases
4. **Shared Fixtures**: Common test data and utilities are shared through fixtures
5. **Clear Organization**: Test structure mirrors the package structure for easy navigation

## Running Tests

### Basic Test Execution

To run the test suite:

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run a specific test file
pytest tests/core/test_flattener.py

# Run a specific test
pytest tests/core/test_flattener.py::test_flattener_with_config

# Run tests by directory
pytest tests/integration/

# Run tests by pattern matching
pytest -k "flattener"
```

### Special Test Categories

Some tests are tagged with markers for selective execution:

```bash
# Run integration tests only
pytest tests/integration/

# Run core functionality tests only
pytest tests/core/
```

### Test Coverage

To check test coverage:

```bash
# Run tests with coverage
pytest --cov=transmog

# Generate a coverage report
pytest --cov=transmog --cov-report=html

# Open the coverage report
open htmlcov/index.html  # On macOS
# or
start htmlcov/index.html  # On Windows
# or
xdg-open htmlcov/index.html  # On Linux
```

### Running Specific Test Categories

You can run specific categories of tests:

```bash
# Run API tests
pytest tests/api/

# Run error handling tests
pytest tests/error/

# Run array processing tests
pytest tests/arrays/

# Run writer tests
pytest tests/writers/
```

## Writing Tests

### Unit Tests

Unit tests focus on testing individual components in isolation. Here's an example from `tests/core/test_flattener.py`:

```python
"""
Tests for the core flattener functionality.
"""

import pytest
from typing import Dict, List, Any, Optional

from transmog.core.flattener import flatten_json
from transmog.error import ProcessingError
from transmog.config import TransmogConfig


class TestFlattener:
    """Tests for the flattener module."""

    def test_basic_flattening(self):
        """Test basic JSON flattening functionality."""
        data = {"name": "John", "address": {"city": "NYC", "zip": "10001"}}
        result = flatten_json(data)

        expected = {
            "name": "John",
            "address_city": "NYC",
            "address_zip": "10001"
        }
        assert result == expected

    def test_flattening_with_custom_separator(self):
        """Test flattening with custom field separator."""
        data = {"user": {"name": "Alice", "age": 30}}
        result = flatten_json(data, separator=".")

        expected = {
            "user.name": "Alice",
            "user.age": 30
        }
        assert result == expected

        # Use the TransmogConfig to get the parameters
        flattened = flatten_json(
            simple_data,
            separator=proc_config.naming.separator,
            cast_to_string=proc_config.processing.cast_to_string,
            deep_nesting_threshold=proc_config.naming.deep_nesting_threshold,
        )

        # Check basic fields are preserved
        assert flattened["id"] == 1
        assert flattened["name"] == "Test"

        # Check nested fields are flattened
        assert "address_street" in flattened
        assert "address_city" in flattened
        assert "address_state" in flattened
```

### Interface-Based Testing

Transmog uses an interface-based testing approach where abstract test classes define the expected behavior,
and concrete test implementations inherit from them. Here's an example from `tests/interfaces/test_processor_interface.p
y`:

```python
class AbstractProcessorTest:
    """
    Abstract base class for processor tests.

    This class defines a standardized set of tests that should apply to all processor implementations.
    Subclasses must define appropriate fixtures.
    """

    @pytest.fixture
    def processor(self):
        """Create a standard processor instance."""
        config = (
            TransmogConfig.default()
            .with_processing(cast_to_string=True)
            .with_naming(separator="_", deep_nesting_threshold=4)
        )
        return Processor(config=config)

    @pytest.fixture
    def simple_data(self):
        """Create a simple data structure."""
        return {
            "id": "123",
            "name": "Test Entity",
            "addr": {"street": "123 Main St", "city": "Anytown", "zip": "12345"},
        }

    def test_process_simple_data(self, processor, simple_data):
        """Test processing simple data."""
        result = processor.process(simple_data, entity_name="test")

        # Verify the result is a ProcessingResult
        assert isinstance(result, ProcessingResult)

        # Verify main records
        main_records = result.get_main_table()
        assert len(main_records) == 1
        assert main_records[0]["id"] == "123"
        assert main_records[0]["name"] == "Test Entity"

        # Check for flattened address fields
        assert "addr_street" in main_records[0]
        assert "addr_city" in main_records[0]
        assert "addr_zip" in main_records[0]
```

This approach ensures that any implementation of the processor interface will be tested consistently
against the same behavioral expectations.

### Integration Tests

Integration tests ensure different components work together correctly. Here's an example of how integration
tests are structured:

```python
# From tests/integration/test_end_to_end.py
def test_process_complex_data_with_deterministic_ids():
    """Test end-to-end processing of complex nested data with deterministic IDs."""
    # Sample data with nested structures and arrays
    data = {
        "user": {
            "id": "user123",
            "name": "Test User",
            "contact": {
                "email": "test@example.com",
                "phone": "555-1234"
            },
            "orders": [
                {"id": "order1", "amount": 99.99},
                {"id": "order2", "amount": 45.50}
            ]
        }
    }

    # Configure processor with deterministic IDs
    config = TransmogConfig.with_deterministic_ids({
        "": "id",                     # Root level uses "id" field
        "user_orders": "id"           # Order records use "id" field
    })

    processor = Processor(config=config)

    # Process the data
    result = processor.process(data)

    # Verify main table
    main_table = result.to_dict()["main"]
    assert len(main_table) == 1
    assert main_table[0]["user_id"] == "user123"

    # Verify orders table
    orders_table = result.to_dict()["user_orders"]
    assert len(orders_table) == 2

    # Verify deterministic IDs were used
    assert orders_table[0]["__transmog_id"].startswith("order1")
    assert orders_table[1]["__transmog_id"].startswith("order2")

    # Verify parent-child relationships
    assert orders_table[0]["__parent_transmog_id"] == main_table[0]["__transmog_id"]
    assert orders_table[1]["__parent_transmog_id"] == main_table[0]["__transmog_id"]
```

### Testing with Fixtures

Use fixtures to provide reusable test data:

```python
import pytest
import json
import os

@pytest.fixture
def sample_data():
    """Load sample data from a fixture file."""
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "fixtures",
        "sample_data.json"
    )

    with open(fixture_path, 'r') as f:
        return json.load(f)

@pytest.fixture
def expected_result():
    """Load expected result from a fixture file."""
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "fixtures",
        "expected_results.json"
    )

    with open(fixture_path, 'r') as f:
        return json.load(f)

def test_with_fixtures(sample_data, expected_result):
    """Test using fixture data."""
    transformer = Transformer()
    result = transformer.transform(sample_data)
    assert result.to_dict() == expected_result
```

### Testing Error Cases

Test how your code handles errors:

```python
import pytest
from transmog import Transformer
from transmog.errors import TransformError

def test_invalid_input():
    """Test that appropriate errors are raised for invalid inputs."""
    transformer = Transformer()

    # Test with None
    with pytest.raises(ValueError):
        transformer.transform(None)

    # Test with invalid type
    with pytest.raises(TypeError):
        transformer.transform(123)
```

### Parametrized Tests

Use parametrization to test multiple cases efficiently:

```python
import pytest
from transmog.path import parse_path

@pytest.mark.parametrize("path_string,expected_parts", [
    ("user.name", ["user", "name"]),
    ("orders[0].items[1].price", ["orders", "0", "items", "1", "price"]),
    ("deeply.nested.structure", ["deeply", "nested", "structure"]),
    ("path.with\\.dot", ["path", "with.dot"]),  # Escaped dot
])
def test_parse_path(path_string, expected_parts):
    """Test that path parsing works correctly for various path formats."""
    parts = parse_path(path_string)
    assert parts == expected_parts
```

## Performance Testing

Transmog uses the pytest-benchmark plugin for performance testing. Here's an example from `tests/benchmarks/test_output_
format_benchmarks.py`:

```python
"""
Benchmarks for output format methods.

This module contains benchmark tests for the new output format methods
added to ProcessingResult.
"""

import pytest
from typing import Dict, List, Any

from transmog import Processor
from transmog.config import TransmogConfig


def generate_test_data(num_records: int = 100) -> list[dict[str, Any]]:
    """Generate synthetic test data for benchmarks."""
    data = []
    for i in range(num_records):
        data.append(
            {
                "id": f"record-{i}",
                "metadata": {
                    "created": "2023-01-01",
                    "modified": "2023-03-15",
                    "source": "benchmark",
                },
                "details": {
                    "name": f"Record {i}",
                    "description": f"Test record {i} for benchmarking",
                    "value": i * 10,
                    "tags": ["test", "benchmark", f"tag-{i}"],
                },
                "items": [
                    {"item_id": f"item-{i}-1", "value": i * 10},
                    {"item_id": f"item-{i}-2", "value": i * 20},
                    {"item_id": f"item-{i}-3", "value": i * 30},
                ],
                "status": {
                    "active": i % 2 == 0,
                    "approved": i % 3 == 0,
                    "visible": i % 5 != 0,
                },
            }
        )
    return data


@pytest.fixture
def processed_result():
    """Create a ProcessingResult with test data."""
    # Generate test data
    data = generate_test_data(100)

    # Process the data
    config = TransmogConfig.default().with_processing(visit_arrays=True)
    processor = Processor(config=config)
    return processor.process_batch(data, entity_name="benchmark")


def test_benchmark_to_dict(processed_result, benchmark):
    """Benchmark the to_dict method."""
    benchmark(processed_result.to_dict)


def test_benchmark_to_json_objects(processed_result, benchmark):
    """Benchmark the to_json_objects method."""
    benchmark(processed_result.to_json_objects)


@pytest.mark.skipif(
    not pytest.importorskip("pyarrow", reason="PyArrow not available"),
    reason="PyArrow required for this benchmark",
)
def test_benchmark_to_pyarrow_tables(processed_result, benchmark):
    """Benchmark the to_pyarrow_tables method."""
    benchmark(processed_result.to_pyarrow_tables)


def test_benchmark_to_json_bytes(processed_result, benchmark):
    """Benchmark the to_json_bytes method."""
    # Test with no indentation for best performance
    benchmark(processed_result.to_json_bytes, indent=None)


def test_benchmark_to_csv_bytes(processed_result, benchmark):
    """Benchmark the to_csv_bytes method."""
    benchmark(processed_result.to_csv_bytes)
```

### Running Performance Tests

To run benchmark tests:

```bash
# Run all benchmark tests
pytest tests/benchmarks/

# Run a specific benchmark file
pytest tests/benchmarks/test_output_format_benchmarks.py

# Run a specific benchmark test
pytest tests/benchmarks/test_output_format_benchmarks.py::test_benchmark_to_dict
```

The pytest-benchmark plugin will output detailed statistics about each benchmark, including:

- Min/max/mean execution time
- Standard deviation
- Number of iterations
- Rounds
- Total time

For detailed performance analysis, see the [Benchmarking Guide](benchmarking.md).

## Mocking

Use mocks to isolate the code being tested:

```python
import pytest
from unittest.mock import patch, MagicMock
from transmog.io import ParquetWriter

def test_parquet_writing():
    """Test Parquet writing with mocked pyarrow."""
    # Mock the pyarrow module
    with patch("transmog.io.parquet.pa") as mock_pa:
        # Set up mock behavior
        mock_table = MagicMock()
        mock_pa.Table.from_pydict.return_value = mock_table

        # Create a writer and write data
        writer = ParquetWriter()
        data = {"column1": [1, 2, 3], "column2": ["a", "b", "c"]}
        writer.write(data, "test.parquet")

        # Verify the mock was called correctly
        mock_pa.Table.from_pydict.assert_called_once_with(data)
        # Verify that certain methods weren't called unnecessarily
        mock_table.write_parquet.assert_called_once()

        # Check the file path was passed correctly
        args, kwargs = mock_table.write_parquet.call_args
        assert args[0] == "test.parquet"
```

## Performance Testing and Benchmarking

For detailed information on performance testing and benchmarking, see the [Benchmarking Guide](benchmarking.md).
The benchmarking guide covers:

- Using the command-line benchmarking script
- Working with pytest benchmark tests
- When to use each approach
- Performance optimization tips
- Contributing performance improvements
