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

The test suite is organized with an interface-based approach that focuses on behavior rather than implementation
details:

```text
tests/
├── benchmarks/           # Performance benchmark tests
│   ├── test_csv_reader_benchmarks.py
│   └── test_output_format_benchmarks.py
├── config/               # Configuration tests
│   ├── test_config_propagation.py
│   └── test_settings.py
├── core/                 # Core functionality tests
│   ├── test_extractor.py
│   ├── test_flattener.py
│   ├── test_hierarchy.py
│   ├── test_metadata.py
│   ├── test_processor.py
│   └── test_*_strategy.py
├── dependencies/         # Dependency management tests
│   └── test_dependency_manager.py
├── error/                # Error handling tests
│   └── test_error_handling.py
├── fixtures/             # Test data files
│   ├── sample.csv
│   ├── sample_bad_format.csv
│   ├── sample_empty.csv
│   └── sample_with_nulls.csv
├── formats/              # Format conversion tests
│   └── test_native_formats.py
├── helpers/              # Shared test utilities and mixins
├── integration/          # End-to-end integration tests
│   ├── test_csv_integration.py
│   ├── test_json_integration.py
│   ├── test_end_to_end.py
│   └── test_*_integration.py
├── interfaces/           # Interface contracts definitions
│   ├── test_flattener_interface.py
│   ├── test_processor_interface.py
│   ├── test_strategy_interface.py
│   └── test_*_interface.py
├── process/              # Process tests
│   └── test_streaming_result.py
├── readers/              # Input format reader tests
│   ├── test_csv_reader.py
│   └── test_json_reader.py
└── writers/              # Output format writer tests
    ├── test_csv_writer.py
    ├── test_json_writer.py
    ├── test_parquet_writer.py
    └── test_*_streaming_writer.py
```

### Test Organization Philosophy

Tests are organized based on the component they test, not by test type. Each component has its own directory
with tests that verify its behavior.

The `interfaces/` directory contains abstract test classes that define the expected behavior for each
component. The concrete test implementations in other directories inherit from these interfaces.

This approach ensures that components are tested against their interfaces rather than implementation
details, making the tests more resilient to refactoring.

The test suite follows these key principles:

1. **Interface-First Testing**: Tests define the expected behavior through interfaces
2. **Component Separation**: Each component has its own dedicated test modules
3. **Integration Tests**: End-to-end tests ensure components work together correctly
4. **Benchmarks**: Performance tests are separated to avoid slowing down the regular test suite
5. **Fixtures**: Common test data is shared through fixtures

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
# Run benchmark tests only
pytest -m benchmark

# Run memory usage tests only
pytest -m memory
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

### Running Performance Tests

Performance tests are separate from regular tests to avoid increasing the duration of the normal test suite:

```bash
# Run performance tests
pytest tests/benchmarks/

# Run a specific performance test
pytest tests/benchmarks/test_output_format_benchmarks.py
```

## Writing Tests

### Unit Tests

Unit tests focus on testing individual components in isolation. Here's an example that shows interface-based
testing from `tests/core/test_flattener.py`:

```python
"""
Tests for the flattener implementation.

This module tests the core flattener functionality using the interface-based approach.
"""

import pytest
from typing import Dict, List, Any, Optional

from transmog.core.flattener import flatten_json
from transmog.error import ProcessingError
from transmog.config import TransmogConfig

# Import and inherit from the interface
from tests.interfaces.test_flattener_interface import AbstractFlattenerTest


class TestFlattener(AbstractFlattenerTest):
    """
    Tests for the flattener module.

    Inherits from AbstractFlattenerTest to ensure it follows the interface-based testing pattern.
    """

    def test_flattener_with_config(self, simple_data):
        """Test flattening with a TransmogConfig object."""
        # Create processor with explicit configuration
        proc_config = (
            TransmogConfig.default()
            .with_naming(separator="_", deep_nesting_threshold=4)
            .with_processing(cast_to_string=False)
        )

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
