# Testing Guide

This guide provides instructions and best practices for testing contributions to Transmog.

## Testing Approach

Transmog uses these testing practices:

1. **Test coverage**: Test key functionalities and code paths
2. **Test-driven development**: Consider writing tests before implementing features
3. **Multiple test types**: Include unit, integration, and performance tests
4. **Test cases**: Use realistic examples where possible
5. **Continuous integration**: Tests run automatically in CI/CD

## Test Structure

The test suite is organized as follows:

```
tests/
├── unit/               # Unit tests for individual components
│   ├── test_transformer.py
│   ├── test_path.py
│   ├── test_processors.py
│   └── ...
├── integration/        # Integration tests for component interactions
│   ├── test_end_to_end.py
│   ├── test_io_formats.py
│   └── ...
├── performance/        # Performance benchmarks
│   ├── test_large_datasets.py
│   ├── test_nested_structures.py
│   └── ...
├── fixtures/           # Test data and fixtures
│   ├── sample_data.json
│   ├── expected_results.json
│   └── ...
└── conftest.py         # Shared pytest fixtures
```

## Running Tests

### Basic Test Execution

To run the test suite:

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run a specific test file
pytest tests/unit/test_transformer.py

# Run a specific test
pytest tests/unit/test_transformer.py::test_transform_nested_dict
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
pytest tests/performance/

# Run a specific performance test
pytest tests/performance/test_large_datasets.py
```

## Writing Tests

### Unit Tests

Unit tests focus on testing individual components in isolation. Here's an example of a unit test:

```python
import pytest
from transmog import Transformer
from transmog.processors import string_processor

def test_string_processor():
    """Test that the string processor correctly handles strings."""
    # Test with a string value
    assert string_processor("hello") == "hello"
    
    # Test with a non-string value
    assert string_processor(123) == 123
    
    # Test with None
    assert string_processor(None) is None

def test_transformer_init():
    """Test that a Transformer can be initialized with various options."""
    # Test default initialization
    transformer = Transformer()
    assert transformer.delimiter == "."
    
    # Test custom initialization
    transformer = Transformer(delimiter="/", preserve_arrays=True)
    assert transformer.delimiter == "/"
    assert transformer.preserve_arrays is True
```

### Integration Tests

Integration tests check how components work together:

```python
import pytest
from transmog import Transformer
from transmog.path import PathResolver
from transmog.processors import default_processors

def test_end_to_end_transformation():
    """Test a complete transformation process."""
    # Sample input data
    data = {
        "user": {
            "id": 1,
            "name": "Test User",
            "addresses": [
                {"type": "home", "city": "New York"},
                {"type": "work", "city": "Boston"}
            ]
        }
    }
    
    # Expected output
    expected = {
        "user.id": 1,
        "user.name": "Test User",
        "user.addresses.0.type": "home",
        "user.addresses.0.city": "New York",
        "user.addresses.1.type": "work",
        "user.addresses.1.city": "Boston"
    }
    
    # Perform transformation
    transformer = Transformer()
    result = transformer.transform(data)
    
    # Verify the result
    assert result.to_dict() == expected
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
    
    # Test with circular reference
    circular = {}
    circular["self"] = circular
    
    with pytest.raises(TransformError) as excinfo:
        transformer.transform(circular)
    
    assert "circular reference" in str(excinfo.value).lower()
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

Performance tests ensure that the library remains efficient:

```python
import pytest
import time
import random
import string
from transmog import Transformer

def generate_large_dataset(size=1000, depth=5, width=5):
    """Generate a large nested dataset for performance testing."""
    def _generate_nested(current_depth):
        if current_depth >= depth:
            return random.choice([
                random.randint(1, 1000),
                "".join(random.choices(string.ascii_letters, k=10)),
                random.random()
            ])
        
        result = {}
        for i in range(width):
            key = f"key_{i}"
            if random.random() < 0.2 and current_depth < depth - 1:
                # Create an array occasionally
                result[key] = [
                    _generate_nested(current_depth + 1)
                    for _ in range(random.randint(1, 5))
                ]
            else:
                result[key] = _generate_nested(current_depth + 1)
        
        return result
    
    return [_generate_nested(0) for _ in range(size)]

def test_performance_large_dataset():
    """Test performance with a large dataset."""
    # Generate a large dataset
    data = generate_large_dataset()
    
    # Initialize transformer
    transformer = Transformer()
    
    # Measure transformation time
    start_time = time.time()
    result = transformer.transform_many(data)
    end_time = time.time()
    
    # Check that it completes within a reasonable time
    elapsed = end_time - start_time
    print(f"Processed {len(data)} records in {elapsed:.2f} seconds")
    
    # This is a flexible threshold - adjust based on your performance expectations
    assert elapsed < 5.0, f"Performance test failed: {elapsed:.2f}s > 5.0s threshold"
```

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