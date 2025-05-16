# Code Style Guide

This document outlines the coding standards and style conventions for the Transmog project.

## Coding Principles

1. **Readability**: Write code that is readable and understandable
2. **Simplicity**: Use straightforward solutions when possible
3. **Consistency**: Follow established patterns within the codebase
4. **Documentation**: Document code appropriately

## Python Style Guide

We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) with specific adaptations:

### Code Formatting

We use **ruff** for code formatting with a line length of 88 characters:

```bash
ruff format src tests
```

### Import Sorting

We use **ruff** to organize imports:

```bash
ruff check --select I --fix src tests
```

### Linting

We use **ruff** to check code quality:

```bash
ruff check src tests
```

Our ruff configuration in pyproject.toml includes:

```toml
[tool.ruff]
target-version = "py37"
line-length = 88
select = [
    "E",  # pycodestyle errors
    "F",  # pyflakes
    "I",  # isort
    "C4", # flake8-comprehensions
    "B",  # flake8-bugbear
    "N",  # pep8-naming
    "D",  # pydocstyle
    "UP", # pyupgrade
    "S",  # flake8-bandit
    "A",  # flake8-builtins
]
```

### Pre-commit Hooks

We use pre-commit hooks to automate style checking. Install pre-commit hooks with:

```bash
pre-commit install
```

## Naming Conventions

### Variables and Functions

Use snake_case for variables and functions:

```python
# Good
user_name = "John"
def calculate_total(items):
    pass

# Bad
userName = "John"
def CalculateTotal(items):
    pass
```

### Classes

Use PascalCase for class names:

```python
# Good
class DataProcessor:
    pass

# Bad
class data_processor:
    pass
```

### Constants

Use UPPER_CASE for constants:

```python
# Good
MAX_CONNECTIONS = 100

# Bad
max_connections = 100
```

### Private Methods and Variables

Prefix private methods and variables with a single underscore:

```python
# Internal/private method
def _internal_method(self):
    pass

# Private variable
self._private_var = 10
```

## Documentation

### Docstrings

Use Google-style docstrings:

```python
def process_data(data, options=None):
    """Process the input data according to the given options.

    Args:
        data (dict): The input data to process.
        options (dict, optional): Processing options. Defaults to None.

    Returns:
        dict: The processed data.

    Raises:
        ValueError: If data is empty or invalid.
    """
    # Implementation
```

### Comments

- Use comments to explain complex logic
- Keep comments updated when code changes
- Write comments in complete sentences
- Avoid redundant comments that restate the code

## Code Organization

### File Structure

- One class per file unless classes are closely related
- Group related functionality in modules
- Keep files under 500 lines when practical

### Module Imports

Order imports as follows:

1. Standard library imports
2. Third-party imports
3. Local application imports

Separate import groups with a blank line:

```python
import json
import os
from typing import Dict, List, Optional

import pyarrow as pa

from transmog.core import flattener
from transmog.io import csv_writer
```

## Type Hints

Use type hints for function parameters and return values:

```python
def process_items(items: List[Dict]) -> Dict[str, int]:
    """Process a list of items and return count summary."""
    # Implementation
```

Type checking uses mypy, configured in pyproject.toml:

```toml
[tool.mypy]
python_version = "3.9"
warn_return_any = true
warn_unused_configs = true
```

## Error Handling

- Be specific with exception types
- Include helpful error messages
- Avoid catching broad exceptions without re-raising

```python
# Good
try:
    value = data["key"]
except KeyError:
    raise ValueError(f"Missing required key 'key' in data")

# Bad
try:
    value = data["key"]
except:
    print("Error")
```

## Testing

- Write tests for all new functionality
- Test both success and failure cases
- Use descriptive test names that indicate what's being tested
- Keep tests simple and focused on a single functionality

```python
def test_processor_handles_empty_input():
    """Test that the processor properly handles empty input."""
    processor = DataProcessor()
    result = processor.process({})
    assert result == {"status": "empty"}
```

## Performance Considerations

- Prefer list comprehensions over loops where appropriate
- Be mindful of memory usage with large datasets
- Use appropriate data structures for the task

## Version Control

- Make small, focused commits
- Write descriptive commit messages
- Reference issue numbers in commit messages when applicable

## Final Note

When in doubt, follow the established patterns in the existing codebase. Consistency within the project
is more important than strictly adhering to external guidelines.
