# Extending Transmog

This guide explains how to extend Transmog with custom components.

## Custom Value Processors

Value processors transform values during the extraction process.

### Creating a Value Processor

A value processor is a function that takes a value and returns a transformed version:

```python
def uppercase_strings(value, path=None, context=None):
    """Convert all string values to uppercase."""
    if isinstance(value, str):
        return value.upper()
    return value

def format_currency(value, path=None, context=None):
    """Format numeric values as currency strings if in a price field."""
    if path and ("price" in path or "amount" in path) and isinstance(value, (int, float)):
        return f"${value:.2f}"
    return value
```

### Using Custom Processors

You can use custom processors by passing them to the `Transformer`:

```python
from transmog import Transformer

# Create transformer with custom processors
transformer = Transformer(
    value_processors=[uppercase_strings, format_currency]
)

# Process data using your custom processors
result = transformer.transform(data)
```

### Processor Context

The `context` parameter provides additional information for your processor:

```python
def add_metadata(value, path=None, context=None):
    """Add metadata based on context information."""
    if context and "metadata" in context:
        # Only process root-level fields
        if path and "." not in path:
            return {
                "value": value,
                "processed_at": context["metadata"].get("timestamp"),
                "source": context["metadata"].get("source")
            }
    return value

# Usage
transformer = Transformer(
    value_processors=[add_metadata],
    context={
        "metadata": {
            "timestamp": "2023-05-01T12:00:00Z",
            "source": "api-export"
        }
    }
)
```

## Custom Path Resolution

You can create custom path resolvers to handle specialized path expressions.

### Creating a Path Resolver

To create a custom path resolver, subclass `BasePathResolver`:

```python
from transmog.path import BasePathResolver

class RegexPathResolver(BasePathResolver):
    """Resolve paths using regular expressions."""

    def resolve(self, data, path_expression):
        """
        Resolve paths using regex patterns.

        Example path_expression: "users./^a.*/.name" matches names of users
        whose keys start with 'a'.
        """
        # Implementation would parse the expression and extract matching paths
        import re

        parts = path_expression.split('.')
        current = data
        results = []

        # Your custom resolution logic here
        # ...

        return results
```

### Registering a Custom Resolver

Register your custom resolver with Transmog:

```python
from transmog import register_path_resolver

# Register your custom resolver
register_path_resolver("regex", RegexPathResolver())

# Use it in a transformer
transformer = Transformer(
    path_resolver="regex",
    paths=["users./^a.*/.name"]
)
```

## Custom Output Formatters

You can create custom output formatters to support additional formats.

### Creating an Output Formatter

Create a custom formatter by subclassing `BaseFormatter`:

```python
from transmog.io import BaseFormatter

class CustomFormatter(BaseFormatter):
    """Format results in a custom format."""

    def format(self, data):
        """Convert the result data to the custom format."""
        # Implement your custom formatting logic here
        return self._convert_to_custom_format(data)

    def write_to_file(self, data, file_path):
        """Write the formatted data to a file."""
        formatted_data = self.format(data)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(formatted_data)

    def _convert_to_custom_format(self, data):
        """Helper method to convert data to custom format."""
        # Implementation details here
        return str(data)  # Placeholder implementation
```

### Registering a Custom Formatter

Register your formatter with Transmog:

```python
from transmog import register_formatter

# Register your custom formatter
register_formatter("custom", CustomFormatter())

# Use it in your code
result = transformer.transform(data)
custom_output = result.to_format("custom")

# Or write directly to a file
result.to_file("output.custom", format="custom")
```

## Custom Recovery Strategies

You can create custom recovery strategies to handle errors during transformation.

### Creating a Recovery Strategy

Subclass `RecoveryStrategy` to create a custom recovery approach:

```python
from transmog.error import RecoveryStrategy

class AuditingRecoveryStrategy(RecoveryStrategy):
    """Recovery strategy that audits errors before handling them."""

    def __init__(self, audit_log_path=None):
        self.audit_log_path = audit_log_path or "transmog_audit.log"
        self.errors = []

    def recover(self, error, context=None):
        """
        Audit the error, then decide how to recover.

        Args:
            error: The exception that occurred
            context: Additional context information

        Returns:
            tuple: (recovered, replacement_value)
        """
        # Log the error
        self._audit_error(error, context)

        # Decide how to recover based on error type
        if isinstance(error, ValueError):
            return True, None  # Skip this value
        elif isinstance(error, KeyError):
            return True, {}  # Replace with empty dict
        else:
            return False, None  # Cannot recover

    def _audit_error(self, error, context):
        """Record the error for auditing."""
        error_info = {
            "type": type(error).__name__,
            "message": str(error),
            "context": context
        }
        self.errors.append(error_info)

        # Write to log file
        with open(self.audit_log_path, 'a') as f:
            f.write(f"{error_info}\n")
```

### Using a Custom Recovery Strategy

Use your custom recovery strategy:

```python
from transmog import Transformer

# Create the recovery strategy
recovery = AuditingRecoveryStrategy(audit_log_path="errors.log")

# Use it in the transformer
transformer = Transformer(recovery_strategy=recovery)
result = transformer.transform(problematic_data)

# Check the recorded errors after processing
for error in recovery.errors:
    print(f"Error: {error['type']} - {error['message']}")
```

## Custom Configuration

You can extend the configuration system to add custom settings.

### Adding Custom Settings

Register custom settings with the configuration system:

```python
from transmog.config import register_option

# Register a custom option
register_option(
    name="my_custom_setting",
    default_value="default",
    validator=lambda x: isinstance(x, str),
    description="A custom setting for my extension"
)

# Use the custom option
from transmog.config import get_option

my_setting = get_option("my_custom_setting")
```

### Creating a Configuration Extension

For more complex needs, create a configuration extension:

```python
from transmog.config import ConfigExtension

class MyConfigExtension(ConfigExtension):
    """Custom configuration extension for specialized needs."""

    def __init__(self):
        self.register_options()

    def register_options(self):
        """Register all options for this extension."""
        from transmog.config import register_option

        register_option(
            name="my_extension.enabled",
            default_value=True,
            validator=lambda x: isinstance(x, bool),
            description="Enable my extension features"
        )

        register_option(
            name="my_extension.mode",
            default_value="standard",
            validator=lambda x: x in ["standard", "advanced", "expert"],
            description="Operation mode for my extension"
        )

    def get_settings(self):
        """Get all settings for this extension."""
        from transmog.config import get_option

        return {
            "enabled": get_option("my_extension.enabled"),
            "mode": get_option("my_extension.mode")
        }

# Initialize the extension
my_extension_config = MyConfigExtension()

# Use the extension settings
settings = my_extension_config.get_settings()
if settings["enabled"]:
    print(f"Extension enabled in {settings['mode']} mode")
```

## Creating Plugins

For more extensive extensions, you can create a plugin package.

### Plugin Structure

A basic plugin structure:

```text
transmog-myplugin/
├── src/
│   └── transmog_myplugin/
│       ├── __init__.py
│       ├── formatters.py
│       ├── processors.py
│       └── resolvers.py
├── tests/
├── pyproject.toml
└── README.md
```

### Plugin Registration

In your plugin's `__init__.py`, register your extensions when the plugin is imported:

```python
from transmog import register_formatter, register_path_resolver
from .formatters import CustomFormatter
from .resolvers import RegexPathResolver

# Auto-register when the plugin is imported
register_formatter("custom", CustomFormatter())
register_path_resolver("regex", RegexPathResolver())

# Define a convenience function to register all processors
def register_processors():
    from transmog import register_processor
    from .processors import uppercase_strings, format_currency

    register_processor("uppercase", uppercase_strings)
    register_processor("currency", format_currency)
```

### Using Plugins

Users can then use your plugin:

```python
# Install your plugin
# pip install transmog-myplugin

# Use your plugin
import transmog as tm
import transmog_myplugin

# Register processors if needed
transmog_myplugin.register_processors()

# Use the extensions
transformer = tm.Transformer(
    path_resolver="regex",
    paths=["users./^a.*/.name"]
)

result = transformer.transform(data)
custom_output = result.to_format("custom")
```

## Hook System

Transmog provides a hook system for more advanced extensions.

### Available Hooks

- `pre_transform`: Called before transformation begins
- `post_transform`: Called after transformation completes
- `pre_resolve`: Called before resolving each path expression
- `post_resolve`: Called after resolving each path expression
- `pre_process_value`: Called before processing each value
- `post_process_value`: Called after processing each value

### Registering Hooks

Register hooks using the hook registration system:

```python
from transmog import register_hook

def my_pre_transform_hook(data, context=None):
    """Pre-transform hook to add metadata."""
    # Add a timestamp to the context
    if context is None:
        context = {}

    context["timestamp"] = datetime.datetime.now().isoformat()
    return data, context

def my_post_transform_hook(result, context=None):
    """Post-transform hook to modify the result."""
    # Add a metadata field to each record
    for record in result.records:
        if context and "timestamp" in context:
            record["_processed_at"] = context["timestamp"]

    return result

# Register the hooks
register_hook("pre_transform", my_pre_transform_hook)
register_hook("post_transform", my_post_transform_hook)
```

## Naming Utilities

The `naming.utils` module provides common utility functions for handling field and table name formatting across the codebase:

```python
from transmog.naming.utils import format_field_name, get_table_name_for_array

# Format a field name with deep nesting handling
formatted_field = format_field_name(
    field_path="user_address_street",
    separator="_",
    max_component_length=4,
    preserve_leaf_component=True,
    deep_nesting_threshold=4
)

# Generate a table name for arrays
table_name = get_table_name_for_array(
    entity_name="user",
    array_name="addresses",
    parent_path="contacts_0",
    separator="_",
    max_component_length=5,
    deep_nesting_threshold=4
)
```

These utilities help maintain consistency in naming conventions across different modules,
and reduce code duplication between the flattener and extractor modules.

## Best Practices

When extending Transmog, follow these best practices:

1. **Test thoroughly**: Write comprehensive tests for your extensions
2. **Document clearly**: Provide clear documentation with examples
3. **Handle errors gracefully**: Don't let your extensions crash the transformation process
4. **Be performance-conscious**: Extensions can impact performance, so optimize where possible
5. **Follow the interface contracts**: Ensure your extensions match the expected interfaces
6. **Provide sensible defaults**: Make your extensions work reasonably well out of the box
7. **Consider edge cases**: Handle special cases appropriately

By following these guidelines, you can create powerful extensions that enhance Transmog's capabilities.
