# Extending and Customization

Transmog provides extension points for customizing data transformation behavior, adding new functionality, and integrating with external systems. This guide covers advanced customization techniques and extension patterns.

## Custom Processors

### Creating Custom Processors

Extend the base processor for specialized functionality:

```python
from transmog.process import Processor
from transmog.config import TransmogConfig

class CustomProcessor(Processor):
    """Custom processor with specialized behavior."""

    def __init__(self, config=None):
        super().__init__(config or TransmogConfig.default())
        self.custom_handlers = {}

    def register_field_handler(self, field_pattern, handler):
        """Register custom handler for specific fields."""
        self.custom_handlers[field_pattern] = handler

    def process_field(self, field_name, value, context):
        """Override field processing with custom logic."""
        # Check for custom handlers
        for pattern, handler in self.custom_handlers.items():
            if self.matches_pattern(field_name, pattern):
                return handler(value, context)

        # Fall back to default processing
        return super().process_field(field_name, value, context)

    def matches_pattern(self, field_name, pattern):
        """Check if field matches pattern."""
        import re
        return re.match(pattern, field_name) is not None

# Usage
processor = CustomProcessor()

# Register custom handler for timestamp fields
def timestamp_handler(value, context):
    """Convert timestamp formats."""
    from datetime import datetime
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return dt.isoformat()
        except ValueError:
            return value
    return value

processor.register_field_handler(r'.*timestamp.*', timestamp_handler)

result = processor.process(data)
```

### Specialized Data Type Processors

Create processors for specific data types:

```python
class GeospatialProcessor(Processor):
    """Processor specialized for geospatial data."""

    def process_coordinates(self, value, context):
        """Process coordinate data."""
        if isinstance(value, dict) and 'lat' in value and 'lon' in value:
            return {
                'latitude': float(value['lat']),
                'longitude': float(value['lon']),
                'coordinate_string': f"{value['lat']},{value['lon']}"
            }
        return value

    def process_field(self, field_name, value, context):
        """Override for geospatial field handling."""
        if 'location' in field_name.lower() or 'coordinates' in field_name.lower():
            return self.process_coordinates(value, context)
        return super().process_field(field_name, value, context)

# Usage
geo_processor = GeospatialProcessor()
result = geo_processor.process(geospatial_data)
```

## Custom Transformations

### Field Transformation Functions

Create reusable field transformation functions:

```python
def create_field_transformer(transformation_func):
    """Create a field transformer."""
    def transformer(processor):
        original_process_field = processor.process_field

        def enhanced_process_field(field_name, value, context):
            # Apply transformation
            transformed_value = transformation_func(field_name, value, context)
            # Continue with normal processing
            return original_process_field(field_name, transformed_value, context)

        processor.process_field = enhanced_process_field
        return processor

    return transformer

# Example transformations
def normalize_currency(field_name, value, context):
    """Normalize currency values."""
    if 'price' in field_name.lower() or 'cost' in field_name.lower():
        if isinstance(value, str):
            # Remove currency symbols and convert to float
            import re
            numeric_value = re.sub(r'[^\d.]', '', value)
            try:
                return float(numeric_value)
            except ValueError:
                return value
    return value

def standardize_dates(field_name, value, context):
    """Standardize date formats."""
    if 'date' in field_name.lower() or 'time' in field_name.lower():
        if isinstance(value, str):
            from datetime import datetime
            date_formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%d-%m-%Y',
                '%Y-%m-%d %H:%M:%S'
            ]
            for fmt in date_formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
    return value

# Apply transformations
processor = Processor()
processor = create_field_transformer(normalize_currency)(processor)
processor = create_field_transformer(standardize_dates)(processor)
```

### Validation Transformations

Add data validation during processing:

```python
class ValidatingProcessor(Processor):
    """Processor with built-in validation."""

    def __init__(self, config=None, validation_rules=None):
        super().__init__(config)
        self.validation_rules = validation_rules or {}
        self.validation_errors = []

    def validate_field(self, field_name, value, context):
        """Validate field against rules."""
        if field_name in self.validation_rules:
            rule = self.validation_rules[field_name]

            if 'type' in rule:
                expected_type = rule['type']
                if not isinstance(value, expected_type):
                    error = f"Field {field_name}: expected {expected_type}, got {type(value)}"
                    self.validation_errors.append(error)
                    return False

            if 'range' in rule and isinstance(value, (int, float)):
                min_val, max_val = rule['range']
                if not (min_val <= value <= max_val):
                    error = f"Field {field_name}: value {value} not in range [{min_val}, {max_val}]"
                    self.validation_errors.append(error)
                    return False

            if 'pattern' in rule and isinstance(value, str):
                import re
                if not re.match(rule['pattern'], value):
                    error = f"Field {field_name}: value does not match pattern"
                    self.validation_errors.append(error)
                    return False

        return True

    def process_field(self, field_name, value, context):
        """Process with validation."""
        if self.validate_field(field_name, value, context):
            return super().process_field(field_name, value, context)
        else:
            # Return original value for invalid data
            return value

# Usage with validation rules
validation_rules = {
    'age': {'type': int, 'range': (0, 150)},
    'email': {'pattern': r'^[^@]+@[^@]+\.[^@]+$'},
    'price': {'type': float, 'range': (0, 10000)}
}

validator = ValidatingProcessor(validation_rules=validation_rules)
result = validator.process(data)

if validator.validation_errors:
    print("Validation errors:", validator.validation_errors)
```

## Plugin System

### Creating Plugins

Develop reusable plugins for common functionality:

```python
class TransmogPlugin:
    """Base class for Transmog plugins."""

    def __init__(self, name, version="1.0.0"):
        self.name = name
        self.version = version

    def configure(self, processor):
        """Configure the processor with plugin functionality."""
        raise NotImplementedError

    def validate_config(self, config):
        """Validate plugin configuration."""
        return True

class DataCleaningPlugin(TransmogPlugin):
    """Plugin for data cleaning operations."""

    def __init__(self, cleaning_rules=None):
        super().__init__("data_cleaning", "1.0.0")
        self.cleaning_rules = cleaning_rules or {}

    def configure(self, processor):
        """Add data cleaning to processor."""
        original_process_field = processor.process_field

        def clean_field(field_name, value, context):
            # Apply cleaning rules
            cleaned_value = self.clean_value(field_name, value)
            return original_process_field(field_name, cleaned_value, context)

        processor.process_field = clean_field
        return processor

    def clean_value(self, field_name, value):
        """Clean individual field values."""
        if isinstance(value, str):
            # Remove leading/trailing whitespace
            value = value.strip()

            # Apply field-specific cleaning
            if field_name in self.cleaning_rules:
                rule = self.cleaning_rules[field_name]
                if rule == 'uppercase':
                    value = value.upper()
                elif rule == 'lowercase':
                    value = value.lower()
                elif rule == 'title_case':
                    value = value.title()

        return value

# Usage
cleaning_rules = {
    'name': 'title_case',
    'email': 'lowercase',
    'status': 'uppercase'
}

plugin = DataCleaningPlugin(cleaning_rules)
processor = Processor()
processor = plugin.configure(processor)
```

### Plugin Manager

Manage multiple plugins:

```python
class PluginManager:
    """Manage Transmog plugins."""

    def __init__(self):
        self.plugins = {}

    def register_plugin(self, plugin):
        """Register a plugin."""
        if plugin.name in self.plugins:
            raise ValueError(f"Plugin {plugin.name} already registered")
        self.plugins[plugin.name] = plugin

    def apply_plugins(self, processor, plugin_names=None):
        """Apply plugins to processor."""
        plugins_to_apply = plugin_names or list(self.plugins.keys())

        for plugin_name in plugins_to_apply:
            if plugin_name in self.plugins:
                plugin = self.plugins[plugin_name]
                processor = plugin.configure(processor)

        return processor

    def list_plugins(self):
        """List registered plugins."""
        return [(name, plugin.version) for name, plugin in self.plugins.items()]

# Usage
manager = PluginManager()
manager.register_plugin(DataCleaningPlugin())
manager.register_plugin(ValidatingProcessor())

processor = Processor()
processor = manager.apply_plugins(processor, ['data_cleaning'])
```

## Custom Output Formats

### Creating Custom Writers

Implement custom output format writers:

```python
from transmog.io import BaseWriter

class XMLWriter(BaseWriter):
    """Custom XML output writer."""

    def __init__(self, config=None):
        super().__init__(config)
        self.format_name = "xml"

    def write_table(self, table_name, records, output_path):
        """Write table to XML format."""
        import xml.etree.ElementTree as ET

        root = ET.Element("table", name=table_name)

        for record in records:
            record_element = ET.SubElement(root, "record")
            for key, value in record.items():
                field_element = ET.SubElement(record_element, "field", name=key)
                field_element.text = str(value) if value is not None else ""

        tree = ET.ElementTree(root)
        tree.write(output_path, encoding="utf-8", xml_declaration=True)

    def write_result(self, result, output_dir):
        """Write complete result to XML files."""
        import os

        # Write main table
        main_path = os.path.join(output_dir, f"{result.name}.xml")
        self.write_table(result.name, result.main, main_path)

        # Write child tables
        for table_name, records in result.tables.items():
            table_path = os.path.join(output_dir, f"{table_name}.xml")
            self.write_table(table_name, records, table_path)

# Register custom writer
from transmog.io import WriterRegistry

WriterRegistry.register_writer("xml", XMLWriter)

# Usage
import transmog as tm

result = tm.flatten(data)
xml_writer = XMLWriter()
xml_writer.write_result(result, "output/xml/")
```

### Custom Format Integration

Integrate custom formats with main API:

```python
def flatten_to_xml(data, output_dir, **kwargs):
    """Flatten data and output to XML format."""
    import transmog as tm

    # Process data
    result = tm.flatten(data, **kwargs)

    # Write to XML
    writer = XMLWriter()
    writer.write_result(result, output_dir)

    return result

# Usage
result = flatten_to_xml(data, "output/", name="custom_data")
```

## Integration Hooks

### Pre/Post Processing Hooks

Add hooks for custom processing stages:

```python
class HookableProcessor(Processor):
    """Processor with hook support."""

    def __init__(self, config=None):
        super().__init__(config)
        self.pre_hooks = []
        self.post_hooks = []
        self.field_hooks = {}

    def add_pre_hook(self, hook_func):
        """Add pre-processing hook."""
        self.pre_hooks.append(hook_func)

    def add_post_hook(self, hook_func):
        """Add post-processing hook."""
        self.post_hooks.append(hook_func)

    def add_field_hook(self, field_pattern, hook_func):
        """Add field-specific hook."""
        if field_pattern not in self.field_hooks:
            self.field_hooks[field_pattern] = []
        self.field_hooks[field_pattern].append(hook_func)

    def process(self, data, **kwargs):
        """Process with hooks."""
        # Execute pre-hooks
        for hook in self.pre_hooks:
            data = hook(data)

        # Normal processing
        result = super().process(data, **kwargs)

        # Execute post-hooks
        for hook in self.post_hooks:
            result = hook(result)

        return result

# Example hooks
def data_validation_hook(data):
    """Validate input data."""
    if not isinstance(data, (dict, list)):
        raise ValueError("Data must be dict or list")
    return data

def result_enhancement_hook(result):
    """Enhance processing result."""
    # Add custom metadata
    if hasattr(result, 'metadata'):
        result.metadata['enhanced'] = True
    return result

# Usage
processor = HookableProcessor()
processor.add_pre_hook(data_validation_hook)
processor.add_post_hook(result_enhancement_hook)

result = processor.process(data)
```

### Event System

Implement event-driven processing:

```python
class EventDrivenProcessor(Processor):
    """Processor with event system."""

    def __init__(self, config=None):
        super().__init__(config)
        self.event_handlers = {}

    def on(self, event_name, handler):
        """Register event handler."""
        if event_name not in self.event_handlers:
            self.event_handlers[event_name] = []
        self.event_handlers[event_name].append(handler)

    def emit(self, event_name, **event_data):
        """Emit event to handlers."""
        if event_name in self.event_handlers:
            for handler in self.event_handlers[event_name]:
                handler(**event_data)

    def process_record(self, record, context):
        """Process single record with events."""
        self.emit('record_start', record=record, context=context)

        try:
            result = super().process_record(record, context)
            self.emit('record_success', record=record, result=result, context=context)
            return result
        except Exception as e:
            self.emit('record_error', record=record, error=e, context=context)
            raise

# Event handlers
def log_record_start(record, context):
    print(f"Processing record: {record.get('id', 'unknown')}")

def log_record_error(record, error, context):
    print(f"Error processing record {record.get('id', 'unknown')}: {error}")

# Usage
processor = EventDrivenProcessor()
processor.on('record_start', log_record_start)
processor.on('record_error', log_record_error)

result = processor.process(data)
```

## Testing Extensions

### Extension Testing Framework

Create testing utilities for extensions:

```python
import unittest
from transmog.testing import ProcessorTestCase

class CustomProcessorTestCase(ProcessorTestCase):
    """Test case for custom processors."""

    def setUp(self):
        self.processor = CustomProcessor()

    def test_custom_field_handler(self):
        """Test custom field handler functionality."""
        # Register test handler
        def test_handler(value, context):
            return f"processed_{value}"

        self.processor.register_field_handler("test_.*", test_handler)

        # Test data
        data = {"test_field": "value", "other_field": "value"}
        result = self.processor.process(data)

        # Assertions
        main_record = result.main[0]
        self.assertEqual(main_record["test_field"], "processed_value")
        self.assertEqual(main_record["other_field"], "value")

    def test_error_handling(self):
        """Test error handling in custom processor."""
        def error_handler(value, context):
            raise ValueError("Test error")

        self.processor.register_field_handler("error_.*", error_handler)

        data = {"error_field": "value"}

        with self.assertRaises(ValueError):
            self.processor.process(data)

# Run tests
if __name__ == '__main__':
    unittest.main()
```

### Plugin Testing

Test plugin functionality:

```python
class PluginTestCase(unittest.TestCase):
    """Test case for plugins."""

    def test_data_cleaning_plugin(self):
        """Test data cleaning plugin."""
        # Configure plugin
        cleaning_rules = {'name': 'title_case'}
        plugin = DataCleaningPlugin(cleaning_rules)

        # Apply to processor
        processor = Processor()
        processor = plugin.configure(processor)

        # Test data
        data = {"name": "john doe", "age": 30}
        result = processor.process(data)

        # Check cleaning was applied
        main_record = result.main[0]
        self.assertEqual(main_record["name"], "John Doe")
        self.assertEqual(main_record["age"], 30)

    def test_plugin_manager(self):
        """Test plugin manager functionality."""
        manager = PluginManager()
        plugin = DataCleaningPlugin()

        # Test registration
        manager.register_plugin(plugin)
        self.assertIn("data_cleaning", manager.plugins)

        # Test duplicate registration
        with self.assertRaises(ValueError):
            manager.register_plugin(plugin)

        # Test plugin application
        processor = Processor()
        configured_processor = manager.apply_plugins(processor)

        # Verify processor was modified
        self.assertNotEqual(processor.process_field, configured_processor.process_field)
```

## Documentation for Extensions

### Documenting Custom Processors

Create documentation for custom processors:

```python
class DocumentedProcessor(Processor):
    """
    Custom processor with comprehensive documentation.

    This processor extends the base Transmog processor with specialized
    functionality for [specific use case].

    Features:
    - Custom field handling for [specific field types]
    - Enhanced error reporting
    - Integration with [external system]

    Example:
        >>> processor = DocumentedProcessor(config)
        >>> result = processor.process(data)
        >>> print(result.main)

    Args:
        config: TransmogConfig instance or None for default config
        custom_param: Additional parameter for [specific functionality]

    Attributes:
        custom_handlers: Dictionary of registered field handlers
        statistics: Processing statistics and metrics
    """

    def __init__(self, config=None, custom_param=None):
        super().__init__(config)
        self.custom_param = custom_param
        self.custom_handlers = {}
        self.statistics = {"processed_fields": 0, "custom_handled": 0}

    def register_field_handler(self, pattern, handler):
        """
        Register a custom field handler.

        Args:
            pattern (str): Regex pattern to match field names
            handler (callable): Function to process matching fields
                Signature: handler(value, context) -> processed_value

        Example:
            >>> def timestamp_handler(value, context):
            ...     return parse_timestamp(value)
            >>> processor.register_field_handler(r'.*_timestamp', timestamp_handler)
        """
        self.custom_handlers[pattern] = handler
```

This completes the major content for the advanced section. Now I'll continue with the API reference documentation.
