# Custom Configuration Patterns

Advanced Transmog usage often requires fine-grained control over processing behavior. The internal configuration system provides flexible options for customizing data transformation, naming conventions, and processing strategies.

## Configuration System Overview

### TransmogConfig Class

The `TransmogConfig` class provides the foundation for advanced configuration:

```python
from transmog.config import TransmogConfig
from transmog.process import Processor

# Create custom configuration
config = TransmogConfig(
    naming_config={
        "separator": ".",
        "nested_threshold": 5,
        "max_field_length": 100
    },
    processing_config={
        "batch_size": 2000,
        "preserve_types": True,
        "error_strategy": "warn"
    },
    output_config={
        "include_metadata": True,
        "timestamp_format": "iso"
    }
)

# Use with processor
processor = Processor(config)
result = processor.process(data)
```

### Factory Methods

Pre-configured settings for common use cases:

```python
# Memory-optimized configuration
memory_config = TransmogConfig.memory_optimized()

# Performance-optimized configuration
performance_config = TransmogConfig.performance_optimized()

# Simple processing configuration
simple_config = TransmogConfig.simple_mode()

# Development/debugging configuration
debug_config = TransmogConfig.debug_mode()
```

## Builder Pattern Configuration

### Fluent Interface

Build configurations using method chaining:

```python
config = (
    TransmogConfig.default()
    .with_naming(
        separator="_",
        nested_threshold=4,
        preserve_case=True
    )
    .with_processing(
        batch_size=3000,
        parallel_workers=2,
        memory_limit="1GB"
    )
    .with_output(
        formats=["json", "parquet"],
        compression="gzip"
    )
)
```

### Conditional Configuration

Apply different settings based on conditions:

```python
def create_config(data_size, memory_available):
    """Create configuration based on data characteristics."""
    config = TransmogConfig.default()

    if data_size > 100000:
        config = config.with_processing(
            batch_size=5000,
            low_memory=memory_available < 8  # GB
        )
    else:
        config = config.with_processing(
            batch_size=2000,
            low_memory=False
        )

    if memory_available < 4:  # GB
        config = config.with_memory_optimization(
            aggressive_cleanup=True,
            type_coercion=True
        )

    return config

# Usage
data_size = len(dataset)
available_memory = get_available_memory_gb()
config = create_config(data_size, available_memory)
```

## Advanced Naming Configuration

### Custom Field Naming

Control how nested fields are named:

```python
# Custom naming strategy
naming_config = {
    "separator": "→",
    "nested_threshold": 6,
    "max_field_length": 50,
    "case_transformation": "snake_case",
    "reserved_words": ["id", "type", "class"],
    "field_mapping": {
        "user_id": "uid",
        "timestamp": "ts"
    }
}

config = TransmogConfig.default().with_naming(**naming_config)
```

### Path Simplification Rules

Customize how deeply nested paths are simplified:

```python
# Path simplification configuration
simplification_config = {
    "threshold": 4,  # Start simplifying at depth 4
    "strategy": "intelligent",  # Options: "intelligent", "truncate", "hash"
    "preserve_terminals": True,  # Keep final field names
    "common_prefixes": ["data", "info", "meta"]  # Remove common prefixes
}

config = (
    TransmogConfig.default()
    .with_path_simplification(**simplification_config)
)
```

## Processing Customization

### Error Handling Strategies

Define custom error handling behavior:

```python
# Advanced error handling
error_config = {
    "strategy": "custom",
    "max_errors": 100,
    "error_callback": lambda error, record: log_error(error, record),
    "recovery_attempts": 3,
    "fallback_value": "__ERROR__"
}

config = (
    TransmogConfig.default()
    .with_error_handling(**error_config)
)

def log_error(error, record):
    """Custom error logging function."""
    print(f"Error processing record {record.get('id', 'unknown')}: {error}")
```

### Type Handling Configuration

Customize data type processing:

```python
# Custom type handling
type_config = {
    "preserve_types": True,
    "type_coercion_rules": {
        "string_to_number": True,
        "date_parsing": True,
        "boolean_conversion": True
    },
    "null_handling": {
        "strategy": "preserve",  # "preserve", "remove", "convert"
        "null_values": [None, "", "null", "NULL"]
    },
    "datetime_formats": [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ"
    ]
}

config = (
    TransmogConfig.default()
    .with_type_handling(**type_config)
)
```

## Array Processing Configuration

### Advanced Array Handling

Fine-tune array processing behavior:

```python
# Custom array configuration
array_config = {
    "default_strategy": "separate",
    "size_thresholds": {
        "inline_max": 5,      # Inline arrays with ≤5 items
        "separate_min": 6     # Separate arrays with ≥6 items
    },
    "field_specific": {
        "tags": "inline",     # Always inline tag arrays
        "items": "separate",  # Always separate item arrays
        "metadata": "skip"    # Skip metadata arrays
    },
    "nested_array_handling": "flatten"  # How to handle arrays within arrays
}

config = (
    TransmogConfig.default()
    .with_array_processing(**array_config)
)
```

### Conditional Array Processing

Process arrays differently based on content:

```python
def array_strategy_callback(field_name, array_data, context):
    """Determine array processing strategy based on content."""
    if len(array_data) <= 3:
        return "inline"
    elif all(isinstance(item, str) for item in array_data):
        return "inline"  # Simple string arrays inline
    else:
        return "separate"  # Complex objects in separate tables

config = (
    TransmogConfig.default()
    .with_dynamic_array_handling(array_strategy_callback)
)
```

## Output Customization

### Multi-Format Output

Configure multiple output formats with different settings:

```python
# Multi-format configuration
output_config = {
    "formats": {
        "json": {
            "indent": 2,
            "ensure_ascii": False,
            "sort_keys": True
        },
        "csv": {
            "delimiter": ",",
            "quoting": "minimal",
            "encoding": "utf-8"
        },
        "parquet": {
            "compression": "snappy",
            "row_group_size": 10000,
            "use_dictionary": True
        }
    },
    "file_naming": {
        "pattern": "{name}_{table}_{timestamp}",
        "timestamp_format": "%Y%m%d_%H%M%S"
    }
}

config = (
    TransmogConfig.default()
    .with_output_configuration(**output_config)
)
```

### Custom Metadata Generation

Add custom metadata to output:

```python
def metadata_generator(processing_context):
    """Generate custom metadata for output."""
    return {
        "processing_time": processing_context.duration,
        "record_count": processing_context.record_count,
        "configuration_hash": processing_context.config_hash,
        "data_source": processing_context.source_info,
        "quality_metrics": calculate_quality_metrics(processing_context)
    }

config = (
    TransmogConfig.default()
    .with_metadata_generation(metadata_generator)
)
```

## Performance Tuning Configuration

### Resource Management

Configure resource usage limits:

```python
# Resource configuration
resource_config = {
    "memory_limit": "2GB",
    "cpu_cores": 4,
    "io_threads": 2,
    "batch_size_auto_tune": True,
    "garbage_collection": {
        "strategy": "aggressive",
        "frequency": 1000  # Every 1000 records
    }
}

config = (
    TransmogConfig.default()
    .with_resource_management(**resource_config)
)
```

### Parallel Processing

Configure parallel processing behavior:

```python
# Parallel processing configuration
parallel_config = {
    "enabled": True,
    "worker_count": 4,
    "chunk_size": 5000,
    "coordination_strategy": "work_stealing",
    "result_aggregation": "streaming"
}

config = (
    TransmogConfig.default()
    .with_parallel_processing(**parallel_config)
)
```

## Environment-Specific Configurations

### Development Configuration

Settings optimized for development and debugging:

```python
def development_config():
    """Configuration for development environment."""
    return (
        TransmogConfig.debug_mode()
        .with_processing(
            batch_size=100,  # Small batches for easier debugging
            preserve_types=True,
            error_strategy="raise"  # Fail fast
        )
        .with_output(
            include_debug_info=True,
            verbose_logging=True
        )
        .with_validation(
            strict_mode=True,
            schema_validation=True
        )
    )
```

### Production Configuration

Settings optimized for production environments:

```python
def production_config(data_characteristics):
    """Configuration for production environment."""
    base_config = TransmogConfig.performance_optimized()

    if data_characteristics.get("high_volume"):
        config = base_config.with_processing(
            batch_size=10000,
            parallel_workers=8,
            memory_optimization=True
        )
    else:
        config = base_config.with_processing(
            batch_size=5000,
            parallel_workers=4
        )

    return config.with_output(
        compression="gzip",
        include_metadata=True,
        error_logging=True
    )
```

### Testing Configuration

Settings for automated testing:

```python
def testing_config():
    """Configuration for test environment."""
    return (
        TransmogConfig.simple_mode()
        .with_processing(
            batch_size=50,  # Small batches for predictable results
            deterministic_ids=True,  # Consistent output for testing
            error_strategy="collect"  # Collect all errors
        )
        .with_output(
            sort_output=True,  # Deterministic ordering
            include_processing_stats=True
        )
    )
```

## Configuration Validation

### Schema Validation

Validate configuration against schema:

```python
from transmog.config import ConfigValidator

def validate_config(config):
    """Validate configuration before use."""
    validator = ConfigValidator()

    # Check for conflicts
    conflicts = validator.check_conflicts(config)
    if conflicts:
        raise ValueError(f"Configuration conflicts: {conflicts}")

    # Validate resource limits
    if not validator.validate_resource_limits(config):
        raise ValueError("Resource limits exceed system capacity")

    # Check format compatibility
    format_issues = validator.check_format_compatibility(config)
    if format_issues:
        print(f"Warning: Format compatibility issues: {format_issues}")

    return True

# Usage
config = create_custom_config()
validate_config(config)
```

### Configuration Profiles

Save and load configuration profiles:

```python
# Save configuration profile
config = create_custom_config()
config.save_profile("my_profile", description="Custom config for project X")

# Load configuration profile
loaded_config = TransmogConfig.load_profile("my_profile")

# List available profiles
profiles = TransmogConfig.list_profiles()
for profile in profiles:
    print(f"{profile.name}: {profile.description}")
```

## Integration Examples

### Integration with External Systems

Configure for specific external system integration:

```python
def database_integration_config():
    """Configuration for database integration."""
    return (
        TransmogConfig.default()
        .with_naming(
            separator="_",
            case_transformation="snake_case",
            reserved_words=["order", "group", "select"]  # SQL keywords
        )
        .with_type_handling(
            preserve_types=True,
            null_handling="database_null"
        )
        .with_output(
            formats=["csv"],  # Database-friendly format
            include_headers=True,
            escape_special_chars=True
        )
    )
```

### API Integration Configuration

Configure for API response processing:

```python
def api_integration_config():
    """Configuration for API response processing."""
    return (
        TransmogConfig.default()
        .with_naming(
            separator=".",
            preserve_case=True  # Maintain API field naming
        )
        .with_array_processing(
            default_strategy="separate",
            preserve_order=True
        )
        .with_output(
            formats=["json"],
            maintain_structure=True
        )
        .with_metadata(
            include_api_metadata=True,
            timestamp_source="api"
        )
    )
```

### Data Pipeline Configuration

Configure for data pipeline integration:

```python
def pipeline_config(stage):
    """Configuration based on pipeline stage."""
    base_config = TransmogConfig.default()

    if stage == "ingestion":
        return base_config.with_processing(
            error_strategy="skip",  # Continue on errors
            batch_size=10000,
            preserve_raw_data=True
        )
    elif stage == "transformation":
        return base_config.with_processing(
            error_strategy="warn",
            type_coercion=True,
            data_cleaning=True
        )
    elif stage == "output":
        return base_config.with_output(
            compression="gzip",
            include_lineage=True,
            quality_validation=True
        )
```
